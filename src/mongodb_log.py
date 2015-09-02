#!/usr/bin/python

###########################################################################
#  mongodb_log.py - Python based ROS to MongoDB logger (multi-process)
#
#  Created: Sun Dec 05 19:45:51 2010
#  Copyright  2010-2012  Tim Niemueller [www.niemueller.de]
#             2010-2011  Carnegie Mellon University
#             2010       Intel Labs Pittsburgh
###########################################################################

#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Library General Public License for more details.
#
#  Read the full text in the LICENSE.GPL file in the doc directory.
# make sure we aren't using floor division
from __future__ import division, with_statement


BACKLOG_WARN_LIMIT = 100
STATS_GRAPHTIME = 60
PACKAGE_NAME = 'mongodb_log'
NODE_NAME = 'mongodb_log'
NODE_NAME_TEMPLATE = '%smongodb_log'
QUEUE_MAXSIZE = 100


import roslib
roslib.load_manifest(PACKAGE_NAME)
import os
import re
import sys
import time
import string
import socket
import abc
import subprocess
from threading import Thread, Timer
from multiprocessing import Process, Lock, Queue, Value, current_process
from Queue import Empty
from optparse import OptionParser
from tempfile import mktemp
from datetime import datetime, timedelta
from time import sleep
import genpy
import rospy
import rosgraph.masterapi
import rostopic
from pymongo import Connection, SLOW_ONLY
from pymongo.errors import InvalidDocument, InvalidStringData
import rrdtool
from random import randint


use_setproctitle = True
try:
    from setproctitle import setproctitle
except ImportError:
    use_setproctitle = False


class Counter(object):
    def __init__(self, value=None, lock=True):
        self.count = value or Value('i', 0, lock=lock)
        self.mutex = Lock()

    def increment(self, by=1):
        with self.mutex:
            self.count.value += by

    def value(self):
        with self.mutex:
            return self.count.value


def register_logger(message, logger):
    """
    Registers a new specialized logger for the given message type.
    Whenever the Logger detects a topic of the given message type, it will create a new instance of the given logger
    class and use this one for the logging.

    :param message: The message type to use the specialized logger for.
    :type message: object
    :param logger: The logger to use
    :type logger: MongoDBLogger
    """
    MongoWriter.registerLogger(message, logger)


class MongoDBLogger(Process):
    """
    This is the abstract base class for all loggers, who want to log to a mongodb.
    This class creates a process and collects all informations necessary to connect to a mongodb.
    It does _NOT_ create a connection to the MongoDB, as this might be done by special loggers.
    This class does also _NOT_ subscribe to a Topic for the same reason.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, id_, topic, collname, mongodb_host, mongodb_port, mongodb_name, nodename_prefix=""):
        """
        Creates a new instance of the MongoDBLogger. This creates a new process to use for logging a topic.
        This collects all necessary information to initalize a logger.
        This method does _NOT_ start the process, as this will be done by the `start()`-Method.

        :param id_: The ID of this logger process
        :type id_: int
        :param topic: The topic to log messages from
        :type topic: basestring
        :param collname: The name of the collection in the MongoDB to log messages to.
        :type collnae: basestring
        :param mongodb_host:The hostname or ip of the host running the MongoDB
        :type mongodb_host: basestring
        :param mongodb_port: The port on which the MongoDB is running on
        :type mongodb_port: int
        :param mongodb_name: The name of the databse to use for logging
        :type mongodb_name: basestring
        :param nodename_prefix: An optional prefix to use for the name of this Process
        :type nodename_prefix: basestring
        :return: A new MonoDBLogger instance
        :rtype: MongoDBLogger
        """
        super(MongoDBLogger, self).__init__(name="%sMongoDBLogger-%d-%s" % (nodename_prefix, id_, topic))
        self.id = id_
        self.topic = topic
        self.collname = collname
        self.mongodb_host = mongodb_host
        self.mongodb_port = mongodb_port
        self.mongodb_name = mongodb_name
        self.nodename_prefix = nodename_prefix
        self.nodename = "%sMongoDBLogger_%d%s" % (nodename_prefix, id_, topic.replace("/", "_"))
        self.quit = Value('i', 0)

    def shutdown(self):
        """
        Shuts down the running process and joins it back to the calling process.
        """
        if not self.is_quit():
            self.quit.value = 1
            self.queue.put("shutdown")
        self.join()

    def is_quit(self):
        """
        Checks if this process should quit
        :return: True if this process should quit
        :rtype: bool
        """
        return self.quit.value == 1

    @property
    def process(self):
        """
        Returns the process object of this Logger.
        This property is designed to be overwritten by subclasses to return the correct process object.

        :return: The process doint the actual logging
        :rtype: Process
        """
        return self

    @abc.abstractmethod
    def run(self):
        """
        This method implements the actual logging.
        It will be executed in the child process and has to do the following things.
            - Initialize a new ros node
            - Create a connection to the MongoDB
            - Do the actual logging
        """
        raise NotImplementedError()

class TopicLogger(MongoDBLogger):
    """
    This class implements a generic topic logger.
    It simply dumps all messages received from the topic into the MongoDB.
    """

    def __init__(self, id_, topic, collname, mongodb_host, mongodb_port, mongodb_name, nodename_prefix,
                 max_queuesize=QUEUE_MAXSIZE):
        MongoDBLogger.__init__(self, id_, topic, collname, mongodb_host, mongodb_port, mongodb_name, nodename_prefix)
        self.worker_out_counter = Counter()
        self.worker_in_counter = Counter()
        self.worker_drop_counter = Counter()
        self.queue = Queue(max_queuesize)

    def _init(self):
        """
        This method initializes this process.
        It has to be called from within the child process, therefore it has to be called in the `run()`-Method.
        It initializes a new ros node, the connection to the MongoDB and subscribes to the topic.
        """
        rospy.logdebug("Inializing node %s" % self.nodename)
        rospy.init_node(self.nodename, anonymous=False)
        if use_setproctitle:
            setproctitle("mongodb_log %s" % self.topic)

        self.mongoconn = Connection(self.mongodb_host, self.mongodb_port)
        self.mongodb = self.mongoconn[self.mongodb_name]
        self.mongodb.set_profiling_level = SLOW_ONLY

        self.collection = self.mongodb[self.collname]
        self.collection.count()

        self.queue.cancel_join_thread()
        self.subscriber = None
        while not self.subscriber:
            try:
                msg_class, real_topic, msg_eval = rostopic.get_topic_class(self.topic, blocking=True)
                self.subscriber = rospy.Subscriber(real_topic, msg_class, self._enqueue, self.topic)
            except rostopic.ROSTopicIOException:
                rospy.logwarn("FAILED to subscribe, will keep trying %s" % self.name)
                time.sleep(randint(1, 10))
            except rospy.ROSInitException:
                rospy.logwarn("FAILED to initialize, will keep trying %s" % self.name)
                time.sleep(randint(1, 10))
                self.subscriber = None

    def run(self):
        """
        Runs the process.
        This method is called by the child process to do the actual logging.
        """
        self._init()
        rospy.logdebug("ACTIVE: %s" % self.name)
        # Process the messages
        while not self.is_quit():
            self._dequeue()

        # we must make sure to clear the queue before exiting,
        # or the parent thread might deadlock otherwise
        self.subscriber.unregister()
        self.subscriber = None
        while not self.queue.empty():
            t = self.queue.get_nowait()
        rospy.logdebug("STOPPED: %s" % self.name)

    def _sanitize_value(self, v):
        if isinstance(v, rospy.Message):
            return self._message_to_dict(v)
        elif isinstance(v, genpy.rostime.Time):
            t = datetime.utcfromtimestamp(v.secs)
            return t + timedelta(microseconds=v.nsecs / 1000.)
        elif isinstance(v, genpy.rostime.Duration):
            return v.secs + v.nsecs / 1000000000.
        elif isinstance(v, list):
            return [self._sanitize_value(t) for t in v]
        else:
            return v

    def _message_to_dict(self, val):
        d = {}
        for f in val.__slots__:
            d[f] = self._sanitize_value(getattr(val, f))
        return d

    def qsize(self):
        return self.queue.qsize()

    def _enqueue(self, data, topic, current_time=None):
        if not self.is_quit():
            if self.queue.full():
                try:
                    self.queue.get_nowait()
                    self.worker_drop_counter.increment()
                except Empty:
                    pass
            self.queue.put((topic, data, rospy.get_time()))
            self.worker_in_counter.increment()

    def _dequeue(self):
        try:
            t = self.queue.get(True)
        except IOError:
            self.quit.value = 1
            return
        if isinstance(t, tuple):
            self.worker_out_counter.increment()
            topic = t[0]
            msg = t[1]
            ctime = t[2]

            if isinstance(msg, rospy.Message):
                doc = self._message_to_dict(msg)
                doc["__recorded"] = ctime or datetime.now()
                doc["__topic"] = topic
                try:
                    self.collection.insert(doc)
                except (InvalidStringData, InvalidDocument), e:
                    rospy.logerr("%s %s@%s:\n%s" % (e.__class__.__name__, current_process().name, topic, e))
        else:
            self.quit.value = 1


class CPPLogger(MongoDBLogger):
    """
    This class implements a base class for spezialized loggers using other languages (like C++).
    """

    def __init__(self, idnum, topic, collname, mongodb_host, mongodb_port, mongodb_name, nodename_prefix, cpp_logger,
                 additional_parameters):
        """
        Creates a new instance of the CPPLogger.
        :param id_: The ID of this logger process
        :type id_: int
        :param topic: The topic to log messages from
        :type topic: basestring
        :param collname: The name of the collection in the MongoDB to log messages to.
        :type collnae: basestring
        :param mongodb_host:The hostname or ip of the host running the MongoDB
        :type mongodb_host: basestring
        :param mongodb_port: The port on which the MongoDB is running on
        :type mongodb_port: int
        :param mongodb_name: The name of the databse to use for logging
        :type mongodb_name: basestring
        :param nodename_prefix: An optional prefix to use for the name of this Process
        :type nodename_prefix: basestring
        :param cpp_logger: The path to the executable to use for the actual logging
        :type cpp_logger: basestring
        :param additional_parameters: Optional additional parameters to give to the logger
        :type additional_parameters: [basestring]
        :return: A new CPPLogger instance
        :rtype: CPPLogger
        """
        super(CPPLogger, self).__init__(idnum, topic, collname, mongodb_host, mongodb_port, mongodb_name, nodename_prefix)
        self.worker_out_counter = Counter()
        self.worker_in_counter = Counter()
        self.worker_drop_counter = Counter()
        self.qsize = 0
        self.additional_parameters = additional_parameters if additional_parameters is not None else []
        self.cpp_logger = cpp_logger
        self.__process = None

    @property
    def process(self):
        return self.__process

    def qsize(self):
        return self.qsize

    def run(self):
        mongodb_host_port = "%s:%d" % (self.mongodb_host, self.mongodb_port)
        collection = "%s.%s" % (self.mongodb_name, self.collname)

        self.__process = subprocess.Popen([self.cpp_logger, "-t", self.topic, "-n", self.nodename,
                                           "-m", mongodb_host_port, "-c", collection] + self.additional_parameters,
                                        stdout=subprocess.PIPE)
        while not self.is_quit():
            line = self.__process.stdout.readline().rstrip()
            if line == "":
                continue
            arr = string.split(line, ":")
            self.qsize = int(arr[3])

            self.worker_in_counter.increment(int(arr[0]))
            self.worker_out_counter.increment(int(arr[1]))
            self.worker_drop_counter.increment(int(arr[2]))

    def shutdown(self):
        super(CPPLogger, self).shutdown()
        self.__process.kill()
        self.__process.wait()


class MongoWriter(object):
    """
    This class acts as the root node for all loggers.
    If initializes the different loggers for each topic given and writes stats them.
    """

    __special_logger = {}

    def __init__(self, topics=None, graph_topics=False,
                 graph_dir=".", graph_clear=False, graph_daemon=False,
                 all_topics=False, all_topics_interval=5,
                 exclude_topics=None,
                 mongodb_host=None, mongodb_port=None, mongodb_name="roslog",
                 no_specific=False, nodename_prefix="", stats_looptime=0):

        # Register specialized loggers
        from special_loggers import register_special_loggers
        register_special_loggers()

        self.graph_dir = graph_dir
        self.graph_topics = graph_topics
        self.graph_clear = graph_clear
        self.graph_daemon = graph_daemon
        self.all_topics = all_topics
        self.all_topics_interval = all_topics_interval
        self.exclude_topics = exclude_topics if exclude_topics is not None else []
        self.mongodb_host = mongodb_host
        self.mongodb_port = mongodb_port
        self.mongodb_name = mongodb_name
        self.no_specific = no_specific
        self.nodename_prefix = nodename_prefix
        self.quit = False
        self.topics = set()
        self.sep = "\n"  # '\033[2J\033[;H'
        self.workers = {}
        self.stats_looptime = stats_looptime

        if self.graph_dir == ".":
            self.graph_dir = os.getcwd()
        if not os.path.exists(self.graph_dir):
            os.makedirs(self.graph_dir)

        if use_setproctitle:
            setproctitle("mongodb_log MAIN")

        self.exclude_regex = []
        for et in self.exclude_topics:
            self.exclude_regex.append(re.compile(et))
        self.exclude_already = []

        self.init_rrd()

        self.subscribe_topics(set(topics if topics is not None else []))
        if self.all_topics:
            rospy.logdebug("All topics")
            self.ros_master = rosgraph.masterapi.Master(NODE_NAME_TEMPLATE % self.nodename_prefix)
            self.update_topics(restart=False)
        self.start_all_topics_timer()

    @classmethod
    def registerLogger(cls, msgClass, logger_class):
        cls.__special_logger[msgClass] = logger_class

    def subscribe_topics(self, topics):
        for topic in topics:
            if topic and topic[-1] == '/':
                topic = topic[:-1]

            if topic in self.topics:
                continue
            if topic in self.exclude_already:
                continue

            do_continue = False
            for tre in self.exclude_regex:
                if tre.match(topic):
                    rospy.loginfo("*** IGNORING topic %s due to exclusion rule" % topic)
                    do_continue = True
                    self.exclude_already.append(topic)
                    break
            if do_continue:
                continue

            # although the collections is not strictly necessary, since MongoDB could handle
            # pure topic names as collection names and we could then use mongodb[topic], we want
            # to have names that go easier with the query tools, even though there is the theoretical
            # possibility of name classes (hence the check)
            collname = topic.replace("/", "_")[1:]
            if collname in self.workers.keys():
                rospy.logwarn("Two converted topic names clash: %s, ignoring topic %s"
                      % (collname, topic))
            else:
                rospy.loginfo("Adding topic %s" % topic)
                self.workers[collname] = self.create_worker(len(self.workers), topic, collname)
                self.topics |= {topic}

    def create_worker(self, idnum, topic, collname):
        msg_class, _, _ = rostopic.get_topic_class(topic, blocking=True)
        logger = None
        if not self.no_specific and msg_class in self.__special_logger:
            loggerClass = self.__special_logger[msg_class]
            try:
                logger = loggerClass(idnum, topic, collname, self.mongodb_host, self.mongodb_port, self.mongodb_name,
                                     self.nodename_prefix)
            except Exception, e:
                rospy.logerr(e.message)

        if logger is None:
            logger = TopicLogger(idnum, topic, collname, self.mongodb_host, self.mongodb_port, self.mongodb_name,
                                 self.nodename_prefix)

        if self.graph_topics:
            self.assert_worker_rrd(collname)
        return logger

    def run(self):
        for worker in self.workers.itervalues():
            worker.start()

        self.graph_thread = Thread(name="RRDGrapherThread", target=self.graph_rrd_thread)
        self.graph_thread.daemon = True
        self.graph_thread.start()
        looping_threshold = timedelta(0, self.stats_looptime, 0)

        while not rospy.is_shutdown() and not self.quit:
            started = datetime.now()

            if self.graph_daemon and self.graph_process.poll() != None:
                print("WARNING: rrdcached died, falling back to non-cached version. Please investigate.")
                self.graph_daemon = False

            self.update_rrd()

            # the following code makes sure we run once per STATS_LOOPTIME, taking
            # varying run-times and interrupted sleeps into account
            td = datetime.now() - started
            while not rospy.is_shutdown() and not self.quit and td < looping_threshold:
                sleeptime = self.stats_looptime - (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6
                if sleeptime > 0:
                    sleep(sleeptime)
                td = datetime.now() - started

    def shutdown(self):
        self.quit = True
        if hasattr(self, "all_topics_timer"):
            self.all_topics_timer.cancel()
        for name, w in self.workers.items():
            w.shutdown()

        if self.graph_daemon:
            self.graph_process.kill()
            self.graph_process.wait()

    def start_all_topics_timer(self):
        if not self.all_topics or self.quit:
            return
        self.all_topics_timer = Timer(self.all_topics_interval, self.update_topics)
        self.all_topics_timer.start()

    def update_topics(self, restart=True):
        if not self.all_topics or self.quit:
            return
        ts = self.ros_master.getPublishedTopics("/")
        topics = set([t for t, t_type in ts if t != "/rosout" and t != "/rosout_agg"])
        new_topics = topics - self.topics
        self.subscribe_topics(new_topics)
        if restart:
            self.start_all_topics_timer()

    def get_memory_usage_for_pid(self, pid):

        scale = {'kB': 1024, 'mB': 1024 * 1024,
                 'KB': 1024, 'MB': 1024 * 1024}
        try:
            f = open("/proc/%d/status" % pid)
            t = f.read()
            f.close()
        except:
            return (0, 0, 0)

        if t == "":
            return (0, 0, 0)

        try:
            tmp = t[t.index("VmSize:"):].split(None, 3)
            size = int(tmp[1]) * scale[tmp[2]]
            tmp = t[t.index("VmRSS:"):].split(None, 3)
            rss = int(tmp[1]) * scale[tmp[2]]
            tmp = t[t.index("VmStk:"):].split(None, 3)
            stack = int(tmp[1]) * scale[tmp[2]]
            return (size, rss, stack)
        except ValueError:
            return (0, 0, 0)

    def get_memory_usage(self):
        size, rss, stack = 0, 0, 0
        for w in self.workers.itervalues():
            if w.process is not None:
                pmem = self.get_memory_usage_for_pid(w.process.pid)
                size += pmem[0]
                rss += pmem[1]
                stack += pmem[2]
        return (size, rss, stack)

    def assert_rrd(self, file, *data_sources):
        if not os.path.isfile(file) or self.graph_clear:
            rrdtool.create(file, "--step", "10", "--start", "0",
                           # remember that we always need to add the previous RRA time range
                           # hence number of rows is not directly calculated by desired time frame
                           "RRA:AVERAGE:0.5:1:720",     # 2 hours of 10 sec  averages
                           "RRA:AVERAGE:0.5:3:1680",    # 12 hours of 30 sec  averages
                           "RRA:AVERAGE:0.5:30:456",    # 1 day   of  5 min  averages
                           "RRA:AVERAGE:0.5:180:412",   # 7 days  of 30 min  averages
                           "RRA:AVERAGE:0.5:720:439",   # 4 weeks of  2 hour averages
                           "RRA:AVERAGE:0.5:8640:402",  # 1 year  of  1 day averages
                           "RRA:MIN:0.5:1:720",
                           "RRA:MIN:0.5:3:1680",
                           "RRA:MIN:0.5:30:456",
                           "RRA:MIN:0.5:180:412",
                           "RRA:MIN:0.5:720:439",
                           "RRA:MIN:0.5:8640:402",
                           "RRA:MAX:0.5:1:720",
                           "RRA:MAX:0.5:3:1680",
                           "RRA:MAX:0.5:30:456",
                           "RRA:MAX:0.5:180:412",
                           "RRA:MAX:0.5:720:439",
                           "RRA:MAX:0.5:8640:402",
                           *data_sources)

    def graph_rrd_thread(self):
        graphing_threshold = timedelta(0, STATS_GRAPHTIME - STATS_GRAPHTIME * 0.01, 0)
        first_run = True

        while not rospy.is_shutdown() and not self.quit:
            started = datetime.now()

            if not first_run:
                self.graph_rrd()
            first_run = False

            # the following code makes sure we run once per STATS_LOOPTIME, taking
            # varying run-times and interrupted sleeps into account
            td = datetime.now() - started
            while not rospy.is_shutdown() and not self.quit and td < graphing_threshold:
                sleeptime = STATS_GRAPHTIME - (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6
                if sleeptime > 0:
                    sleep(sleeptime)
                td = datetime.now() - started

    def graph_rrd(self):
        time_started = datetime.now()
        rrdtool.graph(["%s/logstats.png" % self.graph_dir,
                       "--start=-600", "--end=-10",
                       "--disable-rrdtool-tag", "--width=560",
                       "--font", "LEGEND:10:", "--font", "UNIT:8:",
                       "--font", "TITLE:12:", "--font", "AXIS:8:",
                       "--title=MongoDB Logging Stats",
                       "--vertical-label=messages/sec",
                       "--slope-mode"]
                      + (self.graph_daemon and self.graph_daemon_args or []) +
                      ["DEF:qsize=%s/logstats.rrd:qsize:AVERAGE:step=10" % self.graph_dir,
                       "DEF:in=%s/logstats.rrd:in:AVERAGE:step=10" % self.graph_dir,
                       "DEF:out=%s/logstats.rrd:out:AVERAGE:step=10" % self.graph_dir,
                       "DEF:drop=%s/logstats.rrd:drop:AVERAGE:step=10" % self.graph_dir,
                       "LINE1:qsize#FF7200:Queue Size",
                       "GPRINT:qsize:LAST:Current\\:%8.2lf %s",
                       "GPRINT:qsize:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:qsize:MAX:Maximum\\:%8.2lf %s\\n",
                       "LINE1:in#503001:In",
                       "GPRINT:in:LAST:        Current\\:%8.2lf %s",
                       "GPRINT:in:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:in:MAX:Maximum\\:%8.2lf %s\\n",
                       "LINE1:out#EDAC00:Out",
                       "GPRINT:out:LAST:       Current\\:%8.2lf %s",
                       "GPRINT:out:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:out:MAX:Maximum\\:%8.2lf %s\\n",
                       "LINE1:drop#506101:Dropped",
                       "GPRINT:drop:LAST:   Current\\:%8.2lf %s",
                       "GPRINT:drop:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:drop:MAX:Maximum\\:%8.2lf %s\\n"])

        if self.graph_topics:
            for _, w in self.workers.items():
                rrdtool.graph(["%s/%s.png" % (self.graph_dir, w.collname),
                               "--start=-600", "--end=-10",
                               "--disable-rrdtool-tag", "--width=560",
                               "--font", "LEGEND:10:", "--font", "UNIT:8:",
                               "--font", "TITLE:12:", "--font", "AXIS:8:",
                               "--title=%s" % w.topic,
                               "--vertical-label=messages/sec",
                               "--slope-mode"]
                              + (self.graph_daemon and self.graph_daemon_args or []) +
                              ["DEF:qsize=%s/%s.rrd:qsize:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "DEF:in=%s/%s.rrd:in:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "DEF:out=%s/%s.rrd:out:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "DEF:drop=%s/%s.rrd:drop:AVERAGE:step=10" % (self.graph_dir, w.collname),
                               "LINE1:qsize#FF7200:Queue Size",
                               "GPRINT:qsize:LAST:Current\\:%8.2lf %s",
                               "GPRINT:qsize:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:qsize:MAX:Maximum\\:%8.2lf %s\\n",
                               "LINE1:in#503001:In",
                               "GPRINT:in:LAST:        Current\\:%8.2lf %s",
                               "GPRINT:in:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:in:MAX:Maximum\\:%8.2lf %s\\n",
                               "LINE1:out#EDAC00:Out",
                               "GPRINT:out:LAST:       Current\\:%8.2lf %s",
                               "GPRINT:out:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:out:MAX:Maximum\\:%8.2lf %s\\n",
                               "LINE1:drop#506101:Dropped",
                               "GPRINT:drop:LAST:   Current\\:%8.2lf %s",
                               "GPRINT:drop:AVERAGE:Average\\:%8.2lf %s",
                               "GPRINT:drop:MAX:Maximum\\:%8.2lf %s\\n"])

        rrdtool.graph(["%s/logmemory.png" % self.graph_dir,
                       "--start=-600", "--end=-10",
                       "--disable-rrdtool-tag", "--width=560",
                       "--font", "LEGEND:10:", "--font", "UNIT:8:",
                       "--font", "TITLE:12:", "--font", "AXIS:8:",
                       "--title=ROS MongoLog Memory Usage",
                       "--vertical-label=bytes",
                       "--slope-mode"]
                      + (self.graph_daemon and self.graph_daemon_args or []) +
                      ["DEF:size=%s/logmemory.rrd:size:AVERAGE:step=10" % self.graph_dir,
                       "DEF:rss=%s/logmemory.rrd:rss:AVERAGE:step=10" % self.graph_dir,
                       "AREA:size#FF7200:Total",
                       "GPRINT:size:LAST:   Current\\:%8.2lf %s",
                       "GPRINT:size:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:size:MAX:Maximum\\:%8.2lf %s\\n",
                       "AREA:rss#503001:Resident",
                       "GPRINT:rss:LAST:Current\\:%8.2lf %s",
                       "GPRINT:rss:AVERAGE:Average\\:%8.2lf %s",
                       "GPRINT:rss:MAX:Maximum\\:%8.2lf %s\\n"])
        time_elapsed = datetime.now() - time_started
        rospy.logdebug("Generated graphs, took %s" % time_elapsed)

    def init_rrd(self):
        self.assert_rrd("%s/logstats.rrd" % self.graph_dir,
                        "DS:qsize:GAUGE:30:0:U",
                        "DS:in:COUNTER:30:0:U",
                        "DS:out:COUNTER:30:0:U",
                        "DS:drop:COUNTER:30:0:U")

        self.assert_rrd("%s/logmemory.rrd" % self.graph_dir,
                        "DS:size:GAUGE:30:0:U",
                        "DS:rss:GAUGE:30:0:U",
                        "DS:stack:GAUGE:30:0:U")

        self.graph_args = []
        if self.graph_daemon:
            self.graph_sockfile = mktemp(prefix="rrd_", suffix=".sock")
            self.graph_pidfile = mktemp(prefix="rrd_", suffix=".pid")
            rospy.loginfo("Starting rrdcached -l unix:%s -p %s -b %s -g" %
                  (self.graph_sockfile, self.graph_pidfile, self.graph_dir))
            self.graph_process = subprocess.Popen(["/usr/bin/rrdcached",
                                                   "-l", "unix:%s" % self.graph_sockfile,
                                                   "-p", self.graph_pidfile,
                                                   "-b", self.graph_dir,
                                                   "-g"], stderr=subprocess.STDOUT)
            self.graph_daemon_args = ["--daemon", "unix:%s" % self.graph_sockfile]

    def assert_worker_rrd(self, collname):
        self.assert_rrd("%s/%s.rrd" % (self.graph_dir, collname),
                        "DS:qsize:GAUGE:30:0:U",
                        "DS:in:COUNTER:30:0:U",
                        "DS:out:COUNTER:30:0:U",
                        "DS:drop:COUNTER:30:0:U")

    def update_rrd(self):
        # we do not lock here, we are not interested in super-precise
        # values for this, but we do care for high performance processing
        qsize = 0
        time_started = datetime.now()
        in_counter = 0
        out_counter = 0
        drop_counter = 0
        for _, w in self.workers.items():
            in_counter += w.worker_in_counter.count.value
            out_counter += w.worker_out_counter.count.value
            drop_counter += w.worker_drop_counter.count.value
            wqsize = w.queue.qsize()
            qsize += wqsize
            if wqsize > QUEUE_MAXSIZE / 2:
                rospy.logwarn("Excessive queue size %6d: %s" % (wqsize, w.name))

            if self.graph_topics:
                rrdtool.update(["%s/%s.rrd" % (self.graph_dir, w.collname)]
                               + (self.graph_daemon and self.graph_daemon_args or []) +
                               ["N:%d:%d:%d:%d" %
                                (wqsize, w.worker_in_counter.count.value,
                                 w.worker_out_counter.count.value, w.worker_drop_counter.count.value)])

        rrdtool.update(["%s/logstats.rrd" % self.graph_dir]
                       + (self.graph_daemon and self.graph_daemon_args or []) +
                       ["N:%d:%d:%d:%d" %
                        (qsize, in_counter, out_counter, drop_counter)])

        rrdtool.update(["%s/logmemory.rrd" % self.graph_dir]
                       + (self.graph_daemon and self.graph_daemon_args or []) +
                       ["N:%d:%d:%d" % self.get_memory_usage()])

        time_elapsed = datetime.now() - time_started
        rospy.logdebug("Updated graphs, total queue size %d, dropped %d, took %s" %
                       (qsize, drop_counter, time_elapsed))


def main(argv):
    parser = OptionParser()
    parser.usage += " [TOPICs...]"
    parser.add_option("--nodename-prefix", dest="nodename_prefix",
                      help="Prefix for worker node names", metavar="ROS_NODE_NAME",
                      default="")
    parser.add_option("--mongodb-host", dest="mongodb_host",
                      help="Hostname of MongoDB", metavar="HOST",
                      default="localhost")
    parser.add_option("--mongodb-port", dest="mongodb_port",
                      help="Hostname of MongoDB", type="int",
                      metavar="PORT", default=27017)
    parser.add_option("--mongodb-name", dest="mongodb_name",
                      help="Name of DB in which to store values",
                      metavar="NAME", default="roslog")
    parser.add_option("-a", "--all-topics", dest="all_topics", default=False,
                      action="store_true",
                      help="Log all existing topics (still excludes /rosout, /rosout_agg)")
    parser.add_option("--all-topics-interval", dest="all_topics_interval", default=5,
                      help="Time in seconds between checks for new topics", type="int")
    parser.add_option("-x", "--exclude", dest="exclude",
                      help="Exclude topics matching REGEX, may be given multiple times",
                      action="append", type="string", metavar="REGEX", default=[])
    parser.add_option("--graph-topics", dest="graph_topics", default=False,
                      action="store_true",
                      help="Write graphs per topic")
    parser.add_option("--graph-clear", dest="graph_clear", default=False,
                      action="store_true",
                      help="Remove existing RRD files.")
    parser.add_option("--graph-dir", dest="graph_dir", default=".",
                      help="Directory in which to create the graphs")
    parser.add_option("--graph-daemon", dest="graph_daemon", default=False,
                      action="store_true",
                      help="Use rrddaemon.")
    parser.add_option("--no-specific", dest="no_specific", default=False,
                      action="store_true", help="Disable specific loggers")
    parser.add_option("--stats-looptime", dest="stats_looptime", default=0,
                      type=int, help="Record stats every x seconds")
    (options, args) = parser.parse_args()

    if not options.all_topics and len(args) == 0:
        parser.print_help()
        return

    try:
        rosgraph.masterapi.Master(NODE_NAME_TEMPLATE % options.nodename_prefix).getPid()
    except socket.error:
        rospy.logerr("Failed to communicate with master")

    mongowriter = MongoWriter(topics=rospy.myargv(args), graph_topics=options.graph_topics,
                              graph_dir=options.graph_dir,
                              graph_clear=options.graph_clear,
                              graph_daemon=options.graph_daemon,
                              all_topics=options.all_topics,
                              all_topics_interval=options.all_topics_interval,
                              exclude_topics=options.exclude,
                              mongodb_host=options.mongodb_host,
                              mongodb_port=options.mongodb_port,
                              mongodb_name=options.mongodb_name,
                              no_specific=options.no_specific,
                              nodename_prefix=options.nodename_prefix,
                              stats_looptime=options.stats_looptime)

    mongowriter.run()
    mongowriter.shutdown()


if __name__ == "__main__":
    main(sys.argv)
