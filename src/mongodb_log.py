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
from argparse import ArgumentParser
import logger_registry

BACKLOG_WARN_LIMIT = 100
STATS_GRAPHTIME = 60
PACKAGE_NAME = 'mongodb_log'
NODE_NAME = 'mongodb_log'
NODE_NAME_TEMPLATE = '%smongodb_log'
QUEUE_MAXSIZE = 100


import roslib
roslib.load_manifest(PACKAGE_NAME)
import sys
import time
import string
import socket
import abc
import subprocess
from multiprocessing import Process, Lock, Queue, current_process
from Queue import Empty
from datetime import datetime, timedelta
import genpy
import rospy
import rosgraph.masterapi
import rostopic
from pymongo import Connection, SLOW_ONLY
from pymongo.errors import InvalidDocument, InvalidStringData
from random import randint


class Counter(object):
    def __init__(self, value=None, lock=True):
        self.__value = value if value is not None else 0
        self.mutex = Lock()

    def increment(self, by=1):
        with self.mutex:
            self.__value += by

    def value(self):
        with self.mutex:
            return self.__value


class MongoDBLogger(object):
    """
    This is the abstract base class for all loggers, who want to log to a mongodb.
    This class stores all information necessary to connect to a mongodb.
    It does _NOT_ create a connection to the MongoDB, as this might be done by special loggers.
    This class does also _NOT_ subscribe to a Topic for the same reason.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):
        """
        Creates a new instance of the MongoDBLogger and stores all necessary information to initialize a logger.

        :param name: The name of this logger used in logging output
        :type name: str
        :param topic: The topic to log messages from
        :type topic: str
        :param collname: The name of the collection in the MongoDB to log messages to.
        :type collnae: str
        :param mongodb_host:The hostname or ip of the host running the MongoDB
        :type mongodb_host: str
        :param mongodb_port: The port on which the MongoDB is running on
        :type mongodb_port: int
        :param mongodb_name: The name of the databse to use for logging
        :type mongodb_name: str
        :return: A new MonoDBLogger instance
        :rtype: MongoDBLogger
        """
        self.name = name
        self.topic = topic
        self.collname = collname
        self.mongodb_host = mongodb_host
        self.mongodb_port = mongodb_port
        self.mongodb_name = mongodb_name
        self.quit = False

    def shutdown(self):
        """
        Shuts down the running process and joins it back to the calling process.
        """
        if not self.is_quit():
            self.quit = True

    def is_quit(self):
        """
        Checks if this process should quit
        :return: True if this process should quit
        :rtype: bool
        """
        return self.quit or rospy.is_shutdown()

    @property
    def process(self):
        """
        Returns the process object of this Logger.
        This property is designed to be overwritten by subclasses to return the correct process object.

        :return: The process doing the actual logging
        :rtype: Process
        """
        return self

    def start(self):
        """
        Starts the process by calling the `run()`-Method.
        HACK: This is just a hack because I don't know if it will be replaced by processes.
        """
        self.run()

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

    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name, max_queuesize=QUEUE_MAXSIZE):
        MongoDBLogger.__init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name)
        self.worker_out_counter = Counter()
        self.worker_in_counter = Counter()
        self.worker_drop_counter = Counter()
        self.queue = Queue(max_queuesize)

    def _init(self):
        """
        This method initializes this process.
        It initializes the connection to the MongoDB and subscribes to the topic.
        """
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
        This method does the actual logging.
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
            self.queue.get_nowait()
        rospy.logdebug("STOPPED: %s" % self.name)

    def shutdown(self):
        self.queue.put("shutdown")
        super(TopicLogger, self).shutdown()

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
            self.quit = True
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
            self.quit = True


class CPPLogger(MongoDBLogger):
    """
    This class implements a base class for spezialized loggers using other languages (like C++).
    """

    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name, cpp_logger,
                 additional_parameters):
        """
        Creates a new instance of the CPPLogger.
        :param name: The name of this logger used in logging output
        :type name: str
        :param topic: The topic to log messages from
        :type topic: str
        :param collname: The name of the collection in the MongoDB to log messages to.
        :type collnae: str
        :param mongodb_host:The hostname or ip of the host running the MongoDB
        :type mongodb_host: str
        :param mongodb_port: The port on which the MongoDB is running on
        :type mongodb_port: int
        :param mongodb_name: The name of the databse to use for logging
        :type mongodb_name: str
        :param cpp_logger: The path to the executable to use for the actual logging
        :type cpp_logger: str
        :param additional_parameters: Optional additional parameters to give to the logger
        :type additional_parameters: [str]
        :return: A new CPPLogger instance
        :rtype: CPPLogger
        """
        super(CPPLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name)
        self.worker_out_counter = Counter()
        self.worker_in_counter = Counter()
        self.worker_drop_counter = Counter()
        self.qsize = 0
        self.additional_parameters = additional_parameters if additional_parameters is not None else []
        self.cpp_logger = cpp_logger
        self.__process = None
        self.nodename = self.name.lower().replace("/", "_")

    @property
    def process(self):
        return self.__process

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


def create_worker(name, topic, mongodb_host, mongodb_port, mongodb_name, collname, no_specific=False):
        msg_class, _, _ = rostopic.get_topic_class(topic, blocking=True)
        if no_specific:
            loggerClass = TopicLogger
        else:
            loggerClass = logger_registry.get_logger(msg_class, TopicLogger)
        return loggerClass(name, topic, collname, mongodb_host, mongodb_port, mongodb_name)


def main(argv):
    parser = ArgumentParser(description="Start a new logger")
    parser.add_argument("--nodename-prefix", dest="nodename_prefix", type=str,
                        help="Prefix for worker node names", metavar="ROS_NODE_NAME",
                        default="")
    parser.add_argument("--mongodb-host", dest="mongodb_host", type=str,
                        help="Hostname of MongoDB", metavar="HOST",
                        default="localhost")
    parser.add_argument("--mongodb-port", dest="mongodb_port",
                       help="Hostname of MongoDB", type=int,
                       metavar="PORT", default=27017)
    parser.add_argument("--mongodb-name", dest="mongodb_name", type=str,
                      help="Name of DB in which to store values",
                      metavar="NAME", default="roslog")
    parser.add_argument("--no-specific", dest="no_specific", default=False,
                        action="store_true", help="Disable specific loggers")
    parser.add_argument("topic", metavar="T", type=str, nargs=1,
                        help="The topic to subscribe to")
    args = parser.parse_args(argv)

    # Initialize the rospy node
    topic = args.topic[0]
    name = "%sMongoDB_logger_%s" % (args.nodename_prefix, topic)
    collection_name = topic.replace("/", "_") if topic[0] != "/" else topic[1:].replace("/", "_")
    nodename = "%smongodb_logger_%s" % (args.nodename_prefix, collection_name)

    # Initialize the node
    rospy.init_node(nodename, anonymous=False)

    try:
        rosgraph.masterapi.Master(NODE_NAME_TEMPLATE % args.nodename_prefix).getPid()
    except socket.error:
        rospy.logerr("Failed to communicate with master")

    try:
        import special_loggers
    except:
        pass

    # Create the worker
    logger = create_worker(name, topic, args.mongodb_host, args.mongodb_port,
                  args.mongodb_name, collection_name, args.no_specific)

    try:
        # Start the logger
        logger.start()
    except KeyboardInterrupt:
        rospy.loginfo("Keyboard interrupt received, shutting down!")
    finally:
        logger.shutdown()

if __name__ == "__main__":
    main(rospy.myargv()[1:])
