from roslib.packages import find_node
from mongodb_log import CPPLogger, PACKAGE_NAME
import logger_registry
import rospy


__author__ = 'Torben Hansing'


class TFLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):
        node_path = find_node(PACKAGE_NAME, "mongodb_log_tf")
        # Log only when the preceeding entry of that
        # transformation had at least 0.100 vectorial and radial
        # distance to its predecessor transformation, but at least
        # every second.
        additional_parameters = ["-k" "0.100" "-l" "0.100" "-g" "1"]
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_tf, falling back to generic logger (did not build package?)")

        super(TFLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name, node_path[0],
                                       additional_parameters)

    @classmethod
    def register(cls):
        try:
            from tf.msg import tfMessage
            logger_registry.register_logger(tfMessage, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type tfMessage")
        try:
            from tf2_msgs.msg import TFMessage
            logger_registry.register_logger(TFMessage, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type TFMessage")


class PointCloudLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):

        node_path = find_node(PACKAGE_NAME, "mongodb_log_pcl")
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_pcl, falling back to generic logger (did not build package?)")
        super(PointCloudLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name,
                                               node_path[0], None)

    @classmethod
    def register(cls):
        try:
            from sensor_msgs.msg import PointCloud
            logger_registry.register_logger(PointCloud, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type PointCloud")


class ImageLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):

        node_path = find_node(PACKAGE_NAME, "mongodb_log_img")
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_img, falling back to generic logger (did not build package?)")
        super(ImageLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name,
                                          node_path[0], None)

    @classmethod
    def register(cls):
        try:
            from sensor_msgs.msg import Image
            logger_registry.register_logger(Image, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type Image")


class CompressedImageLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):
        node_path = find_node(PACKAGE_NAME, "mongodb_log_cimg")
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_cimg, falling back to generic logger (did not build package?)")
        super(CompressedImageLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name,
                                                    node_path[0], None)

    @classmethod
    def register(cls):
        try:
            from sensor_msgs.msg import CompressedImage
            logger_registry.register_logger(CompressedImage, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type CompressedImage")


class DesignatorRequestLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):
        node_path = find_node(PACKAGE_NAME, "mongodb_log_desig")
        additional_parameters = ["-d" "designator-request"]
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_desig, falling back to generic logger (did not build package?)")
        super(DesignatorRequestLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name,
                                                      node_path[0], additional_parameters)

    @classmethod
    def register(cls):
        try:
            from designator_integration_msgs.msg import DesignatorRequest
            logger_registry.register_logger(DesignatorRequest, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type DesignatorRequest")


class DesignatorResponseLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):
        node_path = find_node(PACKAGE_NAME, "mongodb_log_desig")
        additional_parameters = ["-d" "designator-response"]
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_desig, falling back to generic logger (did not build package?)")
        super(DesignatorResponseLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name,
                                                       node_path[0], additional_parameters)

    @classmethod
    def register(cls):
        try:
            from designator_integration_msgs.msg import DesignatorResponse
            logger_registry.register_logger(DesignatorResponse, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type DesignatorResponse")


class DesignatorLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):
        node_path = find_node(PACKAGE_NAME, "mongodb_log_desig")
        additional_parameters = ["-d" "designator"]
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_desig, falling back to generic logger (did not build package?)")
        super(DesignatorLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name,
                                               node_path[0], additional_parameters)

    @classmethod
    def register(cls):
        try:
            from designator_integration_msgs.msg import Designator
            logger_registry.register_logger(Designator, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type Designator")


class TriangleMeshLogger(CPPLogger):
    def __init__(self, name, topic, collname, mongodb_host, mongodb_port, mongodb_name):
        node_path = find_node(PACKAGE_NAME, "mongodb_log_trimesh")
        if not node_path:
            raise RuntimeError("FAILED to detect mongodb_log_trimesh, falling back to generic logger (did not build package?)")
        super(TriangleMeshLogger, self).__init__(name, topic, collname, mongodb_host, mongodb_port, mongodb_name,
                                                 node_path[0], None)

    @classmethod
    def register(cls):
        try:
            from triangle_mesh_msgs.msg import TriangleMesh
            logger_registry.register_logger(TriangleMesh, cls)
        except ImportError:
            rospy.logwarn("Can't register for message type TriangleMesh")


TFLogger.register()
PointCloudLogger.register()
ImageLogger.register()
CompressedImageLogger.register()
DesignatorRequestLogger.register()
DesignatorResponseLogger.register()
DesignatorLogger.register()
TriangleMeshLogger.register()
