__author__ = 'Torben Hansing'


__SPECIAL_LOGGERS = {}

def register_logger(message, logger):
    """
    Registers a special logger for the given message type.
    The `message` must be the python class of the message type.

    :param message: The message type to register the logger for
    :type message: Message
    :param logger: The special logger to Register
    :type logger: LoggerProcess
    """
    global __SPECIAL_LOGGERS
    __SPECIAL_LOGGERS[message] = logger


def get_logger(message, default=None):
    """
    Returns the special logger for the given message type.
    If no special logger is found, the default will be returned.

    :param message: The message type to search the logger for
    :type message: Message
    :param default: An optional fallback default to return if no special logger is found
    :type default: MongoDBLogger | None
    :return: The special logger registered for the given message type or the default
    :rtype: MongoDBLogger|None
    """
    if message in __SPECIAL_LOGGERS:
        return __SPECIAL_LOGGERS[message]
    return default
