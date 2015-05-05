import logging

_loggers = {}

def getLogger(name):
  if name in _loggers:
    return _loggers[name]

  logging.basicConfig()
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.DEBUG)

  _loggers[name] = logger
  return logger