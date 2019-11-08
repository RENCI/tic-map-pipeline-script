import logging

def getLogger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",
                              "%Y-%m-%d %H:%M:%S")
    logger.addHandler(handler)
    logger.propagate = False
    handler.setFormatter(formatter)
    return logger
