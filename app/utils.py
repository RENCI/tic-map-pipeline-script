import logging


def getLogger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
    logger.addHandler(handler)
    logger.propagate = False
    handler.setFormatter(formatter)
    return logger


def check_for_dups(lst: list) -> tuple:
    seen = set()
    dups = []
    for x in lst:
        if x in seen:
            dups.append(x)
        else:
            seen.add(x)

    return (dups, seen)
