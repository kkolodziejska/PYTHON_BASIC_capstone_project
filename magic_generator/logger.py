import logging
import sys
from magic_generator import ERRORS


def logger_error(error: int, *args):
    logging.error(ERRORS[error].format(*args))
    sys.exit(1)
