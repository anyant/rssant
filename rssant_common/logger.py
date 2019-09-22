import sys
import logging
import faulthandler

from loguru import logger as loguru_logger

from .loguru_patch import loguru_patch, InterceptHandler


LOG_FORMAT = "%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

LOGURU_LOG_FORMAT = (
    "<level>{level:1.1s}</level> "
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "<cyan>{name}:{line:<4d}</cyan> <level>{message}</level>"
)

LOGURU_HANDLER = {
    "sink": sys.stdout,
    "colorize": True,
    "format": LOGURU_LOG_FORMAT,
    # diagnose and backtrace will cause deadlock, must disable it!
    "diagnose": False,
    "backtrace": False,
}

# How diagnose and backtrace cause dead lock:
#
# thread-1: log a message
# with storage.lock:
#     # thread-1 @here while thread-2 hold logging.lock
#     with logging.lock:
#         with loguru.lock:
#             write_message()
#
# thread-2: log a exception
# with logging.lock:
#     with loguru.lock:
#         # diagnose exception, call __repr__ or get property value
#         # thread-2 @here while thread-1 hold storage.lock
#         with storage.lock:
#             return some_value
#


def configure_logging(level=logging.INFO, enable_loguru=True):
    faulthandler.enable()
    # https://stackoverflow.com/questions/45522159/dont-log-certificate-did-not-match-expected-hostname-error-messages
    logging.getLogger('urllib3.connection').setLevel(logging.CRITICAL)
    logging.getLogger('readability.readability').setLevel(logging.WARNING)
    if enable_loguru:
        loguru_patch()
        logging.basicConfig(
            handlers=[InterceptHandler()], level=level,
            format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
        loguru_logger.configure(handlers=[LOGURU_HANDLER])
    else:
        logging.basicConfig(
            level=level, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)


if __name__ == '__main__':
    configure_logging()
    LOG = logging.getLogger(__name__)
    LOG.debug('debug log')
    LOG.info('info log')
    LOG.warning('warning log')
    LOG.error('error log')
    try:
        raise ValueError('exception log')
    except Exception as ex:
        LOG.exception(ex)
