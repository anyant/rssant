import sys
import logging
import faulthandler

from loguru import logger as loguru_logger

from .loguru_patch import loguru_patch, InterceptHandler


LOG_FORMAT = (
    "<level>{level:1.1s}</level> "
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "<cyan>{name}:{line:<4d}</cyan> <level>{message}</level>"
)

LOGURU_HANDLER = {
    "sink": sys.stdout,
    "colorize": True,
    "format": LOG_FORMAT,
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


def configure_logging(level=logging.INFO):
    faulthandler.enable()
    loguru_patch()
    # https://stackoverflow.com/questions/45522159/dont-log-certificate-did-not-match-expected-hostname-error-messages
    logging.getLogger('urllib3.connection').setLevel(logging.CRITICAL)
    logging.getLogger('readability.readability').setLevel(logging.WARNING)
    logging.basicConfig(handlers=[InterceptHandler()], level=level)
    loguru_logger.configure(handlers=[LOGURU_HANDLER])


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
