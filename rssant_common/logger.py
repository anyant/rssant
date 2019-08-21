import sys
import logging
import faulthandler
from loguru import logger as loguru_logger


class InterceptHandler(logging.Handler):
    def emit(self, record):
        logger_opt = loguru_logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


LOG_FORMAT = (
    "<level>{level:1.1s}</level> "
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "<cyan>{name}:{line:<4d}</cyan> <level>{message}</level>"
)

LOGURU_HANDLER = {
    "sink": sys.stdout,
    "colorize": True,
    "format": LOG_FORMAT,
    "diagnose": False,
    "backtrace": False,
}


def configure_logging(level=logging.INFO):
    faulthandler.enable()
    # https://stackoverflow.com/questions/45522159/dont-log-certificate-did-not-match-expected-hostname-error-messages
    logging.getLogger('urllib3.connection').setLevel(logging.CRITICAL)
    logging.getLogger('readability.readability').setLevel(logging.WARNING)
    logging.basicConfig(handlers=[InterceptHandler()], level=level)
    loguru_logger.configure(handlers=[LOGURU_HANDLER])
