import sys
import logging
import faulthandler
from loguru import logger as loguru_logger


class InterceptHandler(logging.Handler):
    def emit(self, record):
        logger_opt = loguru_logger.opt(depth=7, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


LOG_FORMAT = (
    "<level>{level:1.1s}</level> "
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "<cyan>{name}:{line:<4d}</cyan> <level>{message}</level>"
)
LOGURU_HANDLER = {"sink": sys.stderr, "colorize": True, "format": LOG_FORMAT}


def configure_logging(level=logging.INFO):
    faulthandler.enable()
    logging.getLogger('readability.readability').setLevel(logging.WARNING)
    logging.basicConfig(handlers=[InterceptHandler()], level=level)
    loguru_logger.configure(handlers=[LOGURU_HANDLER])
