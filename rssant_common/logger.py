import sys
import logging
import faulthandler

from loguru import logger as loguru_logger

from rssant.settings import ENV_CONFIG
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
    "diagnose": ENV_CONFIG.debug,
    "backtrace": ENV_CONFIG.debug,
}


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
