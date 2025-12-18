from search_dragon import logger as getlogger
import logging
from os import getenv

# Set the logging config
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# Create a logger
llevel = getenv("LOCUTUS_LOGLEVEL", logging.WARN)
logger = getlogger(logformat=LOGGING_FORMAT, loglevel=llevel)
logger.info(f"Logger instanced with level: {llevel}")
