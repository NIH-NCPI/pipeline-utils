from search_dragon import logger as getlogger
import logging
from os import getenv

# Check if Rich should be used
USE_RICH = getenv("USE_RICH", "false").lower() == "true"

# Conditional import of rich
if USE_RICH:
    from rich.logging import RichHandler

    handler = RichHandler()  # RichHandler for pretty-printed logs
else:
    handler = logging.StreamHandler()  # Default StreamHandler

# Set the logging format
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# Create and configure the logger
llevel = getenv("LOCUTUS_LOGLEVEL", logging.WARN)  # Default log level if not set
logger = logging.getLogger("search_dragon")
logger.setLevel(llevel)
handler.setFormatter(logging.Formatter(LOGGING_FORMAT))  # Apply format
logger.addHandler(handler)

# Log a message indicating the logger setup
logger.info(
    f"Logger instanced with level: {llevel}, using {'Rich' if USE_RICH else 'standard'} handler."
)
