import logging
import sys

# Define log format
LOG_FORMAT = "%(levelname)s - %(message)s"

# Configure logging once when the package is imported
logging.basicConfig(
    level=logging.INFO,  # Default level
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),  # Print to console
        # logging.FileHandler("pipeline.log")  # Log to file
    ]
)

# Create a default logger for the package
logger = logging.getLogger("dbt_pipeline_utils")
logger.debug("Logging is set up.")
