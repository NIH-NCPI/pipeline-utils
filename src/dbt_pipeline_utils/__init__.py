from search_dragon import logger as getlogger
import logging
from pathlib import Path
from dotenv import load_dotenv
from os import getenv

def find_repo_root():
    current = Path.cwd()

    # Look for a requirements.txt file to identify the repo root
    for parent in [current, *current.parents]:
        if any(parent.glob("*requirements.txt")):  # Match requirements.txt
            return parent.resolve()

    raise RuntimeError("Could not find repo root containing requirements.txt")


# Explicitly define the path to the .env file
repo_root = find_repo_root()
env_path = repo_root / ".env"

# Load the .env file
success = load_dotenv(dotenv_path=env_path, override=True)

# Normalize USE_RICH
USE_RICH = getenv("USE_RICH", "false").strip().lower() == "true"

# Map SEARCH_DRAGON_LOGLEVEL to an integer log level
SD_LOGLEVEL = getenv("SEARCH_DRAGON_LOGLEVEL", "INFO").strip().upper()
sdllevel = logging._nameToLevel.get(SD_LOGLEVEL, logging.INFO)

# Map PUTILS_LOGLEVEL to an integer log level
LOGLEVEL_STRING = getenv("PUTILS_LOGLEVEL", "INFO").strip().upper()
llevel = logging._nameToLevel.get(LOGLEVEL_STRING, logging.INFO)

# Set the logging format
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
TIME_FORMAT = "%H:%M:%S"  # Time format: hours, minutes, seconds

# Conditional import of rich
if USE_RICH:
    from rich.logging import RichHandler
    handler = RichHandler(
        show_time=False,  # Disable Rich's own timestamp
        show_level=True,  # Show log level
        show_path=False,  # Disable source file path in logs
        rich_tracebacks=True,  # Optional: Enable rich traceback formatting
    )
    handler.setFormatter(logging.Formatter(fmt=LOGGING_FORMAT, datefmt=TIME_FORMAT))
else:
    handler = logging.StreamHandler()  # Default StreamHandler
    handler.setFormatter(logging.Formatter(fmt=LOGGING_FORMAT, datefmt=TIME_FORMAT))


logger = logging.getLogger("search_dragon")
logger.setLevel(sdllevel)
logger.setLevel(llevel)
logger.addHandler(handler)

