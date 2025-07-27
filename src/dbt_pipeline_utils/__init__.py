import logging
class CustomFormatter(logging.Formatter):
    RESET = "\033[0m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    BOLD_RED = "\033[1;31m"
    BLUE = "\033[34m"
    GREEN = "\033[32m"

    FORMATS = {
        logging.DEBUG: GREEN + "%(levelname)s: %(message)s" + RESET,
        logging.INFO: RESET + "%(levelname)s: %(message)s" + RESET,
        logging.WARNING: BLUE + "%(levelname)s: %(message)s" + RESET,
        logging.ERROR: RED + "%(levelname)s: %(message)s" + RESET,
        logging.CRITICAL: BOLD_RED + "%(levelname)s: %(message)s" + RESET,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.RESET + "%(message)s" + self.RESET)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

console_handler.setFormatter(CustomFormatter())

logger.addHandler(console_handler)
