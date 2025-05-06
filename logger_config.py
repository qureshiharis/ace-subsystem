import logging
import pytz
from datetime import datetime, timezone

class StockholmFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        stockholm = pytz.timezone("Europe/Stockholm")
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone(stockholm)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

def setup_logger(name):
    handler = logging.StreamHandler()
    formatter = StockholmFormatter(fmt="%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger