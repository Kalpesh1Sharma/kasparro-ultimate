import logging
import sys

def setup_logging():
    logger = logging.getLogger()
    if logger.handlers:
        return logger
    
    handler = logging.StreamHandler(sys.stdout)
    
    try:
        from pythonjsonlogger import jsonlogger
        formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    except ImportError:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
