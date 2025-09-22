
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

def define_logger(name:str,log_file:str=None)->logging.Logger:

    """
        log handler for console and file logs
    """
    
    # create the logger
    logger=logging.getLogger(name)
    #set the logger level
    logger.setLevel(logging.INFO)
    #Create  file handler and set level
    file_handler=logging.FileHandler(log_file)
    file_handler.setLevel()
    #create console handler and set level
    console_handler=logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    #Create console handler and set level
    #Create log formatters

    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    #console formatter
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    
    #add formatters to handlers
    #file formatter
    file_handler.setFormatter(file_formatter)
    #console formatter
    console_handler.setFormatter(console_formatter)

    #add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

     # File handler (if log_file is provided)
    if log_file:
        # Create logs directory if it doesn't exist
        log_path = Path(log_file).parent
        log_path.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
            
    return logger

