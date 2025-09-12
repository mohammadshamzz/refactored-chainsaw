import logging
import logging.handlers
import os
from datetime import datetime

def setup_logging(log_level=logging.INFO, log_dir="logs"):
    """
    Configure logging for the trading agent with both file and console output.
    
    Args:
        log_level: Logging level (default: INFO)
        log_dir: Directory to store log files (default: "logs")
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    root_logger.addHandler(console_handler)
    
    # Main log file handler (rotating)
    main_log_file = os.path.join(log_dir, "trading_agent.log")
    file_handler = logging.handlers.RotatingFileHandler(
        main_log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)
    
    # Error log file handler (errors only)
    error_log_file = os.path.join(log_dir, "trading_errors.log")
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(error_handler)
    
    # Daily log file handler (one file per day)
    daily_log_file = os.path.join(log_dir, f"daily_{datetime.now().strftime('%Y%m%d')}.log")
    daily_handler = logging.FileHandler(daily_log_file)
    daily_handler.setLevel(log_level)
    daily_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(daily_handler)
    
    # Add a custom success level for trade confirmations
    logging.SUCCESS = 25  # Between INFO (20) and WARNING (30)
    logging.addLevelName(logging.SUCCESS, 'SUCCESS')
    
    def success(self, message, *args, **kwargs):
        if self.isEnabledFor(logging.SUCCESS):
            self._log(logging.SUCCESS, message, args, **kwargs)
    
    logging.Logger.success = success
    
    logging.info(f"Logging configured. Log files will be written to: {os.path.abspath(log_dir)}")
    logging.info(f"Main log: {main_log_file}")
    logging.info(f"Error log: {error_log_file}")
    logging.info(f"Daily log: {daily_log_file}")
    
    return root_logger

def get_log_tail(log_file="logs/trading_agent.log", lines=50):
    """
    Get the last N lines from a log file.
    
    Args:
        log_file: Path to the log file
        lines: Number of lines to return (default: 50)
    
    Returns:
        List of log lines or empty list if file doesn't exist
    """
    try:
        with open(log_file, 'r') as f:
            return f.readlines()[-lines:]
    except FileNotFoundError:
        return []
    except Exception as e:
        logging.error(f"Error reading log file {log_file}: {e}")
        return []

def get_recent_logs(log_dir="logs", hours=1):
    """
    Get recent log entries from the last N hours.
    
    Args:
        log_dir: Directory containing log files
        hours: Number of hours to look back (default: 1)
    
    Returns:
        List of recent log entries
    """
    import time
    from datetime import timedelta
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    recent_logs = []
    
    try:
        log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]
        
        for log_file in log_files:
            file_path = os.path.join(log_dir, log_file)
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        # Try to parse timestamp from log line
                        if line.strip():
                            try:
                                timestamp_str = line.split(' - ')[0]
                                log_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                                if log_time >= cutoff_time:
                                    recent_logs.append(line.strip())
                            except (ValueError, IndexError):
                                # Skip lines that don't match expected format
                                continue
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")
                continue
                
    except Exception as e:
        logging.error(f"Error accessing log directory {log_dir}: {e}")
    
    return sorted(recent_logs)