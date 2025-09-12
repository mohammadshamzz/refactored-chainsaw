
from logging_config import get_log_tail, get_recent_logs

def get_recent_logs_tail(log_file: str, lines: int = 100):
    return get_log_tail(log_file, lines)

def get_recent_logs_by_hours(hours: int = 1):
    return get_recent_logs(hours=hours)
