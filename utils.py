COLOR_RESET = "\033[0m"
COLOR_RED = "\033[31m"
COLOR_YELLOW = "\033[33m"
COLOR_BLUE = "\033[34m"
COLOR_GREEN = "\033[32m"
DEFAULT_PARTITIONS = 5

def log_error(message):
    print(f"{COLOR_RED}{message}{COLOR_RESET}")


def log_event(message):
    print(f"{COLOR_YELLOW}{message}{COLOR_RESET}")


def log_io(message):
    print(f"{COLOR_BLUE}{message}{COLOR_RESET}")


def log_success(message):
    print(f"{COLOR_GREEN}{message}{COLOR_RESET}")

def read_coordinators(file_path):
    """Reads a list of coordinator addresses from a file, returning a list of strings"""
    import os
    if not os.path.exists(file_path):
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]
