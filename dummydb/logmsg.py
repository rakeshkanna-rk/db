import time
import shutil
from textPlay.colors import *
from typing import Any

class Status:
    INFO = f"{BG_BLUE}{WHITE} INFO {RESET}"
    ERROR = f"{BG_RED}{WHITE} ERROR {RESET}"
    WARN = f"{BG_YELLOW}{BLACK} WARN {RESET}"
    DONE = f"{BG_GREEN}{BLACK} DONE {RESET}"    


def log(message, status:Status|Any , add_time: bool = True, print_able:bool= True):

    ctime = time.strftime("%I:%M %p")
    term = shutil.get_terminal_size().columns

    space = lambda: 9 if status==Status.ERROR else 8
    set_time = f"{DIM}{ctime if add_time else ''}{RESET}"

    if not print_able:
        return f" {status} {message}{' '* (term - len(ctime) - len(message) - 6)}{ctime if add_time else ''}"
    
    print(f" {status} {message}{' '* (term - len(ctime) - len(message) - space())}{set_time}")
    
    
    