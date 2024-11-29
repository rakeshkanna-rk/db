import os
from .utils import *


def check(cmd: str, keyword: str) -> bool:
    return cmd.startswith(keyword)

def cmdline():
    db = None

    loop = True
    while loop:
        cmd = input(">>> ").strip()
        
        if cmd.upper() == "EXIT":
            exit()
        else:
            if check(cmd.upper(), "CREATE"):
                db = createDB(cmd)

            elif check(cmd.upper(), "USE"):
                if cmd.split()[-1].endswith(".json") or cmd.split()[-1].endswith(".bson"):
                    if os.path.exists(cmd.split()[-1]):
                        useTable(cmd, db)
                else:
                    if len(cmd.split()) == 3 and cmd.split()[-1].startswith(".") and cmd.split()[2].upper() == "DB":
                        db = cmd.strip()[2]
                    else:
                        log("Prefer using dot notation for database", Status.WARN)
                        
                    

            elif check(cmd.upper(), "INSERT INTO"):
                insertValue(cmd, db)
            
            elif check(cmd.upper(), "SELECT"):
                selectValue(cmd, db)


            else:
                print("ERROR")

if __name__ == '__main__':
    cmdline()