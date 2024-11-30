import os
from .utils import *


def check(cmd: str, keyword: str) -> bool:
    return cmd.startswith(keyword)

def cmdline():
    db = None

    loop = True
    while loop:
        try:
            cmd = input(f"{f"{CYAN}{db.getdb()} {RESET}" if db else ""}>>> ").strip()

            if cmd.upper() == "EXIT":
                raise KeyboardInterrupt
            else:

                if check(cmd.upper(), "CREATE"):
                    db = createDB(cmd)
                elif check(cmd.upper(), "USE"):
                    if (
                        cmd.split()[-1].endswith(".json") or 
                        cmd.split()[-1].endswith(".bson")
                        ):
                        if os.path.exists(cmd.split()[-1]):
                            table = useTable(cmd, db)
                    else:
                        if (
                            len(cmd.split()) == 3 and 
                            cmd.split()[-1].strip().startswith(".") and 
                            cmd.split()[1].upper() == "DB" and 
                            os.path.exists(cmd.split()[-1])
                            ):
                            db = JsonDB(cmd.split()[-1])
                        elif not os.path.exists(cmd.split()[-1]):
                            log("Database does not exist", Status.ERROR)
                        else:
                            log("Prefer using dot notation for database", Status.WARN)

                elif check(cmd.upper(), "INSERT INTO"):
                    insertValue(cmd, db)

                elif check(cmd.upper(), "SELECT"):
                    selectValue(cmd, db)

                elif check(cmd.upper(), "DEL"):
                    deleteValue(cmd, db)
                
                elif not cmd:
                    continue

                else:
                    log("Unknown Command", Status.WARN)

        except FileNotFoundError:
            log("File does not exist, please check the directory.", Status.ERROR)
        
        except KeyboardInterrupt:
            loop = False
            print("\nExiting...")
            
