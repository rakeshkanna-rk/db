import time
from textPlay.colors import *
import json

from ..database import JsonDB, BsonDB
from ..logmsg import log, Status

def error(cmd, type, hint):

    hint = hint.upper()
    print(f"ERROR IN COMMAND: {" ".join(cmd)}")

    if type.lower() == "nf":
        print(f"{hint} not found")
    elif type.lower() == "h":
        print(f"Do you mean {hint}?")
    elif type.lower() == "sh":
        print(f"Syntax error: {hint}")
    elif type.lower() == "c":
        print(hint)
    elif type.lower() == "s":
        print(f"Syntax: {hint}")

def use(line):
    print(f"USE {line}")

def nameChecker(name:str, in_name):
    if name.startswith(in_name):
        return True
    else:
        return False

def createDB(cmd:str) -> JsonDB | BsonDB | None:
    cmd:str = cmd.split()

    cmdok = False

    db = cmd[1].upper()

    if (db == "DB" or db== "JDB") and len(cmd) == 2:
        db = JsonDB()
        cmdok = True

    elif db == "BDB" and len(cmd) == 2:
        db = BsonDB()
        cmdok = True
        
    elif db == "DB" or db== "JDB" and len(cmd) == 3:
        if nameChecker(cmd[-1], "."):
            db = JsonDB(cmd[2].lower())
            cmdok = True
        else:
            log("Prefer using dot notation for database", Status.WARN)
        

    elif db == "BDB"  and len(cmd) == 3:
        if nameChecker(cmd[-1], "."):
            db = BsonDB(cmd[2].lower())
            cmdok = True
        else:
            log("Prefer using dot notation for database", Status.WARN)


    else:
        error(cmd, "H", "CREATE DB or CREATE BDB")
        db = None

    if cmdok:
        log("Created Database successfully", Status.DONE)
    else:
        pass

    return db

def useTable(cmd: str, db: JsonDB | BsonDB | None):
    cmd = cmd.split()

    if len(cmd) == 2:
        if not db: db = createDB("CREATE DB")

        try:
            db.usetable(cmd[1])
            
        except Exception as e:
            log(f"Exception: {e}", Status.ERROR)

        return
    else:
        log("Syntax error: USE <TABLE.json>", Status.ERROR)
    

def insertValue(cmd:str , db: JsonDB | BsonDB | None) -> None:

    if len(cmd.split()) == 3 :
        print("Entering Insert mode...")
        print(f"Data format:",MAGENTA,"{'key': {'value'}}",RESET)
        time.sleep(1)

        loop = True
        while loop:
            insert_data = input(f"{GREEN}>>> {RESET}").strip()
            if not insert_data:
                print("Exiting Insert mode...")
                loop = False

            else:
                try:
                    data = json.loads(insert_data)
                except Exception as e:
                    err = ["Invalid JSON format: Use in the format", MAGENTA, "{'{'key': {'value'}}'}", RESET]
                    log(" ".join(err), Status.ERROR)
                    continue

                key = next(iter(data))
                value = data[key]

                if not db: db = createDB("CREATE DB")

                db.insert(key, value, cmd.split()[-1])

def selectValue(cmd:str, db: JsonDB | BsonDB | None) -> None:

    if not db: db = createDB("CREATE DB")

    if len(cmd.split()) == 2:
        table = cmd.split()[-1]
        print(json.dumps(db.get(table), indent=4))



    