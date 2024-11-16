import time
import os
import re

from ..database import JsonDB, BsonDB

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

def ok():
    print(f"[{time.strftime('%H:%M:%S')}] Command executed successfully")

def splitCmdValue(cmd:str) -> list:

    cmd.replace("insert")

    pattern = r'\b(INSERT|INTO)\b'
    parts = re.split(pattern, cmd, flags=re.IGNORECASE)
    parts = [part.strip() for part in parts if part.strip()]
    return parts


def createDB(cmd:str) -> JsonDB | BsonDB | None:
    cmd:str = cmd.split()

    db = cmd[1].upper()

    if (db == "DB" or db== "JDB") and len(cmd) == 2:
        db = JsonDB()
        cmdok = True

    elif db == "BDB" and len(cmd) == 2:
        db = BsonDB()
        cmdok = True
        
    elif db == "DB" or db== "JDB" and len(cmd) == 3:
        db = JsonDB(cmd[2].lower())
        cmdok = True

    elif db == "BDB"  and len(cmd) == 3:
        db = BsonDB(cmd[2].lower())
        cmdok = True

    else:
        error(cmd, "H", "CREATE DB or CREATE BDB")
        db = None

    if cmdok:
        ok()
    else:
        pass

    return db

def useDB(cmd: str, db: JsonDB | BsonDB | None):
    cmd = cmd.split()

    if len(cmd) == 2:
        if not db: db = createDB("CREATE DB")

        try:
            db.usetable(cmd[1])
            
        except Exception as e:
            print(e)

        return
    else:
        error("")
    

def insertValue(cmd:str , db: JsonDB | BsonDB | None) -> None:

    getSplitValue:list = splitCmdValue(cmd)

    data:dict = eval(getSplitValue[1])
    
    key = list(data.keys())
    value = list(data.values())

    if len(getSplitValue) == 4:
        if not db: db = createDB("CREATE DB")

        db.insert(key[0], value[0], getSplitValue[-1])
        pass
    