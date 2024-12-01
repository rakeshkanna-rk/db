import time
import os
from textPlay.colors import *
import json

from ..database import JsonDB, BsonDB
from ..logmsg import log, Status
from ..query import Query

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

def createDB(cmd:str) -> JsonDB | None:
    cmd:str = cmd.split()

    cmdok = False

    db = cmd[1].upper()

    if (db == "DB" or db== "JDB") and len(cmd) == 2:
        db = JsonDB()
        cmdok = True
        
    elif db == "DB" or db== "JDB" and len(cmd) == 3:
        if nameChecker(cmd[-1], "."):
            db = JsonDB(cmd[2].lower())
            cmdok = True
        else:
            log("Prefer using dot notation for database", Status.WARN)


    else:
        log("Usage: CREATE DB <DB-NAME>", Status.WARN)
        db = None

    if cmdok:
        log("Created Database successfully", Status.DONE)
    else:
        pass

    return db


def insertValue(cmd:str , db: JsonDB | BsonDB | None) -> None:

    if not db: db = createDB("CREATE DB")

    if len(cmd.split()) == 3 :
        print("Entering Insert mode...")
        print(f"Data format:",MAGENTA,"{'key': {'value' : 001}}",RESET)
        time.sleep(1)

        loop = True
        while loop:
            insert_data = input(f"{db.getdb()} {GREEN}>>> {RESET}").strip()
            if not insert_data:
                print("Exiting Insert mode...")
                loop = False

            else:
                try:
                    data = json.loads(insert_data)
                except Exception as e:
                    log(f"{e}", Status.ERROR)
                    continue

                key = next(iter(data))
                value = data[key]


                db.insert(key, value, cmd.split()[-1])

def selectValue(cmd:str, db: JsonDB | None) -> None:

    if not db: 
        log("Initialize Database before selecting items", Status.WARN)
        log("Usage: USE DB <DB-NAME>", Status.INFO)
    

    if len(cmd.split()) == 4 and cmd.split()[2].upper()=="FROM" and db:
        table = cmd.split()[-1]
        key = cmd.split()[1]
        if key.upper() == "ALL":
           print(json.dumps(db.getall(table), indent=4))
        elif "." in key:
            print(json.dumps(db.getnested(key, table), indent=4))
        else:
            print(json.dumps(db.get(key, table), indent=4))

    elif not len(cmd.split()) == 4 or not cmd.split()[2].upper()=="FROM" :
        log("Syntax Error - Usage: SELECT <KEY> FROM <TABLE>", Status.ERROR)


def deleteValue(cmd:str, db: JsonDB | None) -> None:

    if not db: 
        log("Initialize Database before selecting items", Status.WARN)
        log("Usage: USE DB <DB-NAME>", Status.INFO)
    

    if len(cmd.split()) == 4 and cmd.split()[2].upper()=="FROM" and db:
        table = cmd.split()[-1]
        key = cmd.split()[1]
        db.delete(key, table)

    elif not len(cmd.split()) == 4 or not cmd.split()[2].upper()=="FROM" :
        log("Syntax Error - Usage: DEL <KEY> FROM <TABLE>", Status.ERROR)






























from typing import Any

def convert_value(value: str) -> Any:
    """
    Converts a string value to an int or float if it's numeric, else returns the original string.
    """
    try:
        # Try converting the value to a float (for numbers with decimals) or int
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value  # If it's not numeric, return the string


def searchQuery2(cmd: str, db: JsonDB | None):
    if not db:
        log("Initialize Database before selecting items", Status.WARN)
        log("Usage: USE DB <DB-NAME>", Status.INFO)
        return
    
    cmd = cmd.replace("search", "SEARCH")
    cmd = cmd.replace("and", "AND")
    cmd = cmd.replace("or", "OR")
    cmd = cmd.replace("from", "FROM")

    split_cmd = cmd.split()
    query = split_cmd[1:-2]
    table = split_cmd[-1]

    qc = Query()
    all_query = []

    is_and = None
    is_or = None

    for word in query:
        if word.upper() == 'AND':
            is_and = True
        elif word.upper() == 'OR':
            is_or = True
        else:
            normal = True

    if is_and and is_or:
        log("Combined Queries under development", Status.WARN)
    
    elif is_and and not is_or:
        queries = " ".join(query).split("AND")

        for word in queries:

            word = word.strip()

            if "==" in word:
                key, value = word.split("==")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) == value
                all_query.append(set_query)

            elif "!=" in word:
                key, value = word.split("!=")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) != value
                all_query.append(set_query)

            elif "<=" in word:
                key, value = word.split("<=")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) <= value
                all_query.append(set_query)


            elif ">=" in word:
                key, value = word.split(">=")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) >= value
                all_query.append(set_query)

            elif "<" in word:
                key, value = word.split("<")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) < value
                all_query.append(set_query)

            elif ">" in word:
                key, value = word.split(">")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) > value
                all_query.append(set_query)            

        combined_query = Query.AND(all_query[0], *all_query[1:])

    elif not is_and and is_or:
        queries = " ".join(query).split("OR")

        for word in queries:
            word = word.strip()

            if "==" in word:
                key, value = word.split("==")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) == value
                all_query.append(set_query)

            elif "!=" in word:
                key, value = word.split("!=")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) != value
                all_query.append(set_query)

            elif "<=" in word:
                key, value = word.split("<=")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) <= value
                all_query.append(set_query)


            elif ">=" in word:
                key, value = word.split(">=")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) >= value
                all_query.append(set_query)

            elif "<" in word:
                key, value = word.split("<")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) < value
                all_query.append(set_query)

            elif ">" in word:
                key, value = word.split(">")
                key = key.strip().strip('"')   
                value = value.strip().strip('"')
                value = convert_value(value)
                set_query = Query().key(key) > value
                all_query.append(set_query) 

        combined_query = Query.OR(all_query[0], *all_query[1:])

    results = db.search(table, combined_query)
    print("Search Results: \n", json.dumps(results, indent=4))
    all_query = []






