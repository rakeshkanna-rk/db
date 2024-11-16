from ..database import JsonDB, BsonDB
from ..query import Query
from .utils import createDB, useDB, insertValue


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
                useDB(cmd, db)

            elif check(cmd.upper(), "INSERT"):
                insertValue(cmd, db)


            else:
                print("ERROR")

if __name__ == '__main__':
    cmdline()