import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dummydb import JsonDB, Query

db = JsonDB()

table = "shop.json"

word = "price >= 500"
key = word.split(">=")[0].strip()  # Strip whitespace
value = int(word.split(">=")[1].strip())  # Convert to integer
print(f"Extracted key: '{key}', Extracted value: {value}")

test_query = Query().key(key) >= value
test_result = db.search(table, test_query)
# print("Test query result:", test_result)


query = Query.AND(
    Query().key("category") == "Electronics",
    Query().key(key) >= value  # Dynamically constructed
)
combined_result = db.search(table, query)
# print("Combined query result:", combined_result)


key = word.split(">=")[0].strip()
value = int(word.split(">=")[1].strip())

test_query = Query().key(key) >= value

test_result = db.search(table, test_query)
# print("Test query result:", test_result)

table = "class.json"
query = Query.OR(
    Query().key("name") == "Ramesh", 
    Query().key("name") == "Rahul"
)
or_case = db.search(table, query)
import json
# print("Class Members:\n", json.dumps(or_case, indent=4))

query = Query.OR(
    Query().key("name") == "Ramesh", 
    Query().key("name") == "Rahul"
)
or_case = db.search(table, query)
print("Class Members:", json.dumps(or_case, indent=4))
