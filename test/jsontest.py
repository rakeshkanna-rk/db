import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dummydb import JsonDB, Query

db = JsonDB()
table = "shop.json"
db.usetable(table)

db.insert(key="spare1", value={"id": "p1", "name": "Laptop", "category": "Electronics", "price": 1200})
db.insert(key="spare2", value={"id": "p2", "name": "Smartphone", "category": "Electronics", "price": 800})
db.insert(key="spare3", value={"id": "p3", "name": "Chair", "category": "Furniture", "price": 150})
db.insert(key="spare4", value={"id": "p4", "name": "Desk", "category": "Furniture", "price": 200})
db.insert(key="spare5", value={"id": "p5", "name": "Headphones", "category": "Electronics", "price": 150})

# Get DB name
print(
    db.getdb(), '\n'
    )

# Get a value from top level key
print(
    db.get(key="spare1"), '\n'
    )

# Get a value from nested keys
print(
    db.getnested(keyline="spare1.name"), '\n'
    )

# Get all key-value pairs
print(
    db.getall(), '\n'
    )

# Update a value with key
db.update(key="spare1", new_value={"id": "p1", "name": "Gaming Laptop", "category": "Electronics", "price": 2200})
print("Updated spare1\n",
    db.get(key="spare1"), '\n'
    )

# Delete a key-value pair
db.delete(key="spare1")
print("Deleted spare1\n",
    db.get(key="spare1"), '\n'
    )

print(f"\n{'-'*20}\nQuery Operators\n")

db.archive(table)
print("Archived\n")

db.unarchive(table)
print("Unarchived\n")

# Search with Arthmetic Operators
query = Query().key("category") == "Furniture"
equal_case = db.search(table, query)
print("Furniture Items:\n", equal_case, "\n")

# Search with Logical Operators
query = Query.AND(
    Query().key("category") == "Electronics", 
    Query().key("price") >= 500
)
and_case = db.search(table, query)
print("Electronics priced >= 500:\n", and_case, "\n")

query = Query.OR(
    Query().key("name") == "Desk", 
    Query().key("price") == 150
)
or_case = db.search(table, query)
print("Desk or priced 150:\n", or_case)

# Drop (delete) a table file
choose = input("\nDrop table? (y/n) ") 
if choose.lower() == "y":
    db.drop(table)