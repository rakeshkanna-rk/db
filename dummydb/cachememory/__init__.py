"""## Cache Memory from Database Management

### Classes and Functions

- `DatabaseManagement`
- `BinaryDatabaseManagement`

### Usage for DatabaseManagement

```python
from DB import DatabaseManagement
from query import Query 

db = DatabaseManagement(".db")

# Precache
db.cache_memory("test.json")

# Example Query
query = Query.AND(
    Query().key("category") == "Furniture", 
    Query().key("price") >= 150
)

results = db.search_cache("test.json", query)
print("Furniture priced >= 150:", json.dumps(results, indent=4))
```

"""
from .json_cache import DatabaseManagement
from .binary_cache import BinaryDatabaseManagement
from .csv_cache import CSVDatabaseManagement

__all__ = ['DatabaseManagement', 'BinaryDatabaseManagement']