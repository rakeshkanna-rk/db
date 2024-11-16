import json
import os
from collections import defaultdict

class DatabaseManagement:
    """Cache Memory Management"""
    def __init__(self, dbname):
        self.dbname = dbname
        self.cache_dir = os.path.join(self.dbname, ".cache")
        os.makedirs(self.cache_dir, exist_ok=True)

    def group_by_common_keys(self, data: dict) -> dict:
        if not data:
            return {}

        common_keys = set.intersection(*(set(details.keys()) for details in data.values()))
        grouped_results = {}

        for key in common_keys:
            grouped_data = defaultdict(list)
            for user_id, details in data.items():
                value = details.get(key)
                if value is not None:
                    grouped_data[value].append({user_id: details})
            grouped_results[key] = dict(grouped_data)

        return grouped_results

    def cache_memory(self, table):
        path = os.path.join(self.dbname, table)
        cache_file = os.path.join(self.cache_dir, table)

        with open(path, "r") as fh:
            data = json.load(fh)

        grouped_data = self.group_by_common_keys(data)

        with open(cache_file, "w") as fh:
            json.dump(grouped_data, fh, indent=4)

    def search_cache(self, table, query):
        cache_file = os.path.join(self.cache_dir, table)

        if not os.path.exists(cache_file):
            raise FileNotFoundError(f"Cache for table {table} does not exist. Please run cache_memory first.")

        with open(cache_file, "r") as fh:
            cached_data = json.load(fh)

        results = []
        seen = set()  # To avoid duplicates

        for key, grouped_data in cached_data.items():
            for value, items in grouped_data.items():
                for item in items:
                    for user_id, details in item.items():
                        if query.matches(details):
                            # Use a unique identifier (user_id + details) to track duplicates
                            unique_identifier = (user_id, json.dumps(details, sort_keys=True))
                            if unique_identifier not in seen:
                                results.append({user_id: details})
                                seen.add(unique_identifier)

        return results


# if __name__ == "__main__":
#     from query import Query  # Assuming Query class is in query.py

#     db = DatabaseManagement(".db")

#     # Prepare cache
#     db.cache_memory("test.json")

#     # Example Query
#     query = Query.AND(
#         Query().key("category") == "Furniture", 
#         Query().key("price") >= 150
#     )
#     results = db.search_cache("test.json", query)
#     print("Furniture priced >= 150:", json.dumps(results, indent=4))
