import os
import bson
from collections import defaultdict

class BinaryDatabaseManagement:
    """Cache Memory Management for BSON"""
    
    def __init__(self, dbname):
        self.dbname = dbname
        self.cache_dir = os.path.join(self.dbname, ".cache")
        os.makedirs(self.cache_dir, exist_ok=True)

    def group_by_common_keys(self, data: dict) -> dict:
        """Groups data by common keys."""
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
        """Caches the memory data into BSON."""
        path = os.path.join(self.dbname, table)
        cache_file = os.path.join(self.cache_dir, table)

        with open(path, "rb") as fh:  # Read as binary
            data = bson.decode_all(fh.read())

        grouped_data = self.group_by_common_keys(data)

        with open(cache_file, "wb") as fh:  # Write as binary
            fh.write(bson.dumps(grouped_data))

    def search_cache(self, table, query):
        """Searches in the cached BSON data."""
        cache_file = os.path.join(self.cache_dir, table)

        if not os.path.exists(cache_file):
            raise FileNotFoundError(f"Cache for table {table} does not exist. Please run cache_memory first.")

        with open(cache_file, "rb") as fh:
            cached_data = bson.decode_all(fh.read())

        results = []
        seen = set()  # To avoid duplicates

        for key, grouped_data in cached_data.items():
            for value, items in grouped_data.items():
                for item in items:
                    for user_id, details in item.items():
                        if query.matches(details):
                            # Use a unique identifier (user_id + details) to track duplicates
                            unique_identifier = (user_id, bson.dumps(details))
                            if unique_identifier not in seen:
                                results.append({user_id: details})
                                seen.add(unique_identifier)

        return results
