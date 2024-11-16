import os
import csv
from collections import defaultdict

class CSVDatabaseManagement:
    """Cache Memory Management for CSV"""
    
    def __init__(self, dbname):
        self.dbname = dbname
        self.cache_dir = os.path.join(self.dbname, ".cache")
        os.makedirs(self.cache_dir, exist_ok=True)

    def group_by_common_keys(self, data: list) -> dict:
        """Groups data by common keys."""
        if not data:
            return {}

        # Assuming the first row contains the column names (keys)
        column_names = data[0].keys()
        grouped_results = {}

        for column in column_names:
            grouped_data = defaultdict(list)
            for row in data:
                value = row.get(column)
                if value is not None:
                    grouped_data[value].append(row)
            grouped_results[column] = dict(grouped_data)

        return grouped_results

    def cache_memory(self, table):
        """Caches the memory data into CSV."""
        path = os.path.join(self.dbname, table)
        cache_file = os.path.join(self.cache_dir, table)

        with open(path, "r") as fh:
            reader = csv.DictReader(fh)
            data = [row for row in reader]  # List of dictionaries

        grouped_data = self.group_by_common_keys(data)

        with open(cache_file, "w", newline='') as fh:
            # Get the fieldnames from the grouped data (use the first key from the grouped data)
            fieldnames = list(grouped_data.keys())
            writer = csv.DictWriter(fh, fieldnames=fieldnames)

            writer.writeheader()
            # Write grouped data as rows
            for row in grouped_data:
                writer.writerow(row)

    def search_cache(self, table, query):
        """Searches in the cached CSV data."""
        cache_file = os.path.join(self.cache_dir, table)

        if not os.path.exists(cache_file):
            raise FileNotFoundError(f"Cache for table {table} does not exist. Please run cache_memory first.")

        with open(cache_file, "r") as fh:
            reader = csv.DictReader(fh)
            cached_data = [row for row in reader]

        results = []
        seen = set()  # To avoid duplicates

        for row in cached_data:
            if query.matches(row):
                # Create a unique identifier using a tuple of row data
                unique_identifier = tuple(row.items())
                if unique_identifier not in seen:
                    results.append(row)
                    seen.add(unique_identifier)

        return results
