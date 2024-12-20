import csv
import os
from typing import List, Dict, Union
from ..query import Query
from ..logmsg import log, Status


class CSVStorage:
    def __init__(self, db_path: str):
        """
        Initialize the CSV storage system.

        Args:
            db_path (str): Path to the directory where CSV files are stored.
        """
        self.db_path = db_path
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path, exist_ok=True)
            log(f"Created {self.db_path}, as Database not exist", Status.INFO)

    def _get_file_path(self, table: str) -> str:
        """
        Get the full file path for a table.

        Args:
            table (str): Table name (CSV file name).

        Returns:
            str: Full file path of the CSV file.
        """
        return os.path.join(self.db_path, table)

    def create_table(self, table: str, columns: List[str]):
        """
        Create a new table (CSV file) with the specified columns.

        Args:
            table (str): Table name (CSV file name).
            columns (List[str]): List of column names.
        """
        file_path = self._get_file_path(table)
        if os.path.exists(file_path):
            log(f"{table} already exist", Status.ERROR)
            exit()

        with open(file_path, mode="w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(columns)
            log(f"Created {table}", Status.DONE)

    def insert(self, table: str, row: Dict[str, Union[str, int, float]]):
        """
        Insert a row of data into a table.

        Args:
            table (str): Table name (CSV file name).
            row (Dict[str, Union[str, int, float]]): Row data as a dictionary.
        """
        file_path = self._get_file_path(table)
        if not os.path.exists(file_path):
            log(f"{table} does not exist", Status.ERROR)
            exit()

        with open(file_path, mode="a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=row.keys())
            if fh.tell() == 0:
                # Write header if the file is empty
                writer.writeheader()
            writer.writerow(row)
            log(f"Inserted Data sucessfully", Status.DONE)

    def fetch_all(self, table: str) -> List[Dict[str, Union[str, int, float]]]:
        """
        Fetch all rows from a table.

        Args:
            table (str): Table name (CSV file name).

        Returns:
            List[Dict[str, Union[str, int, float]]]: List of all rows as dictionaries.
        """
        file_path = self._get_file_path(table)
        if not os.path.exists(file_path):
            log(f"{table} does not  exist", Status.ERROR)
            exit()

        with open(file_path, mode="r") as fh:
            reader = csv.DictReader(fh)
            return list(reader)

    def delete_table(self, table: str):
        """
        Delete a table (CSV file).

        Args:
            table (str): Table name (CSV file name).
        """
        file_path = self._get_file_path(table)
        if os.path.exists(file_path):
            os.remove(file_path)
            log(f"{table} Droped", Status.DONE)
        else:
            log(f"{table} does not  exist", Status.ERROR)
            exit()

    def update(self, table: str, condition: callable, update_values: Dict[str, Union[str, int, float]]):
        """
        Update rows in a table based on a condition.

        Args:
            table (str): Table name (CSV file name).
            condition (callable): A function that takes a row and returns True if it should be updated.
            update_values (Dict[str, Union[str, int, float]]): Values to update in matching rows.
        """
        file_path = self._get_file_path(table)
        if not os.path.exists(file_path):
            log(f"{table} does not  exist", Status.ERROR)
            exit()

        with open(file_path, mode="r") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        updated_rows = []
        for row in rows:
            if condition(row):
                row.update(update_values)
            updated_rows.append(row)

        with open(file_path, mode="w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(updated_rows)
            log(f"Rows Updated", Status.DONE)

    def search(self, table: str, query: Query) -> List[Dict[str, Union[str, int, float]]]:
        """
        Search rows in a table based on a Query object.

        Args:
            table (str): Table name (CSV file name).
            query (Query): A Query object defining the search condition.

        Returns:
            List[Dict[str, Union[str, int, float]]]: List of matching rows as dictionaries.
        """
        file_path = self._get_file_path(table)
        if not os.path.exists(file_path):
            log(f"{table} does not exist", Status.ERROR)
            exit()

        with open(file_path, mode="r") as fh:
            reader = csv.DictReader(fh)
            return [row for row in reader if query.matches(row)]


