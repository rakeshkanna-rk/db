import os
import bson
import logging
from ..query import Query

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class BStorage:
    def __init__(self, dbname: str = '.db'):
        self.dbname = dbname
        self.data = {}

        # Ensure the database directory exists
        os.makedirs(self.dbname, exist_ok=True)
    
    def save(self, table: str):
        """Save current data to the given table."""
        with open(table, 'wb') as f:
            f.write(bson.dumps(self.data))
        self.data = {}

    def load(self, table: str) -> dict:
        """Load data from the given table."""
        table = os.path.join(self.dbname, table)

        if os.path.exists(table):
            with open(table, 'rb') as f:
                return bson.loads(f.read())
        else:
            return {}

    def usetable(self, tablename):
        """Use a common table name for all."""
        self.table = tablename

    def insert(self, key, value, table: str = None):
        """Insert the value into the table with key-value pair."""
        if not table:
            table = self.table

        table = os.path.join(self.dbname, table)

        # Load existing data before adding a new key-value
        if os.path.exists(table):
            with open(table, 'rb') as f:
                self.data = bson.loads(f.read())
        else:
            # Create an empty BSON file if the table doesn't exist
            self.data = {}
        
        # Add the new key-value pair and save the data
        self.data[key] = value
        self.save(table)
        self.data = {}

    def get(self, key, table: str = None):
        """Get the value associated with a key."""
        if not table:
            table = self.table

        load = self.load(table)
        return load.get(key)
    
    def getnested(self, keyline: str, table: str = None):
        """Get a value from nested keys using a keyline like `key.subkey`."""
        if not table:
            table = self.table

        load: dict = self.load(table)
        keys = keyline.split(".")
        value: dict = load
        for key in keys:
            value = value.get(key)
            if value is None:
                return None
        
        return value

    def getall(self, table: str = None) -> dict:
        """Get all key-value pairs from the table."""
        if not table:
            table = self.table
            
        load = self.load(table)
        return load

    def delete(self, key, table: str = None):
        """Delete a key-value pair from the table."""
        if not table:
            table = self.table
        
        load = self.load(table)  
        table = os.path.join(self.dbname, table)

        if key in load:
            del load[key]
            self.save(table)
        else:
            logging.error("%s not found in %s", key, table)


    def drop(self, table: str = None):
        """Drop (delete) a table file."""
        if not table:
            table = self.table
            
        table = os.path.join(self.dbname, table)
        if os.path.exists(table):
            os.remove(table)
        else:
            logging.error('File %s Not Found', table)

    def update(self, key, new_value, table: str = None):
        """Update the value associated with a key in the table."""
        if not table:
            table = self.table
        savetable = os.path.join(self.dbname, table)

        # Load existing data and check if the key exists
        self.data = self.load(table)
        if key in self.data:
            self.data[key] = new_value  # Update the value
            self.save(savetable)
        else:
            logging.error("%s not found in %s", key, table)

    def search(self, table: str, query: Query):
        data = self.load(table)
        results = []
        for item in data.values():
            if query.matches(item):
                results.append(item)
        return results
    
    def flush(self, table: str = None):
        """Clear all data from the table."""
        if not table:
            table = self.table

        table = os.path.join(self.dbname, table)
        with open(table, 'wb') as f:
            f.write(bson.dumps({}))

    def archive(self, table: str = None):
        """Move the table file to an archive folder."""
        if not table:
            table = self.table
        
        table_path = os.path.join(self.dbname, table)
        if not os.path.exists(table_path):
            logging.error("File %s Not Found", table)
        
        os.makedirs(os.path.join(self.dbname, ".archive"), exist_ok=True)
        transfer_path = os.path.join(self.dbname, ".archive", table)
        if os.path.exists(transfer_path):
            logging.error("%s already exist in archive", table)
            exit()

        os.rename(table_path, transfer_path)
        
    def unarchive(self, table):
        """Move the archived table file back to the main directory."""
        if not table:
            table = self.table
        
        table_path = os.path.join(self.dbname, ".archive", table)

        if not os.path.exists(table_path):
            logging.error("File %s not found in archive", table)
        
        transfer_path = os.path.join(self.dbname, table)
        if os.path.exists(transfer_path):
            logging.error("%s already exist in Database", table)
        
        os.rename(table_path, transfer_path)
