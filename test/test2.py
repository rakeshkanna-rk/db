import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dummydb.export import ExportManager

manager = ExportManager(dbname="db")

# Export full database
manager.export_full_db("full_backup.zip")

# Export specific tables
manager.export_tables("tables_backup.tar", "tar", "shop.json", "shop2.json")

# Export cache
# manager.export_cache("cache_backup.zip")
