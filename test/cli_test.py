import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import dummydb as db
import dummydb.cli.utils as utils

db.cmdline()
