import sys

sys.path.append("./..")

import pymysql
pymysql.install_as_MySQLdb()

from backend.database_manager import DataBaseManager

db = DataBaseManager(local=True)
db.count_rows()