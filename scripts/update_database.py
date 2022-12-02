import sys

sys.path.append("./..")

import pymysql
pymysql.install_as_MySQLdb()

from backend.rent_scrapper import Arkadia_scrapper
from backend.database_manager import DataBaseManager

scarpper_west_arkadia = Arkadia_scrapper()
db = DataBaseManager(local=True)
df = scarpper_west_arkadia.main()
db.push_newest_data(df)
