#controlCotUpdate.py

'''
    1. prompts user to create new SQL table or use existing
       for each specified COT commodity and
       then calls Type1CotFormat.py or Type2CotFormat.py (depending on requirements)
       to to populate SQL table
    2. Which of the 2 files is called depends on the COT format for each specific commodity
    3. Allows for creating new table or updating existing table
'''

# import json
# import urllib.request
import sqlite3

class CreateCotSQL():

    def __init__(self):
        self.conn = sqlite3.connect('allCotEtf.db')
        self.c = self.conn.cursor()

    def createSQL(self):
        self.c.execute("DROP TABLE IF EXISTS DataCOT")

        self.c.execute("CREATE TABLE DataCOT (ID INTEGER PRIMARY KEY,ID_NameKey,"
                       "Dataset TEXT,Database TEXT,Name TEXT,Date,OpenInt INTEGER,"
                       "RptableLong INTEGER,RptableShort INTEGER, "
                  "NonRptableLong INTEGER,NonRptableShort INTEGER,UNIQUE (Name,Date))")

def main():
    a = CreateCotSQL()
    print("Entered cotCotSQL")

if __name__ == '__main__': main()
