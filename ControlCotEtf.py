#controlCotUpdate.py

'''
    1. prompts user to create new SQL table or use existing
       for each specified COT commodity and
       then calls Type1CotFormat.py or Type2CotFormat.py (depending on requirements)
       to to populate SQL table
    2. Which of the 2 files is called depends on the COT format for each specific commodity
    3. Allows for creating new table or updating existing table
'''

import json
import urllib.request
import sqlite3

class BuildCot():

    def __init__(self):
        self.conn = sqlite3.connect('allCotEtf.db')
        self.c = self.conn.cursor()

    def createSQL(self):
        self.c.execute("DROP TABLE IF EXISTS DataCOT")

        self.c.execute("CREATE TABLE DataCOT (ID INTEGER PRIMARY KEY,ID_NameKey,"
                       "Dataset TEXT,Database TEXT,Name TEXT,Date,OpenInt INTEGER,"
                       "RptableLong INTEGER,RptableShort INTEGER, "
                  "NonRptableLong INTEGER,NonRptableShort INTEGER,UNIQUE (Name,Date))")
        # NetRptable REAL,NetNonRptable REAL

            # db.execute('insert into test(t1, i1) values(?,?)', ('one', 1)) ## sample for format syntax

    def populateTables(self,createOrUpdate):
        self.createOrUpdate = createOrUpdate
        self.etfList = []
        import cotType1Format,cotType2Format
        stocks = cotType1Format.main("https://www.quandl.com/api/v3/datasets/CFTC/TIFF_CME_SC_ALL.json", 1,'SPY')
        self.etfList.append(stocks)
        bonds = cotType1Format.main("https://www.quandl.com/api/v3/datasets/CFTC/US_F_ALL.json",2,'THL')
        self.etfList.append(bonds)
        gold = cotType2Format.main("https://www.quandl.com/api/v3/datasets/CFTC/GC_F_ALL.json",3,'GLD')
        self.etfList.append(gold)
        oil = cotType2Format.main("https://www.quandl.com/api/v3/datasets/CFTC/CL_F_ALL.json",4,'USO')
        self.etfList.append(oil)
        print('etfList: ', self.etfList)

    def printMessage(self):
        counter = 1
        print()
        print()
        print("COT Table has been {0} for: ".format(self.createOrUpdate))
        for i in self.etfList:
            print("{0}: {1}".format(counter,i))
            counter +=1


def main():
    a = BuildCot()
    print()
    newOrExist = input("Create a new table('new') or update existing table('u')?: ")
    print()
    if newOrExist == 'new':
        print("CAUTION: Creating a new table will delete all current data")
        print()
        doubleCheck = input("Type 'y' to verify you want to create a new table: ")
        if doubleCheck == 'y':
            a.createSQL()
            b = a.populateTables('created')
            print('b: ',b)
        else:
            main()
    else:
        print("Updating existing table")
        a.populateTables('updated')

    a.printMessage()

if __name__ == '__main__': main()
