

import sqlite3
import csv
# import sys

class GenericCsv2SQL():

    def __init__(self,symbol):
        self.symbol = symbol

        self.conn = sqlite3.connect('zstxSQL.db')
        self.c=self.conn.cursor()
        self.keyFiller = 1

    def createTables(self):

        for i in self.symbol:

            print(i)
            self.c.execute("DROP TABLE IF EXISTS StxCompanyData")

            ###Following uses multiple PRIMARY KEYS (but not ID) so uses "keynumber" count i/o ID
            # self.c.execute("CREATE TABLE StxCompanyData (ID INTEGER, keynumber Integer,Symbol CHAR,Name, "
            #                "Industry,Dividend, "
            #                "PRIMARY KEY(Symbol))")

            ### Following uses ID as only PRIMARY KEY in order to get ID to autoincrement
            self.c.execute("CREATE TABLE StxCompanyData (ID INTEGER PRIMARY KEY, keynumber Integer,Symbol CHAR,Name, "
                           "Industry,Dividend)") #, "
                            #"PRIMARY KEY(ID,Symbol,date,open,high,low,close,vol,adjclose))")

            # self.index1 = self.c.execute("CREATE INDEX INDEXKEY ON StxData2(date)")
            # self.index2 = self.c.execute("CREATE UNIQUE INDEX INDEXDATE ON StxData2(keynumber)")

    def populateTables(self):
        for i in self.symbol:
            rowNumber=0
            with open('{0}.csv'.format('CompanyInfo'), newline='') as csvfile:
              reader = csv.reader(csvfile, delimiter=',', quotechar='|')
              for row in reader:
                if rowNumber > 0:

                  self.c.execute("INSERT OR IGNORE INTO StxCompanyData (keynumber, symbol, name,"
                                 "industry,dividend) VALUES (?,?,?,?,?)",
                                 (self.keyFiller,row[0],row[1],row[2],row[3]))


                  # self.c.execute("REPLACE INTO StxData2 (keynumber, symbol, date,open,high ,low ,close ,vol ,adjclose ) VALUES (?,?,?,?,?,?,?,?,?)", (self.keyFiller,i,row[0],row[1],row[2],row[3],row[4],row[5],row[6]))
                  self.keyFiller += 1
                else:
                    rowNumber += 1
              self.conn.commit()

        roww = self.c.lastrowid
        print(roww)

    def printMessage(self,whichOne):
        if whichOne == 'b':
            print("SQL Table Created")
        else:
            print("SQL Table Updated")
        # self.c.execute(select count(*) from <stxTable1> where ..


def start(x,createOrUpdate):
    a = GenericCsv2SQL(x)
    if createOrUpdate== 'c':
        b = a.createTables()
        c= a.populateTables()
    elif createOrUpdate == 'e':
        c = a.populateTables()
    else:
        print("Invalid Response. Try Again")
        start(x)
    d = a.printMessage(createOrUpdate)


start('x','c')



# start(['AAPL', 'FB', 'MSFT', 'IBM', 'ORCL']) #['IBM','AAPL','FB','MSFT'])

# if self.c.lastrowid > 0:
#             self.keyFiller = self.c.lastrowid + 1
#
#         else:
#             self.keyFiller = 1
#         print(self.symbol)