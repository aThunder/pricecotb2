# cotsqlqPandas.py

'''
    1. Queries ????2 SQLite tables (COT & ETFs) and uses Join for results
    2. Performs several calculations and summaries on results
    3. Weekly data only
    4.
'''


import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

class spCOT():

    def __init__(self):
        self.conn = sqlite3.connect('allStks.db')
        self.cursor = self.conn.cursor()
        self.cursor.row_factory = sqlite3.Row
        self.diskEngine = create_engine('sqlite:///allStks.db')
        self.recentList =[]

    def checkStkData(self,symbol,IDKEY):
        # self.symbol = symbol
        self.symbol = ('spy','aapl')
        print('ID KEY: ',IDKEY,self.symbol)
        self.stkDataDetail = pd.read_sql_query("SELECT * "
                                               "FROM SymbolsDataDaily "
                                               "WHERE ID_NAMEKEY = {0} "
                                               "AND DATE >= '2016-01-01' "
                                               "AND SYMBOL IN ('aapl') "
                                               " ".format(IDKEY,self.symbol),self.diskEngine)
        print("StkDataDetail: ", self.stkDataDetail[['date','Symbol','open','high','low','close','vol']])

        print()
        self.df = self.stkDataDetail

    def volumeChg(self):
        self.volChg = self.stkDataDetail['vol']-self.stkDataDetail['vol'][1]
        self.volChg1 = self.stkDataDetail['vol'].diff()
        print('VolumeChg: ',self.volChg,self.volChg1)
        print("TESTS: ",self.stkDataDetail.sort_index(ascending=False, by = ['vol']))

    def mask1(self):
        mask = self.stkDataDetail.open>200
        results = self.stkDataDetail[mask]
        print("Mask: ",results)

    def orderData(self):
        # print("Close: ",self.df['close'])
        # print("SortClose: ",self.df.sort('close'))
        movavg = pd.rolling_mean(self.df['vol'],50)
        periodchg = self.df['close'].diff(2)
        print("MovAvg: ",movavg)
        # print("Change: ",periodchg)

    def grouping(self):
        group1 = self.df.groupby()
        print('Group1: ',group1[['date','Symbol','close']])



    def innerJoin1(self,criteria1,startDate):
        self.criteria1 = criteria1
        # startDate = '2015-11-01'
        print('startDate: ', startDate)
        self.intoPandasJoin1 = pd.read_sql_query("SELECT DataCOT.NAME,"
                                                 " DataCOT.OPENINT, "
                                                 "DataCOT.DATE,"
                                                 " DataCOT.RptableLong,"
                                                 " DataCOT.RptableShort,"
                                                 " CotEtfDataWeekly.SYMBOL,"
                                                 " CotEtfDataWeekly.CLOSE,"
                                                 " CotEtfDataWeekly.VOL, "
                                                 " CotEtfDataWeekly.DATE "
                                                 " FROM DataCOT "
                                                 "INNER JOIN CotEtfDataWeekly "
                                                 "ON DataCOT.ID_NAMEKEY =  CotEtfDataWeekly.ID_NAMEKEY "
                                                 "AND DataCOT.DATE = CotEtfDataWeekly.DATE "
                                                 "WHERE DataCOT.DATE > '{0}' "
                                                 "AND NAME LIKE '{1}'"
                                                 "ORDER BY CotEtfDataWeekly.DATE asc".format(startDate,self.criteria1),
                                                 self.diskEngine)

        countLinesAll = self.intoPandasJoin1['Date'].count()
        self.intoPandasJoin1['NetReportable'] = self.intoPandasJoin1['RptableLong']-self.intoPandasJoin1['RptableShort']
        self.intoPandasJoin1['WkNetRptableChg'] = self.intoPandasJoin1['NetReportable'].diff()
        self.intoPandasJoin1['WkPriceChg'] = np.round(self.intoPandasJoin1['close'].diff(),decimals=2)

        print("JOINED: ",self.intoPandasJoin1)

    def mostRecent(self):

        countLines = self.intoPandasJoin1['WkNetRptableChg'].count()
        # print('#Lines: ',countLines)
        mostRecent = ("{0}: NetReportable: {1}  WeeklyChg: {2} WkPriceChg: {3}".
                format(self.intoPandasJoin1['Symbol'][countLines],self.intoPandasJoin1['NetReportable'][countLines],
                self.intoPandasJoin1['WkNetRptableChg'][countLines],self.intoPandasJoin1['WkPriceChg'][countLines]))

        # print("MostRecent {0}: {1}".
        #         format(self.criteria1,mostRecent))

        self.recentList.append(mostRecent)

    def summary1(self):
        print()
        counter=1
        for i in self.recentList:
            print(counter, i)
            counter +=1

    def plot1(self):
        print("Now plotting")
        plt.plot(self.intoPandas1['NetReportable'])
        plt.ylabel("Net Position")
        plt.xlabel("Date")
        plt.title("COT: {0} Net Reportable Position".format(self.criteria))
        plt.show()
        print("Was there a plot?")

    #         # db.execute('insert into test(t1, i1) values(?,?)', ('one', 1)) ## sample for format syntax

def main():
    a = spCOT()
    # criteria5 = ['%S&P%','%Gold%','%Bond%','%Oil%']
    criteria5 = ['spy','aapl']
    # startDate = input("Enter start date (YYYY-MM-DD): ") ## commented out for testing only
    startDate = '2015-10-01' ## for testing expediting only
    for i in range(1): #criteria5:
        a.checkStkData(i,99)
        # a.volumeChg()
        # a.mask1()
        a.orderData()
        # a.grouping()
        # a.checkEtfData(1)
        # a.innerJoin1(i,startDate)
        # a.mostRecent()
        # c= a.plot1()
    # a.summary1()

if __name__ == '__main__': main()



