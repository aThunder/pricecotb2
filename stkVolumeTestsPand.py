# cotsqlqPandas.py

'''
    1. Queries SQLite table for symbol and date range specified by user
    2. Performs several volume calculations:
            a: On-Balance Volume
            b: Avg Up v. Down volume

'''

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

class stkVolume():

    def __init__(self):
        self.conn = sqlite3.connect('allStks.db')
        self.cursor = self.conn.cursor()
        self.cursor.row_factory = sqlite3.Row
        self.diskEngine = create_engine('sqlite:///allStks.db')
        self.recentList =[]

    # def numberDaysToDate(self,numberOfDays):
    #     self.numberOfDays = numberOfDays
    #     print("DaysCounter: ",self.numberOfDays)
        # dateadd(-5)
        # current date getdate()

    def setSettings(self,symbol,IDKEY,startDate,numberDays):
        self.symbol = symbol
        self.startDate = startDate
        self.numberDays = numberDays + 1 #for subset
        self.numberDaysRetrieve = numberDays * 2 #for fullset
        print()
        print('ID KEY: ',IDKEY,self.symbol.upper())

    def retrieveFullSet(self):
        status1 = True
        try:
            # self.dfFull = pd.read_sql_query("SELECT SYMBOL, date('now','-1 day'),CLOSE,date " #max(DATE), CLOSE " #date('now','-1 day'), CLOSE " # date('now') - 1 day "#max(date)-1,SYMBOL,CLOSE "
            #                                        "FROM SymbolsDataDaily "
            #                                        "WHERE SYMBOL IN ('aapl') "
            #                                        " AND date('now',-5 days) "
            #                                        " ".format(IDKEY,self.symbol),self.diskEngine)
            #
            self.dfFull = pd.read_sql_query("SELECT SYMBOL,DATE,CLOSE,VOL "
                                                   "FROM (SELECT * FROM SymbolsDataDaily "
                                                   "WHERE SYMBOL IN ('{0}')"
                                                   "ORDER BY DATE DESC LIMIT {1}) "
                                                   "ORDER BY DATE ASC "
                                                   " ".format(self.symbol,self.numberDaysRetrieve),self.diskEngine)

            # print("dfFull: ", self.dfFull[['ID','date','Symbol','open','high','low','close','vol']])
            print("FullSet: ", self.dfFull['date'][1])
            print()
            # self.df = self.dfFull[self.numberDays-1:]

            status1 = True
            return status1

        except:
            print("******{0} Not in Database******".format(self.symbol))
            print()
            print("==================================")
            status1 = False
            return status1

    def retrieveSubset1(self):
        # # self.df = self.dfFull[0:][self.numberDays-2:]
        # # print("Self df2: ", self.dfFull['vol'][4])
        # self.df3 = self.dfFull.ix[self.numberDays-2:]
        # print("Self df3: ", self.dfFull3[3:4])

        try:
            self.dfSubset = pd.read_sql_query("SELECT SYMBOL,DATE,CLOSE,VOL "
                                                   "FROM (SELECT * FROM SymbolsDataDaily "
                                                   "WHERE SYMBOL IN ('{0}')"
                                                   "ORDER BY DATE DESC LIMIT {1}) "
                                                   "ORDER BY DATE ASC "
                                                   " ".format(self.symbol,self.numberDays),self.diskEngine)
            print("Subset: ", self.dfSubset)

        except:
            print('False')

    def volumeChg(self):
        self.volChg = self.dfFull['vol']-self.dfFull['vol'][1]
        self.volChg1 = self.dfFull['vol'].diff()
        print('VolumeChg: ',self.volChg,self.volChg1)
        print("TESTS: ",self.dfFull.sort_index(ascending=False, by = ['vol']))

    def mask1(self):
        mask = self.dfFull.open>200
        results = self.dfFull[mask]
        print("Mask: ",results)

    def orderData(self):
        # print("SortClose: ",self.df.sort('close'))
        periodchg = self.dfFull['close'].diff(2)
        print("CloseChange: ",periodchg)

    def priceVolStats(self):
        self.dfSubset['changeClose'] = self.dfSubset['close'].diff()

        self.volMaskUp = self.dfSubset['close'].diff() >= 0
        self.volMaskDn = self.dfSubset['close'].diff() < 0
        self.upMean = self.dfSubset[self.volMaskUp].describe()
        print("upMean: ",self.upMean)
        # print("volMask: ", volMaskUp)

        # gains = self.dfSubset[volMaskUp]
        # print("Gains: ", gains, gains.count())
        print("UpDays: ",   self.dfSubset[['date','Symbol','close','changeClose','vol']][self.volMaskUp])
        print()
        print(self.dfSubset[self.volMaskUp].count())
        print()
        print("DownDays: ", self.dfSubset[['date','Symbol','close','changeClose','vol']][self.volMaskDn])
        print()
        print(self.dfSubset[self.volMaskDn].count())

    def onBalanceVolume(self):
        self.runningVol = 0
        obvFirstLast = []

        counter = 0
        for i in self.dfSubset['close'].diff():
            # print("i: ",i)
            # print("counter: ", counter)
            # print(self.dfSubset['date'][counter])

            if i > 0 and counter > 0:
                # print("YES")
                self.runningVol += self.dfSubset['vol'][counter]
                print("OBVPlus: ", self.dfSubset['date'][counter],self.runningVol)
            elif i < 0 and counter > 0:
                # print("NO")
                self.runningVol -= self.dfSubset['vol'][counter]
                print("OBVMinus: ", self.dfSubset['date'][counter],self.runningVol)

            obvFirstLast.append(self.runningVol)
            # print()

            counter += 1
        firstOBV = obvFirstLast[1]
        lastOBV = obvFirstLast[counter-1]
        print()
        print("OBV:first,last: ",firstOBV,lastOBV)
        print("OBV Change From {0} days prior: {1}".format(self.numberDays-1,lastOBV-firstOBV))
        print()

    def avgVolumeUpDown(self):
        self.upVol = []
        self.dnVol = []
        totalUp = 0
        totalDn = 0
        counter = 0

        for i in self.dfSubset['close'].diff():
            # print("i: ",i)
            # print("counter: ", counter)
            # print(self.dfSubset['date'][counter])
            #
            # print("VRunItem: ",self.dfSubset['vol'][counter])

            if i > 0 and counter > 0:
                # print("YES")
                self.upVol.append(self.dfSubset['vol'][counter])
                # print("upVol: ", self.upVol)
            elif i < 0 and counter > 0:
                # print("NO")
                self.dnVol.append(self.dfSubset['vol'][counter])
                # print("dnVol: ", self.dnVol)
            # print()

            counter += 1

        for i in self.upVol:
            totalUp += i
        upAvg = totalUp/len(self.upVol)
        print('upVolumeMean: ', upAvg)
        print("UpDaysCount: ",len(self.upVol))

        upVolNP = np.mean(self.upVol)
        print("upVolumeMeanNP: ", upVolNP)
        print()

        for i in self.dnVol:
            totalDn += i
        dnAvg = totalDn/len(self.dnVol)
        print('downVolumeMean: ', dnAvg)
        print("DownDaysCount: ",len(self.dnVol))

        dnVolNP = np.mean(self.dnVol)
        print("downVolumeMeanNP: ", dnVolNP)

        print()
        print("Up:Down Volume Avg: ", upVolNP/dnVolNP)
        print("Up:Down Volume Days: ",len(self.upVol)/len(self.dnVol))

    def movAvg(self):
        self.dfFull['rolling']= pd.rolling_mean(self.dfFull['vol'],self.numberDays-1)
        print("{0}-day moving average for {1} is {2}".format(self.numberDays-1,self.symbol,
                                                                 self.dfFull[['date','rolling']][self.numberDays-1:]))

        # self.movAvg1 = pd.rolling_mean(self.dfFull['vol'],self.numberDays-1)
        # print("{0}-day moving average for {1} is {2} {3}".format(self.numberDays-1,self.symbol,
        #                                                          self.dfFull['date'][self.numberDays-1:],
        #                                                          self.movAvg1[self.numberDays-1:]))

    def priceMove(self):
        print()
        print("{0} days observations: ".format(self.numberDays-1))
        mostRecentPrice = self.dfSubset['close'][self.numberDays-1]
        firstPrice = self.dfSubset['close'][1]
        # print()
        print("First,Last: ",firstPrice, mostRecentPrice)
        print("PriceChange: ",mostRecentPrice-firstPrice)
        print("% Change: ", ((mostRecentPrice-firstPrice)/firstPrice)*100)
        print("==================================")

    def grouping(self):
        group1 = self.df.groupby()
        print('Group1: ',group1[['date','Symbol','close']])

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
    a = stkVolume()
    numberOfDays = 4
    # criteria5 = ['%S&P%','%Gold%','%Bond%','%Oil%']
    criteria5 = ['aapl'] #,'mmm','gld']
    # startDate = input("Enter start date (YYYY-MM-DD): ") ## commented out for testing only
    startDate = '2015-10-01' ## for testing expediting only

    for i in criteria5:
        a.setSettings(i,99,startDate,numberOfDays)
        check1 = a.retrieveFullSet()
        a.retrieveSubset1()
        # print("check1: ", check1)

        if check1:
            # a.volumeChg()
            # a.mask1()
            # a.orderData()
            # # a.grouping()
            # # a.priceVolStats()
            a.onBalanceVolume()
            a.avgVolumeUpDown()
            a.movAvg()
            a.priceMove()
            # c= a.plot1()
        else:
            print('NOOOOOO')
            print()
    # a.summary1()

if __name__ == '__main__': main()



################################
################################


