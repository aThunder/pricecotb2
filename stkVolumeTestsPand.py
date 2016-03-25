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


    def retrieveStkData(self,symbol,IDKEY,startDate,numberDays):
        self.symbol = symbol
        # self.symbol = ('aapl')
        self.startDate = startDate
        self.numberDays = numberDays + 1
        print()
        print('ID KEY: ',IDKEY,self.symbol.upper())

        status1 = True

        try:
            # self.stkDataDetail = pd.read_sql_query("SELECT * "
            #                                        "FROM SymbolsDataDaily "
            #                                        "WHERE ID_NAMEKEY = {0} "
            #                                        "AND DATE >= '2016-01-05' "
            #                                        "AND SYMBOL IN ('aapl')"
            #                                        "AND ID > 5 "
            #                                        " ".format(IDKEY,self.symbol),self.diskEngine)

            # self.stkDataDetail = pd.read_sql_query("SELECT SYMBOL, date('now','-1 day'),CLOSE,date " #max(DATE), CLOSE " #date('now','-1 day'), CLOSE " # date('now') - 1 day "#max(date)-1,SYMBOL,CLOSE "
            #                                        "FROM SymbolsDataDaily "
            #                                        "WHERE SYMBOL IN ('aapl') "
            #                                        " AND date('now',-5 days) "
            #                                        " ".format(IDKEY,self.symbol),self.diskEngine)
            #
            self.stkDataDetail = pd.read_sql_query("SELECT SYMBOL,DATE,CLOSE,VOL "
                                                   "FROM (SELECT * FROM SymbolsDataDaily "
                                                   "WHERE SYMBOL IN ('{0}')"
                                                   "ORDER BY DATE DESC LIMIT {1}) "
                                                   "ORDER BY DATE ASC "
                                                   " ".format(self.symbol,self.numberDays),self.diskEngine)



             # print("StkDataDetail: ", self.stkDataDetail[['ID','date','Symbol','open','high','low','close','vol']])
            print("StkDataDetail: ", self.stkDataDetail['date'][1])
            print()
            self.df = self.stkDataDetail
            # if self.df['Index'] == []:
            #     print('MMMMMMMMMMMOOOOOO')

            status1 = True
            return status1

        except:
            print("******{0} Not in Database******".format(self.symbol))
            print()
            print("==================================")
            status1 = False
            return status1



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
        movavg = pd.rolling_mean(self.df['vol'],3)
        periodchg = self.df['close'].diff(2)
        print("VolMovAvg: ",movavg)
        print("CloseChange: ",periodchg)

    def priceVolStats(self):
        self.df['changeClose'] = self.df['close'].diff()

        self.volMaskUp = self.df['close'].diff() >= 0
        self.volMaskDn = self.df['close'].diff() < 0
        self.upMean = self.df[self.volMaskUp].describe()
        print("upMean: ",self.upMean)
        # print("volMask: ", volMaskUp)

        # gains = self.df[volMaskUp]
        # print("Gains: ", gains, gains.count())
        print("UpDays: ",   self.df[['date','Symbol','close','changeClose','vol']][self.volMaskUp])
        print()
        print(self.df[self.volMaskUp].count())
        print()

        print("DownDays: ", self.df[['date','Symbol','close','changeClose','vol']][self.volMaskDn])
        print()
        print(self.df[self.volMaskDn].count())

    def onBalanceVolume(self):
        self.runningVol = 0
        obvFirstLast = []
        # # print("RunningVolume: ", self.df[['date','Symbol','close','changeClose','vol','runVol']])
        counter = 0
        for i in self.df['close'].diff():
            # print("i: ",i)
            # print("counter: ", counter)
            # print(self.df['date'][counter])
            #
            # print("VRunItem: ",self.df['vol'][counter])

            if i > 0 and counter > 0:
                # print("YES")
                self.runningVol += self.df['vol'][counter]
                print("OBVPlus: ", self.df['date'][counter],self.runningVol)
            elif i < 0 and counter > 0:
                # print("NO")
                self.runningVol -= self.df['vol'][counter]
                print("OBVMinus: ", self.df['date'][counter],self.runningVol)

            obvFirstLast.append(self.runningVol)
            # print()

            counter += 1
        firstOBV = obvFirstLast[1]
        lastOBV = obvFirstLast[counter-1]
        print()
        print("OBV:first,last: ",firstOBV,lastOBV)
        print("OBV Change: ",lastOBV-firstOBV)
        print()

        # print(self.df['date'][5:8])

    def avgVolumeUpDown(self):
        self.upVol = []
        self.dnVol = []
        totalUp = 0
        totalDn = 0
        counter = 0

        for i in self.df['close'].diff():
            # print("i: ",i)
            # print("counter: ", counter)
            # print(self.df['date'][counter])
            #
            # print("VRunItem: ",self.df['vol'][counter])

            if i > 0 and counter > 0:
                # print("YES")
                self.upVol.append(self.df['vol'][counter])
                # print("upVol: ", self.upVol)
            elif i < 0 and counter > 0:
                # print("NO")
                self.dnVol.append(self.df['vol'][counter])
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

    def priceMove(self):
        # print(self.df['close'])
        print()
        print("{0} days observations: ".format(self.numberDays-1))
        mostRecentPrice = self.df['close'][self.numberDays-1]
        firstPrice = self.df['close'][1]
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
    numberOfDays = 15
    # a.numberDaysToDate(numberOfDays)
    # criteria5 = ['%S&P%','%Gold%','%Bond%','%Oil%']
    criteria5 = ['aapl','mmm','gld']
    # startDate = input("Enter start date (YYYY-MM-DD): ") ## commented out for testing only
    startDate = '2015-10-01' ## for testing expediting only

    for i in criteria5:
        check1 = a.retrieveStkData(i,99,startDate,numberOfDays)
        # print("check1: ", check1)
        # a.volumeChg()
        # a.mask1()
        # a.orderData()
        # # a.grouping()
        # # a.priceVolStats()
        if check1:
            a.onBalanceVolume()
            a.avgVolumeUpDown()
            a.priceMove()
            # a.checkEtfData(1)
            # a.innerJoin1(i,startDate)
            # a.mostRecent()
            # c= a.plot1()
        else:
            # print('NOOOOOO')
            print()
    # a.summary1()

if __name__ == '__main__': main()



################################
################################


