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
        self.IDInit = 'xxxxx'

    def setSettings(self,symbol,IDKEY,numberDays,dfFullSet,dfSubSet,dfOverallMktSet):
        self.symbol = symbol
        self.numberDays = numberDays + 1 #for subset
        self.numberDaysRetrieve = numberDays * 2 #for fullset
        self.dfFullSet = dfFullSet
        self.dfSubSet = dfSubSet
        self.dfOverallMktSet = dfOverallMktSet
        print()
        print('ID KEY: ',IDKEY,self.symbol.upper())
        print()
        # print("Full: ", self.dfFullSet)
        # print("sub: ", self.dfSubSet)

    def volumeChg(self):
        self.volChg = self.dfFullSet['vol']-self.dfFullSet['vol'][1]
        self.volChg1 = self.dfFullSet['vol'].diff()
        print('VolumeChg: ',self.volChg,self.volChg1)
        print("TESTS: ",self.dfFullSet.sort_index(ascending=False, by = ['vol']))

    def mask1(self):
        mask = self.dfFullSet.open>200
        results = self.dfFullSet[mask]
        print("Mask: ",results)

    def orderData(self):
        # print("SortClose: ",self.df.sort('close'))
        periodchg = self.dfFullSet['close'].diff(2)
        print("CloseChange: ",periodchg)

    def priceVolStats(self):
        self.dfSubSet['changeClose'] = self.dfSubSet['close'].diff()

        self.volMaskUp = self.dfSubSet['close'].diff() >= 0
        self.volMaskDn = self.dfSubSet['close'].diff() < 0
        # self.upMean = self.dfSubSet[self.volMaskUp].describe()
        # print("upMean: ",self.upMean)
        # print("volMask: ", volMaskUp)

        # gains = self.dfSubSet[volMaskUp]
        # print("Gains: ", gains, gains.count())
        print("{0} Days of Tests For {1}".format(self.numberDays-1,self.symbol.upper()))
        print()
        print("UpDays: ")
        # print(self.dfSubSet[['date','Symbol','close','changeClose','vol']][self.volMaskUp])
        # print()
        print(self.dfSubSet[self.volMaskUp]['close'].count())
        print()
        print("DownDays: ")
        # print(self.dfSubSet[['date','Symbol','close','changeClose','vol']][self.volMaskDn])
        # print()
        print("Count: ", self.dfSubSet[self.volMaskDn]['close'].count())

    def onBalanceVolume(self):
        self.runningVol = 0
        obvFirstLast = []

        counter = 0
        for i in self.dfSubSet['close'].diff():
            # print("i: ",i)
            # print("counter: ", counter)
            # print(self.dfSubSet['date'][counter])

            if i > 0 and counter > 0:
                # print("YES")
                self.runningVol += self.dfSubSet['vol'][counter]
                # print("OBVPlus: ", self.dfSubSet['date'][counter],self.runningVol)
            elif i < 0 and counter > 0:
                # print("NO")
                self.runningVol -= self.dfSubSet['vol'][counter]
                # print("OBVMinus: ", self.dfSubSet['date'][counter],self.runningVol)

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

        for i in self.dfSubSet['close'].diff():
            # print("i: ",i)
            # print("counter: ", counter)
            # print(self.dfSubSet['date'][counter])
            #
            # print("VRunItem: ",self.dfSubSet['vol'][counter])

            if i > 0 and counter > 0:
                # print("YES")
                self.upVol.append(self.dfSubSet['vol'][counter])
                # print("upVol: ", self.upVol)
            elif i < 0 and counter > 0:
                # print("NO")
                self.dnVol.append(self.dfSubSet['vol'][counter])
                # print("dnVol: ", self.dnVol)
            # print()

            counter += 1

        for i in self.upVol:
            totalUp += i
        try:
            upAvg = totalUp/len(self.upVol)
            print('upVolumeMean: ', upAvg)
            print("UpDaysCount: ",len(self.upVol))

            upVolNP = np.mean(self.upVol)
            print("upVolumeMeanNP: ", upVolNP)
            print()
        except:
            print("There were no UP days in the {0}-day range".format(self.numberDays-1))
            print()
        for i in self.dnVol:
            totalDn += i
        try:
            dnAvg = totalDn/len(self.dnVol)
            print('downVolumeMean: ', dnAvg)
            print("DownDaysCount: ",len(self.dnVol))

            dnVolNP = np.mean(self.dnVol)
            print("downVolumeMeanNP: ", dnVolNP)
            print()
        except:
            print("There were no DOWN days in the {0}-day range".format(self.numberDays-1))
            print()
        try:
            print("Up:Down Volume Avg: ", upVolNP/dnVolNP)
            print("Up:Down Volume Days: ",len(self.upVol)/len(self.dnVol))
        except:
            print("Ratio of Up:Down Volume Days N/A")
            print()

    def priceMove(self):
        print()
        # print("XXXXX: ", self.dfSubSet)
        print("{0} days Price Observations: ".format(self.numberDays-1))
        mostRecentPrice = self.dfSubSet['close'][self.numberDays-1]
        firstPrice = self.dfSubSet['close'][1]
        # print()
        print("First,Last: ",firstPrice, mostRecentPrice)
        print("PriceChange: ",mostRecentPrice-firstPrice)
        print("% Change: ", ((mostRecentPrice-firstPrice)/firstPrice)*100)
        print("==================================")
        return
    def movAvg(self):
        self.dfFullSet['rolling'] = pd.rolling_mean(self.dfFullSet['vol'], self.numberDays - 1)
        print("{0}-day moving average for {1} is {2}".format(self.numberDays - 1, self.symbol,
                                                             self.dfFullSet[
                                                                 ['date', 'rolling']].tail()))  # [self.numberDays-1:]))

        # self.movAvg1 = pd.rolling_mean(self.dfFull['vol'],self.numberDays-1)
        # print("{0}-day moving average for {1} is {2} {3}".format(self.numberDays-1,self.symbol,
        #                                                          self.dfFull['date'][self.numberDays-1:],
        #

    def vsOverallVolume(self):
        # print("SPY: ",self.dfMarket)
        self.dfFullSet['MktVolu'] = self.dfOverallMktSet['vol']
        self.dfFullSet['MktRatioVol'] = self.dfFullSet['MktVolu'] / pd.rolling_mean(self.dfFullSet['MktVolu'],
                                                                                    self.numberDays - 1)
        # print('MktRatio: ', self.dfFullSet)

        self.dfFullSet['IndivRatioVol'] = self.dfFullSet['vol'] / pd.rolling_mean(self.dfFullSet['vol'],
                                                                                  self.numberDays - 1)
        # print('MktRatio: ', self.dfFullSet)

        self.dfFullSet['IndivtoMktVol'] = np.round(self.dfFullSet['IndivRatioVol'] / self.dfFullSet['MktRatioVol'],
                                                   decimals=3)
        print("Complete: ")
        print(self.dfFullSet[-10:])


        # def plot1(self):
    #     print("Now plotting")
    #     plt.plot(self.intoPandas1['NetReportable'])
    #     plt.ylabel("Net Position")
    #     plt.xlabel("Date")
    #     plt.title("COT: {0} Net Reportable Position".format(self.criteria))
    #     plt.show()
    #     print("Was there a plot?")
    #
    # #         # db.execute('insert into test(t1, i1) values(?,?)', ('one', 1)) ## sample for format syntax

def main(choice1,symbol,dfFullSet,dfSubSet,dfOverallMktSet,numberDays):
    a = stkVolume()
    numberOfDays = numberDays

    a.setSettings(symbol,99,numberOfDays,dfFullSet,dfSubSet,dfOverallMktSet)
    #         # a.volumeChg()
    #         # a.mask1()
    #         # a.orderData()
    #         # # a.grouping()
    if choice1 == 1:
        print("Choice1: ",choice1)
        print("1. Volume Up/Down")
        a.priceVolStats()
        a.onBalanceVolume()
        a.avgVolumeUpDown()
        a.priceMove()
    if choice1 == 2:
        print("2. Volume Moing Averages")
        print("Choice1: ",choice1)
        a.movAvg()
    if choice1 == 3:
        print("Choice1: ",choice1)
        print("Volume Stock:Market Ratios")
        a.vsOverallVolume()
    # if choice == 4:
    #     Print("So Long")
    #     break

    print("Request Completed. Select another choice")
    print()
    import ControlStkVolume
    ControlStkVolume.main()

if __name__ == '__main__': main(choice1,symbol,fullSet1,subSet1,overallMktSet,numberDays)