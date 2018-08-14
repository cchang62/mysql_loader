# -*- coding: utf-8 -*-
from datetime import datetime
import configparser
import pandas as pd
import mysql.connector

import jaydebeapi
import teradata
import pyodbc
import numpy

Config = configparser.ConfigParser()
Config.read(u"./config.ini")


def getMySourceData():
    mySourceConf = Config['source_my']
    conn = mysql.connector.connect(
        user=mySourceConf['user'],
        password=mySourceConf['password'],
        host=mySourceConf['host'],
        database=mySourceConf['database'],
        auth_plugin='mysql_native_password'
    )
    sourceQry = """
    SELECT * from bank_simple;
    """
    # cursor = conn.cursor()
    # cursor.execute(sourceQry)
    # result = cursor.fetchall()
    # for x in result:
    #   print(x)

    sourceMyDF = pd.read_sql(sourceQry, conn)
    # print(pd.DataFrame(sourceMyDF))
    return sourceMyDF

def postMyData(df):
    # import pdb; pdb.set_trace()
    insertValue = []
    print(df.columns)
    # for index, row in df.iterrows():
    for idx, row in df.iterrows():
        strFormat = """({0[0]}, \"{0[1]}\", \"{0[2]}\", \"{0[3]}\", \"{0[4]}\", \"{0[5]}\", \"{0[6]}\", \'{0[7]}\')"""
        insertValue.append(strFormat.format(row))
    insertValueStr = ','.join(insertValue)
    print(insertValueStr)
    print(type(insertValueStr))


def getMSSourceData():
    msSourceConf = Config['source_ms']
    SYS_NOW = datetime.now()
    # SYS_VT_END_TS = datetime.strptime('9999-12-31 23:59:59.000000', '%Y-%m-%d %H:%M:%S.%f')
    SYS_VT_END_TS = '9999-12-31 23:59:59.000000'

    conn = pyodbc.connect(Driver=msSourceConf['driver'],
                          Server=msSourceConf['Server'],
                          Database=msSourceConf['Database'],
                          UID=msSourceConf['UID'],
                          PWD=msSourceConf['PWD'])

    cursor = conn.cursor()

    sourceQry = """
    SELECT 
    """

    sourceDF = pd.read_sql(sourceQry, conn)
    # print(pd.DataFrame(sourceDF))
    return sourceDF


def getCsvData(path_file):
    df = pd.read_csv(path_file, header=0, nrows=5)
    return df


if __name__ == "__main__":
    csvDataDf = getCsvData('/Users/CMC/Desktop/Dev/pydev/bank_simple1.csv')
    print(pd.DataFrame(csvDataDf))
    # mySourceDf = getMySourceData()
    # postMyData(mySourceDf)