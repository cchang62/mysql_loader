from __future__ import print_function

from datetime import date, datetime
import mysql.connector

import pandas as pd
import numpy as np



df = pd.read_csv('/Users/CMC/Desktop/Dev/pydev/bank_simple1.csv', header=0, names=['age', 'job', 'marital', 'education', 'default', 'balance', 'housing'])

cnx = mysql.connector.connect(
            user='nativeSuperuser',
            password='nativeSuperuser@123',
            host='127.0.0.1',
            database='test',
            auth_plugin='mysql_native_password'
            )

cursor = cnx.cursor()


df['updatetime'] = datetime.now()
insertValue=[]
for index, row in df.iterrows():
        strFormat ='({0[0]}, \"{0[1]}\", \"{0[2]}\", \"{0[3]}\", \"{0[4]}\", \"{0[5]}\", \"{0[6]}\", \'{0[7]}\')'
        insertValue.append(strFormat.format(row))

insertValueStr = ','.join(insertValue)

inertBankSql= "INSERT INTO bank_simple "\
              "(Age, Job, Marital, Education, xDefault, Balance, Housing, UPDATETIME) "\
              "VALUES " + insertValueStr + ";"

print(inertBankSql)
cursor.execute(inertBankSql)

# Make sure data is committed to the database
cnx.commit()

cursor.close()
cnx.close()
