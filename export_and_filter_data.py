#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script that uses mysql_connector class to call database, get all the tables,
filter text and json fields and export tables to comma separated csv files
use like:
python filter_data.py <hostname> <username> <password> <database>
<server-ca.pem> <client-cert.pem> <client-key.pem> <path_to_export_tables>
"""
__author__ = "Ari Lopez"

from mysql_connector import *
import sys
import csv
import datetime

_host=sys.argv[1]
_user=sys.argv[2]
_password=sys.argv[3]
_database=sys.argv[4]
_ca=sys.argv[5]
_cert=sys.argv[6]
_key=sys.argv[7]
_path_to_export_tables=sys.argv[8]

def getDateNow():
     return datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

def create_columns(table_name,data):
    new_row=[]
    for row in data:
        for column in row:
            column=str(column)
            new_row.append(column)
    myFile = open(_path_to_export_tables + '/' + table_name + '.csv', 'w')
    with myFile:
        writer = csv.writer(myFile)
        writer.writerow(new_row)

def filter_data(table_name,columns,data):
    TABLE_TYPE=1
    str_mysql_types=["char","varchar","tinytext","text","mediumtext","longtext"]
    new_data=[]
    for row in data:
        new_row=[]
        index = 0
        for column in row:
            if column == None:
                new_row.append(column)
            else:
                if columns[index][TABLE_TYPE] in str_mysql_types:
                    new_col=column.replace("\n", "").replace("\r", "")
                    new_row.append(new_col.encode("utf-8"))
                elif "json" in columns[index]:
                    new_col=column.replace("\n", "").replace("\r", "")
                    new_row.append(new_col.encode("utf-8"))
                else:
                    new_row.append(column)
            index += 1
        new_data.append(new_row)
    myFile = open(_path_to_export_tables + '/' + table_name + '.csv', 'a')
    try:
        with myFile:
            writer = csv.writer(myFile)
            writer.writerows(new_data)

    except UnicodeEncodeError as uee:
        print str(uee)
        sys.exit()

if __name__ == "__main__":

    con = Mysql_connector(host=_host,
     user=_user,
     password=_password,
     database=_database,
     ca=_ca,
     key=_key,
     cert=_cert)


    db_tables=con.select('SHOW TABLES;')
    # db_tables=[["HsAddress"]]
    for table in db_tables:
        print(str(getDateNow()) + " Exporting data for: " + str(table[0]) + " table")
        table=str(table[0])
        columns_types = con.nice_select('INFORMATION_SCHEMA.COLUMNS',
                            'table_name = "'+ table +'"','COLUMN_NAME', 'DATA_TYPE')
        column_names = con.nice_select('INFORMATION_SCHEMA.COLUMNS',
                            'table_name = "'+ table +'"','COLUMN_NAME')
        create_columns(table,column_names)
        data = con.nice_select(table, None, '*')
        filter_data(table,columns_types,data)
