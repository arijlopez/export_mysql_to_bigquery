#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Class that wraps around mysql.connecto and uses ssl to connect and query
a mysql database.
"""
__author__ = 'Ari Lopez'

import mysql.connector
from mysql.connector import errorcode

class Mysql_connector(object):
    __instance = None

    __host = None
    __user = None
    __password = None
    __database = None
    __session = None
    __connection = None
    __ca = None
    __key = None
    __cert = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(Mysql_connector, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    def __init__(self, host='localhost', user='root', password='', database='', ca='', key='', cert=''):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
        self.__ca = ca
        self.__key = key
        self.__cert = cert

    #Open connection with database
    def _open(self):
        try:
            cnx = mysql.connector.connect(host=self.__host, user=self.__user, password=self.__password,
                                          database=self.__database, ssl_ca=self.__ca, ssl_key=self.__key, ssl_cert=self.__cert)
            self.__connection = cnx
            self.__session = cnx.cursor(buffered=True)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print 'Something is wrong with your user name or password'
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print 'Database does not exists'
            else:
                print err
    #Close connection
    def _close(self):
        self.__session.close()
        self.__connection.close()

    def insert(self, table, *args, **kwargs):
        values = None
        query = "INSERT INTO %s " % table
        if kwargs:
            keys = kwargs.keys()
            values = kwargs.values()
            query += "(" + ",".join(["`%s`"]*len(keys)) % tuple(keys) + ") VALUES(" + ",".join(["%s"]*len(values)) + ")"
        elif args:
            values = args
            query += " VALUES(" + ",".join(["%s"]*len(values)) + ")"
        self._open()
        self.__session.execute(query, values)
        self.__connection.commit()
        self._close()
        return self.__session.lastrowid

    def nice_select(self, table, where=None, *args):
        result = []
        query = "SELECT "
        keys = args
        l = len(keys) - 1
        for i, key in enumerate(keys):
            query += "`"+key+"`"
            if i < l:
                query += ","
        query += " FROM %s" % table
        if where:
            query += " WHERE %s" % where
        self._open()
        self.__session.execute(query)
        self.__connection.commit()
        for i in range(self.__session.rowcount):
          result.append(self.__session.fetchone())
        self._close()
        return result

    def select(self, query):
        result = []
        self._open()
        self.__session.execute(query)
        self.__connection.commit()
        for i in range(self.__session.rowcount):
          result.append(self.__session.fetchone())
        self._close()
        return result
