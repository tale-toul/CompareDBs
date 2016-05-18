#!/usr/bin/env python

import MySQLdb

#Gather information from first database#
bd_user='mogutu'
bd_password='seseseco'
bd_host='motril.epsa.junta-andalucia.es'

cnx = MySQLdb.connect(user=bd_user,passwd=bd_password,host=bd_host)
cursor=cnx.cursor()
cursor.execute("show databases")
databases=cursor.fetchall()
cursor.close()

dict_bases=dict()

for d in databases:
    if not d[0].upper().endswith('SCHEMA'):
        if not d[0] in dict_bases:
            cone=MySQLdb.connect(user=bd_user,passwd=bd_password,host=bd_host,db=d[0])
            crs=cone.cursor()
            crs.execute("show tables")
            tables=crs.fetchall()
            dict_tables=dict()
            for t in tables:
                crs.execute("SELECT COUNT(*) from %s" % (t[0],))
                count=crs.fetchone()
                if not t[0] in dict_tables:
                    dict_tables[t[0]]=count[0]
                else:
                    print "Big ERROR table %s is already in dictionary" % t[0]
            dict_bases[d[0]]=dict_tables
            crs.close()
        else: #Database already in dictionary
            print "Big ERROR database %s is already in dictionary" % d[0]
            exit(-1)


#Gather information from second database#
bd2_user='mogutu'
bd2_password='seseseco'
bd2_host='motril.epsa.junta-andalucia.es'

cnx = MySQLdb.connect(user=bd_user,passwd=bd_password,host=bd_host)
cursor=cnx.cursor()
cursor.execute("show databases")
databases=cursor.fetchall()
cursor.close()

dict2_bases=dict()

for d in databases:
    if not d[0].upper().endswith('SCHEMA'):
        if not d[0] in dict2_bases:
            cone=MySQLdb.connect(user=bd2_user,passwd=bd2_password,host=bd2_host,db=d[0])
            crs=cone.cursor()
            crs.execute("show tables")
            tables=crs.fetchall()
            dict2_tables=dict()
            for t in tables:
                crs.execute("SELECT COUNT(*) from %s" % (t[0],))
                count=crs.fetchone()
                if not t[0] in dict2_tables:
                    dict2_tables[t[0]]=count[0]
                else:
                    print "Big ERROR table %s is already in dictionary" % t[0]
            dict2_bases[d[0]]=dict2_tables
            crs.close()
        else: #Database already in dictionary
            print "Big ERROR database %s is already in dictionary" % d[0]
            exit(-1)

list_dbs1=sorted(dict_bases.keys())
list_dbs2=sorted(dict2_bases.keys())
if list_dbs1 == list_dbs2:
    print "Databases are the same in both servers: \n%s.-%s\n%s.-%s" % (bd_host,list_dbs1,bd2_host,list_dbs2)
    for common_db in list_dbs1:
        list_tables1=sorted(dict_bases[common_db].keys())
        list_tables2=sorted(dict2_bases[common_db].keys())
        if list_tables1 == list_tables2:
            print "Tables in %s are the same in both servers: \n%s\t%s" % (common_db,bd_host,bd2_host)
            for 

else:
    print "Different databases in each server: \n%s.-%s\n%s.-%s" % (bd_host,list_dbs1,bd2_host,list_dbs2)
    exit (-2)

