#!/usr/bin/env python

#Version 1.1

import MySQLdb
from multiprocessing import Process,Queue

#Parameters: bd_user.- database user to connect with
#            bd_password.- bd_user password in the database
#            bd_host.- host name or IP of the database server
#Returns:    a dictionary with the collect data from the database server
def collect_data_from_base(bd_user,bd_password,bd_host,result_queue):
    '''Collect the databases, its tables and the row counts and save in a dictionary that
    will be returned for comparison'''
    cnx = MySQLdb.connect(user=bd_user,passwd=bd_password,host=bd_host)
    cursor=cnx.cursor()
    cursor.execute("show databases")
    databases=cursor.fetchall() #All database names
    cursor.close()

    dict_bases=dict() #Main dictionary to hold the collected information
    for d in databases:
        if not d[0].upper().endswith('SCHEMA'): #Ignore the *_schema databases
            if not d[0] in dict_bases: #Database should not exist in the dictionary
                cone=MySQLdb.connect(user=bd_user,passwd=bd_password,host=bd_host,db=d[0])
                crs=cone.cursor()
                crs.execute("show tables")
                tables=crs.fetchall() #Get the whole list of result at once because I need to
                                      # reuse the cursor
                dict_tables=dict() #Dictionary holding {table_name:row_count} records
                for t in tables:
                    crs.execute("SELECT COUNT(*) from %s" % (t[0],)) #Count of rows in table
                    count=crs.fetchone()
                    if not t[0] in dict_tables: #Table should not exist yet in the dictionary
                        dict_tables[t[0]]=count[0] #Add an entry to the dictionary {table_name:row_count}
                    else:
                        print "Big ERROR table %s is already in dictionary" % t[0]
                        exit(-2)
                dict_bases[d[0]]=dict_tables #Assign the tables dictionary to the databases dictionary
                crs.close()
            else: #Database already in dictionary
                print "Big ERROR database %s is already in dictionary" % d[0]
                exit(-1)
    #Here I should have the data from the first database in dict_bases
    result_queue.put(dict_bases)


#Parameters: bd_host1; bdw_host.- database servers to compare
#            dict1_bases; dict2_bases.- dictionary with the results from the queries to
#                                       the databases
#Returns: Nothing
def show_diffs(bd_host1,bd2_host,dict1_bases,dict2_bases):
    print "\n-SERVERS:\n\n %s\n %s" % (bd_host1,bd2_host)
    list_dbs1=sorted(dict1_bases.keys())
    list_dbs2=sorted(dict2_bases.keys())
    if list_dbs1 == list_dbs2:
        print "\n-DATABASES:"
        for common_db in list_dbs1:
            list_tables1=dict1_bases[common_db].keys()
            list_tables2=dict2_bases[common_db].keys()
            if list_tables1 == list_tables2:
                print "\n-Tables in database %s:\n\n%39s %39s" % (common_db,bd_host1,bd2_host)
                for common_table in list_tables1:
                    print "%35s:%5d %35s:%5d" % (common_table,dict1_bases[common_db][common_table],common_table,dict2_bases[common_db][common_table])
                    if not dict1_bases[common_db][common_table] == dict2_bases[common_db][common_table]:
                        print "COUNT MISSMATCH"
                        raw_input("Press any key to continue")
            else:
                print "TABLES MISSMATCH \n %s.-%s\n %s.-%s" % (bd_host1,list_tables1,bd2_host,list_tables2)
    else:
        print "\nDIFFERENT DATABASES IN EACH SERVER: \n %s.-%s\n %s.-%s" % (bd_host1,list_dbs1,bd2_host,list_dbs2)
        exit (-2)


##MAIN##

if __name__ == '__main__':
    job_list=list()
    result_queue=Queue() #To store the data from the DBs
    db_connection_data=[('mdsole','LamidelaSo','lomopardo.epsa.junta-andalucia.es'),
                        ('mdsole','LamidelaSo','guadalcacin.epsa.junta-andalucia.es')]
    #Gather information from the two databases at once 
    for bd_user,bd_user_pwd,bd_host in db_connection_data:
        job=Process(target=collect_data_from_base,args=(bd_user,bd_user_pwd,bd_host,result_queue))
        job_list.append(job)
        job.start()
    #Wait for the processes to finish
    for p in job_list:
        p.join()
    if result_queue.qsize() == 2:
        r1=result_queue.get()
        r2=result_queue.get()
        if r1 == r2: 
            print "OK - Data matches"
            show_diffs(db_connection_data[0][2],db_connection_data[1][2],r1,r2)
        else:
            print "ERROR - Data missmatch" 
            show_diffs(db_connection_data[0][2],db_connection_data[1][2],r1,r2)
    else:
        print "ERROR, number of result sets is not 2: %d" % result_queue.qsise()
        exit(-4)
      

