#!/usr/bin/env python

#Version 1.3

import MySQLdb
from multiprocessing import Process,Queue
import argparse

def parse_arguments():
    '''Parses the command line arguments'''
    parser=argparse.ArgumentParser(description="Compares two databases and checks for content differences")
    parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true")
    parser.add_argument("bd_user", help="User to connect to the database")
    parser.add_argument("bd_pass", help="Password of the database user")
    parser.add_argument("bd_host1", help="hostname or IP where the first database is hosted")
    parser.add_argument("bd_host2", help="hostname or IP where the second database is hosted")
    return(parser.parse_args()) 



#Parameters: bd_user.- database user to connect with
#            bd_password.- bd_user password in the database
#            bd_host.- host name or IP of the database server
#Returns:    a dictionary with the collected data from the database server
def collect_data_from_base(bd_user,bd_password,bd_host,result_queue):
    '''Collects the databases, tables and row counts, and save all in a dictionary that
    will be returned for comparison'''
    try:
        cnx = MySQLdb.connect(user=bd_user,passwd=bd_password,host=bd_host)
    except:
        print "UNKNOWN - Cannot connect to database %s" % bd_host
        exit(3)
    cursor=cnx.cursor()
    cursor.execute("show databases")
    databases=cursor.fetchall() #All database names
    cursor.close()
#Fill in the dictionary
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
                        print "CRITICAL - table %s is already in dictionary" % t[0]
                        exit(2)
                dict_bases[d[0]]=dict_tables #Assign the tables dictionary to the databases dictionary
                crs.close()
            else: #Database already in dictionary
                print "CRITICAL - database %s is already in dictionary" % d[0]
                exit(2)
    #Here I should have the data from the first database in dict_bases
    result_queue.put(dict_bases)


#Parameters: bd_host1; bdw_host.- database servers to compare
#            dict1_bases; dict2_bases.- dictionary with the results from the queries to
#                                       the databases
#Returns: Nothing
def show_diffs(bd_host1,bd2_host,dict1_bases,dict2_bases):
    '''Follow the whole two dictionaries and compare every element, showing them on the
    screen.  If any difference is found, stop the and wait for keypress '''
    print "\n-SERVERS:\n\n %s\n %s" % (bd_host1,bd2_host)
    list_dbs1=dict1_bases.keys()
    list_dbs2=dict2_bases.keys()
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


#Parameters: bd_host1; bdw_host.- database servers to compare
#            dict1_bases; dict2_bases.- dictionary with the results from the queries to
#                                       the databases
#Returns: String with the first different found
def return_first_diff(bd_host1,bd2_host,dict1_bases,dict2_bases):
    '''Follow the whole two dictionaries, and compare every element, if any difference is
    found return with an error, and just show the difference.  It stops at the first
    difference found, if any'''
    return_string=''
    list_dbs1=dict1_bases.keys()
    list_dbs2=dict2_bases.keys()
    if not list_dbs1 == list_dbs2: #Database lists are different
        return_string="Different databases between servers: %s - %s" % (list_dbs1,list_dbs2)
    else: #Databases are the same in both servers
        for common_db in list_dbs1:
            list_tables1=dict1_bases[common_db].keys()
            list_tables2=dict2_bases[common_db].keys()
            if not list_tables1 == list_tables2: #Lists of tables are different
                return_string="Different tables: Database=%s Uncommon table=%s" % (common_db,list(set(list_tables1) ^ set(list_tables2)))
            else: #Both lists of tables are the same
                for common_table in list_tables1:
                    if not dict1_bases[common_db][common_table] == dict2_bases[common_db][common_table]: #Row counts are different
                        return_string="Database=%s Table=%s Row count3=%d Row count2=%d" % (common_db,common_table,dict1_bases[common_db][common_table],dict2_bases[common_db][common_table])
    return return_string


##MAIN##

if __name__ == '__main__':
    arguments=parse_arguments()
    job_list=list()
    result_queue=Queue() #To store the data from the DBs
    db_connection_data=[(arguments.bd_user,arguments.bd_pass,arguments.bd_host1),
                        (arguments.bd_user,arguments.bd_pass,arguments.bd_host2)]
    #Gather information from the two databases in parallel
    for bd_user,bd_user_pwd,bd_host in db_connection_data:
        job=Process(target=collect_data_from_base,args=(bd_user,bd_user_pwd,bd_host,result_queue))
        job_list.append(job)
        job.start()
    #Wait for the processes to finish
    for p in job_list:
        p.join()
        if p.exitcode: #Process exit with exit code not zero
            exit(p.exitcode)
    if result_queue.qsize() == 2:
        r1=result_queue.get()
        r2=result_queue.get()
        if r1 == r2: 
            print "OK - Data matches"
            if arguments.verbose: 
                show_diffs(db_connection_data[0][2],db_connection_data[1][2],r1,r2)
            exit(0)
        else:
            perf_data=return_first_diff(db_connection_data[0][2],db_connection_data[1][2],r1,r2)
            print "CRITICAL - Data missmatch | %s" % perf_data
            if arguments.verbose:
                show_diffs(db_connection_data[0][2],db_connection_data[1][2],r1,r2)
            exit(2)
    else:
        print "UNKNOWN: Number of result sets is %d, should be 2" % result_queue.qsize()
        exit(3)
