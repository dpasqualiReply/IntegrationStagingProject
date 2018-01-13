from Connector import *
import getopt

def printUsage():
    print "Usage --help --database=DB --psql_user=USER --psql_password=PASSWORD --selectAllFrom=TABLE | query"
    print "--help                       show this message"
    print "--selectAllFrom=TABLE        print all the entries of defined TABLE"
    print ""
    print "--query=QUERY                a query to execute"
    print ""
    print "--host=HOST                  host"
    print "--port=PORT                  port"
    print "--database=DB                postgres DB"
    print "--psql_user=USER             postgres DB user"
    print "--psql_password=PASSWORD     portegres DB password"

    exit(0)


if __name__ == '__main__':

    database = ""
    psql_user = ""
    psql_pass = ""
    psql_host = ""
    psql_port = ""

    select = False
    rq = False

    select_table = ""
    query = ""

    
    #print "ARGV:    ", sys.argv[1:]

    opts, rem = getopt.getopt(sys.argv[1:], "", ['help',
     'query=',
     'selectAllFrom=',
     'host=',
     'port=',
     'database=',
     'psql_user=',
     'psql_password='])


    #print "OPTIONS: ", opts

    for opt, arg in opts:
        if opt == '--help':
            printUsage()
            exit(0)
        if opt == '--database':
            database = arg
        if opt == '--psql_user':
            psql_user = arg
        if opt == '--psql_password':
            psql_pass = arg  
        if opt == '--host':
            host = arg  
        if opt == '--port':
            port = arg
        if opt == '--selectAllFrom':
            select_table = arg
            select = True   
        if opt == '--query':
            query = arg
            rq = True         
            
    """
    print "psql user    ", str(psql_user)
    print "psql pass    ", str(psql_pass)
    print "psql host    ", str(psql_host)
    print "psql port    ", str(psql_port)
    print "psql database    ", str(database)
    print "query    ", str(query)
    print "select from table    ", str(select_table)
    """

    #m20Connector = M20PSQLConnector('data_reply_db', 'dario', 'localhost', 'password')
    #m20Connector = M20PSQLConnector('postgres', 'cloudera-scm', 'localhost', '7432', 'y6jOvCiNAz')
    #connector = Connector('postgres', psql_user, 'localhost', '7432', psql_pass)
    connector = Connector(database, psql_user, psql_host, psql_port, psql_pass)
    connector.connect()

    if(rq):
        connector.showQuery(query)

    if(select):
        connector.printAll(select_table)

    connector.close()
