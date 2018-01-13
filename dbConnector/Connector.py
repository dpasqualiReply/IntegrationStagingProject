import sys
import psycopg2


class Connector(object):
    def __init__(self, db_name, user, host, port, password):

        self.connection = None
        self.cursor = None

        self.db_name = db_name
        self.user = user
        self.host = host
        self.port = port
        self.password = password

    """ Connection Management =================================== """

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                "dbname='" + self.db_name + "' user='" + self.user + "' host='" + self.host + "' port='" + self.port + "' password='" + self.password + "'")
            self.cursor = self.connection.cursor()

        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    def close(self):
        try:
            self.connection.commit()
            self.cursor.close()
            self.connection.close()

            self.cursor = None
            self.connection = None

        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    """ SelectAll ============================================== """

    def selectAll(self, table_name):
        try:
            self.cursor.execute("SELECT * FROM %s" % table_name)
            return self.cursor.fetchall()


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    def printAll(self, table_name):
        try:
            for row in self.selectAll(table_name):
                print str(row)


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

    def runQuery(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)
    
    def showQuery(self, table_name):
        try:
            for row in self.runQuery(table_name):
                print str(row)


        except Exception as e:
            print "[ EXCEPTION ]" + str(e)
            sys.exit(1)

