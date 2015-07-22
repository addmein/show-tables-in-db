import psycopg2, fnmatch, os

source = "B:\\DTP DEPARTMENT\\LIBRARY TIF\\Spanish"
table = "sp_lib"
extension = "*.tif"

class filelist:
    def connected(self):
        print "Connected to the database"
        
    def notConnected(self):
        print "Could not connect to the database"
        
    def taskCompleted(self):
        print "\nTASK COMPLETED"

    def establishConnection(self, db):
        conn = psycopg2.connect("dbname='%s' user='postgres' host='localhost' password='123456'" %db)
        return conn
    
    def testConnection(self):
        conn = x.establishConnection('filelist')
        if conn:
            filelist().connected()
            return 1
        else:
            filelist().notConnected()
            return 0
         
    def showDatabases(self):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        cur.execute("""SELECT datname from pg_database""")
        rows = cur.fetchall()
        
        print "\nShow me the databases:"
        for row in rows:
            print " ", row[0]        
    
    def checkTable(self, tableName):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        cur.execute("""SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='%s')""" %tableName)
        exists = cur.fetchone()[0]
        return exists
    
    def createTable(self, tableName):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        cur.execute("""CREATE TABLE %s(ID SERIAL PRIMARY KEY, 
                                        FileType VARCHAR(3), 
                                        FileName VARCHAR(20), 
                                        FilePath VARCHAR(99))""" %tableName)
        conn.commit()
        print "Table '%s' created" %tableName
       
    def deleteTable(self, tableName):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        cur.execute("""DROP TABLE %s""" %tableName)
        conn.commit()
        print "Table '%s' deleted" %tableName
        
    def showTables(self):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        cur.execute("""SELECT table_schema,table_name
                        FROM information_schema.tables
                            WHERE table_type = 'BASE TABLE'
                                AND table_schema = 'public'
                        ORDER BY table_schema,table_name""")
        tables = cur.fetchall()
        for table in tables:
            cur.execute("""SELECT COUNT (*) FROM %s""" % table[1])
            files = cur.fetchall()
            print table[1], "\t", files[0][0]
            
    def insert_into_db(self):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        for root, dirnames, filenames in os.walk(source):
            print("Processing %s" % root)
            for filename in fnmatch.filter(filenames, "*.eps"):
                print filename
                print root
                ext = os.path.splitext(filename)[1][1:]
                print ext
                cur.execute(""" INSERT INTO %s (FileType, FileName, FilePath) VALUES( '%s', '%s', '%s' ); 
                                """ %(table, ext, filename, root))
                conn.commit()

    def insert_file_into_db(self, ext, filename, root):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        cur.execute(""" INSERT INTO %s (FileType, FileName, FilePath) VALUES( '%s', '%s', '%s' ); 
                                """ %(table, ext, filename, root))
        conn.commit()
    
    def search_file(self, filename):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        cur.execute("""SELECT exists (SELECT 1 FROM %s WHERE filename = '%s' LIMIT 1);""" %(table, filename))
        exists = cur.fetchone()[0]
        return exists
    
    def update_db(self):
        conn = x.establishConnection('filelist')
        cur = conn.cursor()
        for root, dirnames, filenames in os.walk(source):
            print("Processing %s" % root)
            for filename in fnmatch.filter(filenames, "%s" %extension):
                print filename
                print root
                ext = os.path.splitext(filename)[1][1:]
                print ext
                print x.search_file(filename)
                if (not x.search_file(filename)):
                    print "Inserting file in database..."
                    x.insert_file_into_db(ext, filename, root)
    
        
    
"""
==================================================
"""
x = filelist()
#x.createTable("epsai")
#x.update_db()
x.showTables()
#x.taskCompleted()