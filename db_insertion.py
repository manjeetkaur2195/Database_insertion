import pyodbc
from configparser import ConfigParser

#Getting DB details from config.ini file
def get_dbDetails():
    dict_db ={}
    config = ConfigParser(interpolation=None)
    config.read("config.ini")
    dict_db['DB_Address'] = config['dbDetails']['Data Source']
    dict_db['DB_Name'] = config['dbDetails']['Initial Catalog']
    dict_db['DB_UserId'] = config['dbDetails']['User ID']
    dict_db['DB_Password'] = config['dbDetails']['Password']
    dict_db['DB_PythonTransactions_TableName'] = config['dbDetails']['DB PythonTransactions TableName']
    return dict_db

# '''Creating DB connection
#    Argument DB details dictionary
#    Output Bool ( Connected / Not Connected )'''
def create_dbConnection(dict_db):
    connectionStatus = False
    db_connection_string = 'Server={0};Database={1};UID={2};PWD={3}'.format(str(dict_db["DB_Address"]),
                                                                            str(dict_db["DB_Name"]),
                                                                            str(dict_db["DB_UserId"]),
                                                                            str(dict_db["DB_Password"]))
    try:
        conn = pyodbc.connect('Driver={SQL Server};' + db_connection_string)
        print("Connected")
        connectionStatus =True

        #Creating a connection cursor
        cursor = conn.cursor()
    except Exception as Ex:
        print("Error while creating DB Connection. Error Message: "+str(Ex))
        raise
    return connectionStatus,cursor,conn

# DB Insertion Query
def insertion_query(dict_db):
    insert_query = """
    INSERT INTO {0} 
    (
    [Country],[SourceParentFileName],[SourceParentFilePath],
    [TransactionDesc],[POFileName],
    [TransactionID],[GST_Amount],[ExecutionTimeStamp],
    [PONumber],[TransactionOverallStatus],[StatusReason]
    ) 
    VALUES
    (?,?,?,?,?,?,?,?,?,?,?)
    """.format(str(dict_db["DB_PythonTransactions_TableName"]))
    return insert_query

def execute_sql(master_dt):
    try:
        records_to_insert = master_dt.values.tolist()
        dict_db = get_dbDetails()
        connectionStatus, cursor, conn = create_dbConnection(dict_db)
        insert_query = insertion_query(dict_db)

        if connectionStatus:
            cursor.executemany(insert_query, records_to_insert)
            conn.commit()
    except:
        raise




