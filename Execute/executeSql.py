from route import mysql
from Execute import responses,middleware
import pyodbc
# conn_str = 'DRIVER={SQL Server};SERVER=10.11.48.20;PORT=1433;DATABASE=DEV_PIL_RPA;UID=DEV_PIL_RPA;PWD=Dev#Rpa01'
# connection = pyodbc.connect('DRIVER={SQL Server};SERVER=10.11.48.20;PORT=1433;DATABASE=DEV_PIL_RPA;UID=DEV_PIL_RPA;PWD=Dev#Rpa01')

# connection = pyodbc.connect('DRIVER={SQL Server};SERVER=172.16.12.22;PORT=1433;DATABASE=PRD_PIL_RPA;UID=PRD_PIL_RPA;PWD=Prd#Rpa01')
conn_str = 'DRIVER={SQL Server};SERVER=172.16.12.22;PORT=1433;DATABASE=PRD_PIL_RPA;UID=PRD_PIL_RPA;PWD=Prd#Rpa01;'
# connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-5KG5U9P;DATABASE=DB_RPA;Trusted_Connection=yes;')Auto_Commit=true
#for Selecting All record
import cx_Oracle
dsn_tns_uat = cx_Oracle.makedsn('10.11.48.20', '1521', service_name='DUPILDB')
dsn_tns = cx_Oracle.makedsn('172.16.12.20', '1521', service_name='PRDPILDB') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
#connorl = cx_Oracle.connect(user='UAT_PGT', password='Upgts#01', dsn=dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'
def ExecuteAll(query,data):
    try:
        
        cur = mysql.connection.cursor()
        cur.execute(query,data)
        result = cur.fetchall()
        mysql.connection.commit()
        cur.close()
        return result
    except Exception as e:
                print("Error in ExecuteAll=============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1023300')

#for selecting one record
def ExecuteReturn(query,data):
    try:
        connection=pyodbc.connect(conn_str)
        cur=connection.cursor()
        cur.execute(query,data)
        result=responses.execution_200
        # cur = mysql.connection.cursor()
        # cur.execute(query,data)
        # result = cur.fetchone()
        
        connection.commit()
        cur.close()
        return result
    except Exception as e:
                print("Error in ExecuteReturn============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1022300')


#for executing one record with no return data
def ExecutereturnOne(query,data):
    try:
        #cur = mysql.connection.cursor()
        #cur.execute(query,data)
        connection=pyodbc.connect(conn_str)
        cur=connection.cursor()
        cur.execute(query,data)
        cur.execute("SELECT @@IDENTITY AS ID;")
        id=cur.fetchone()
        result=id[0]
        connection.commit()
        cur.close()
        return result
    except Exception as e:
                print("Error in ExecuteOne==============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1020300')
########################reurt with id
def ExecuteOne(query,data):
    try:
        #cur = mysql.connection.cursor()
        #cur.execute(query,data)
        connection=pyodbc.connect(conn_str)
        cur=connection.cursor()
        cur.execute(query,data)
        result=responses.execution_200
        connection.commit()
        cur.close()
        return result
    except Exception as e:
                print("Error in ExecuteOne==============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1020300')
####oracle insert
def ExecuteOrcOne(query,data):
    try:
        #cur = mysql.connection.cursor()
        #cur.execute(query,data)
        # connorl = cx_Oracle.connect(user='UAT_PGT', password='Upgts#01', dsn=dsn_tns_uat)
        connorl = cx_Oracle.connect(user='PRD_GT_RPA', password='PrdGtrpa#01', dsn=dsn_tns)
        c = connorl.cursor()
        outVal = c.var(cx_Oracle.CURSOR)
        c.callproc(query, data)
        # data=outVal.fetchone()
        result=responses.execution_200
        #c.execute('EXEC PROC_INST_MEASUREMENT_ENTRY_RPA "28/12/2021","Ankot-HVJ-GAIL","3006233","108111.50","97365.88","sanket.sagvekar"')
        print(outVal)
        c.close()
        connorl.close()
        return result
    except Exception as e:
                print("Error in ExecuteOrcOne==============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1020300')  
####oracle getdata
def ExecuteOrcgetall(query,data):
    try:
        #cur = mysql.connection.cursor()
        #cur.execute(query,data)
        
        connorl = cx_Oracle.connect(user='PRD_GT_RPA', password='PrdGtrpa#01', dsn=dsn_tns)
        c = connorl.cursor()
        outVal = c.var(cx_Oracle.CURSOR)
        c.callproc(query, [data,outVal])
        data=outVal.getvalue().fetchall()
        row_headers=[x[0] for x in outVal.getvalue().description] #this will extract row headers
                #mysql.connection.commit()
        # connorl.commit()
        c.close()
        payload = []
        for result in data:
            payload.append(dict(zip(row_headers,result)))

        connorl.close()
        return payload
        
    except Exception as e:
                print("Error in ExecuteOrcgetall==============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1020300')  

#for executing many record with no return data
def ExecuteMany(query,data):
    try:
        cur = mysql.connection.cursor()
        cur.executemany(query,data)
        result=responses.execution_200
        mysql.connection.commit()
        cur.close()
        return result
    except Exception as e:
                print("Error in ExecuteMany==============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1020300')

#for returning Inserted Id
def ExecuteReturnId(query,data):
    try:
        connection=pyodbc.connect(conn_str)
        cur = mysql.connection.cursor()
        cur.execute(query,data)
        id=mysql.connection.insert_id()
        result = id
        mysql.connection.commit()
        cur.close()
        return result
    except Exception as e:
                print("Error in ExecuteReturnId=============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1022300')



def ExecuteAllNew(query,data):
    try:
        #cur = mysql.connection.cursor()
        connection=pyodbc.connect(conn_str)
        cur=connection.cursor()
        cur.execute(query,data)
        results = cur.fetchall()
        if results == ():
            return middleware.exe_msgs(responses.execution_501,"No Record Found",'1023300')
        row_headers=[x[0] for x in cur.description] #this will extract row headers
        #mysql.connection.commit()
        connection.commit()
        cur.close()
        payload = []
        for result in results:
            payload.append(dict(zip(row_headers,result)))
        return payload
    except Exception as e:
        print("Error in ExecuteAll=============================",e)
        return middleware.exe_msgs(responses.execution_501,str(e.args),'1023300')


def ExecuteAllNew1(query):
    try:
        #cur = mysql.connection.cursor()
        connection=pyodbc.connect(conn_str)
        cur=connection.cursor()
        cur.execute(query)
        results = cur.fetchall()
        if results == ():
            return middleware.exe_msgs(responses.execution_501,"No Record Found",'1023300')
        row_headers=[x[0] for x in cur.description] #this will extract row headers
        #mysql.connection.commit()
        connection.commit()
        cur.close()
        payload = []
        for result in results:
            payload.append(dict(zip(row_headers,result)))
        return payload
    except Exception as e:
        print("Error in ExecuteAll=============================",e)
        return middleware.exe_msgs(responses.execution_501,str(e.args),'1023300')

def ExecuteRaw(query,data):
    try:
        # cur = mysql.connection.cursor()
        connection=pyodbc.connect(conn_str)
        cur=connection.cursor()
        cur.execute(query,data)
        result = cur.fetchall()
        # mysql.connection.commit()
        connection.commit()
        cur.close()
        return result
    except Exception as e:
                print("Error in ExecuteRaw=============================",e)
                return middleware.exe_msgs(responses.execution_501,str(e.args),'1023300')


