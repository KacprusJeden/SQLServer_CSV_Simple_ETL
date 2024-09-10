import os
import pyodbc
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation

# Author: Kacper Prusiński

# This is a program to simple migration data (ETL) between two databases: AmazonKP and AmazonStageKP in SqlServer

# connection parameters
server = 'localhost'
database = 'AmazonKP'
database2 = 'AmazonStageKP'
username = 'sa'
password = 'password'

# connection objects
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn_str2 = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database2};UID={username};PWD={password}'


# connection to databases
conn = pyodbc.connect(conn_str)
print(conn_str)
print(conn)
conn2 = pyodbc.connect(conn_str2)

# creating cursor
cursor = conn.cursor()
cursor2 = conn2.cursor()

# ETL - functions

# query to get all foreign keys in database
getForeignKeys = """
    SELECT 
        fk.name AS 'PrimaryKeyName',
        OBJECT_SCHEMA_NAME(fk.parent_object_id) + '.' + OBJECT_NAME(fk.parent_object_id) AS 'TableWithForeignKey',
        c.name AS 'ColumnWithForeignKey',
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '.' + OBJECT_NAME(fk.referenced_object_id) AS 'TableWithPrimaryKey',
        pc.name AS 'ColumnWithPrimaryKey'
    FROM 
        sys.foreign_keys fk
    INNER JOIN 
        sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
    INNER JOIN 
        sys.columns c ON c.object_id = fk.parent_object_id AND c.column_id = fkc.parent_column_id
    INNER JOIN 
        sys.columns pc ON pc.object_id = fk.referenced_object_id AND pc.column_id = fkc.referenced_column_id
    ORDER BY 
        'TableWithForeignKey', 'PrimaryKeyName';
"""
# dropping all foreign keys from database, writing to list
def dropForeignKeys():
    cursor.execute(getForeignKeys)
    results = cursor.fetchall()
    rowList = []
    for row in results:
        rowList.append(row)
        cursor.execute(f"ALTER TABLE {row[1]} DROP CONSTRAINT {row[0]}")
        conn.commit()
    return rowList

# restoring all foreign keys to database from list
def restoreForeignKeys(foreignKeyList):
    for foreignKey in foreignKeyList:
        cursor.execute(f"""ALTER TABLE {foreignKey[1]} ADD CONSTRAINT {foreignKey[0]} FOREIGN KEY ({foreignKey[2]})
                REFERENCES {foreignKey[3]}({foreignKey[4]})""")
        conn.commit()

#extracting data
def exportDataToCsvFile(table):
    try:
        cursor.execute(f'SELECT * FROM {table}')
        results = cursor.fetchall()
        fileSource = f'{table}_stage{table}.csv'
        with open(fileSource, 'w+', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',')
            rowCounter = 0
            for row in results:
                rowCounter += 1
                row_values = list(row)  # Convert row to values list
                csvwriter.writerow(row_values)
        if len(results) == rowCounter:
            return fileSource
        else:
            os.remove(fileSource)
            print('Not all records exported to csv')
            return 'x'
    except pyodbc.ProgrammingError:
        print('Table does not exist')

# clear table
# export data to csv and clear table ('delete from', not 'truncate', because of error due to present of primary and
# foreign key)
def truncateTable(table):
    try:
        cursor.execute(f'TRUNCATE TABLE {table}')
        conn.commit()
        cursor.execute(f'SELECT * FROM {table}')
        results = cursor.fetchall()
        return len(results) == 0
    except pyodbc.ProgrammingError:
        print('Table does not exist')


# transforming data
def transformDateValue(date):
    return datetime.fromisoformat(date).strftime('%Y/%m/%d')

def transformNumberValue(value):
    number = Decimal(value)
    return number if isinstance(number, int) else round(number)

def transformValueToQuery(value):
    # from csv we get all values as string, but if it is possible, we can convert string to another type
    try:
        return transformNumberValue(value)
    except InvalidOperation:
        try:
            return f"'{transformDateValue(value)}'"
        except ValueError:
            return f"'{value}'"

# creating insert query
def insertInto(table, argList):
    # formatting query
    # transformation data is necessary because part of data are as string, date or number
    # so, in query cannot be value taken directly from csv. For example, when we get string value, we have a value
    # without apostrophes, but it must be in query. And at number they don't have to be.
    query = f"insert into {table} values ("
    for i in range(len(argList)):
        if i < len(argList) - 1:
            query += f'{transformValueToQuery(argList[i])},'
        else:
            query += str(transformValueToQuery(argList[i]))
    query += ")"
    return query

# load data to table from csv file
def importDataFromCsvFile(table, fileName):
    try:
        with open(fileName, 'r', newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            for row in csvreader:
                cursor2.execute(insertInto(table, row))
                conn2.commit()
    except pyodbc.ProgrammingError:
        print('Table does not exist')


# ETL in practice (for this, NOT EVERY situation)
print(F'ETL from {database} to {database2}:')
successful, unsuccessful = 0, 0

tablesToExport = ['SalesKP', 'ProductMasterKP', 'CustomerMasterKP', 'CategoryMasterKP']
tablesToImport = ['StageSalesKP', 'StageProductMasterKP', 'StageCustomerMasterKP', 'StageCategoryMasterKP']

foreignKeysList = dropForeignKeys()

if len(tablesToExport) == len(tablesToImport):
    for i in range(len(tablesToExport)):
        # get file name
        fileName = exportDataToCsvFile(tablesToExport[i])
        # if file not exists, variable fileName has symbolic value: 'x'
        if len(fileName) != 'x':
            # we assume, that file exists
            # clear table (export)
            truncateTable(tablesToExport[i])
            # import data from csv, transformation is inside a function 'importDataFromCsvFile'
            importDataFromCsvFile(tablesToImport[i], fileName)
            # remove file csv
            os.remove(fileName)
            # is operation successful
            print(f'{tablesToExport[i]} -> {tablesToImport[i]}: successful')
            successful += 1
        else:
            # if file no exists, let's go to the next table
            if i < len(tablesToExport) - 1:
                print(f'{tablesToExport[i]} -> {tablesToImport[i]}: unsuccessful')
                unsuccessful += 1
                i += 1
    restoreForeignKeys(foreignKeysList)
else:
    print('different number of table')
    unsuccessful = len(tablesToExport)

# reasume process
print(f'Successful: {successful} ({int(successful * 100/(successful + unsuccessful))}%)')
print(f'Unsuccessful: {unsuccessful} ({int(unsuccessful * 100/(successful + unsuccessful))}%)')

# Close cursors and connections
cursor2.close()
conn2.close()

cursor.close()
conn.close()
