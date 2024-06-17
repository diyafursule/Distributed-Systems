from django.shortcuts import render
from django.db import connections
from django.db.utils import OperationalError
from django.http import JsonResponse

from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import re


READ_DATABASES = ['read1', 'read2', 'read3']
MAX_READS = len(READ_DATABASES)
LOAD_BALANCER_COUNTER = 0

SELECT_MASTERS = ['main','read1', 'read2', 'read3']
MAX_RETRIES = len(SELECT_MASTERS)

DATABASE_ALIASES = ['main','read1', 'read2', 'read3']
LOGS = {alias: [] for alias in DATABASE_ALIASES}


def get_master_connection():
    global SELECT_MASTERS

    # Sort the database aliases based on the decreasing order of the number of logs
    sorted_aliases = sorted(SELECT_MASTERS, key=lambda alias: len(LOGS[alias]), reverse=True)

    for db_alias in sorted_aliases:
        try:
            connection = connections[db_alias]
            connection.ensure_connection()
            return connection

        except OperationalError:
            # Move the selected database to the end of the list for the next iteration
            SELECT_MASTERS.remove(db_alias)
            SELECT_MASTERS.append(db_alias)

    # If all attempts fail, raise an OperationalError
    raise OperationalError("All master server connections failed.")




def append_to_logs(db_alias, log_entry):

    if db_alias in LOGS:
        LOGS[db_alias].append(log_entry)
        return True
    return False

def maintain_logs():
    max_logs = []
    max_length = -1
    max_length_db = None

    # Find the log array with the maximum length
    for db_alias, logs in LOGS.items():
        if len(logs) > max_length:
            max_length = len(logs)
            max_length_db = db_alias
            max_logs = logs
    

    if max_length_db is not None:
        # Process queries in the shorter log arrays
        for db_alias, logs in LOGS.items():
            if db_alias != max_length_db:
                check = process_queries(db_alias, len(logs),max_length_db)

                if check == False:
                    return False

            elif db_alias == max_length_db and logs!=max_logs:
                new_logs = []
                for i in range(len(logs)):
                    if logs[i] != max_logs[i]:
                        new_logs.append(logs[i])
                check = process_different_queris(db_alias,new_logs)

                if check == False:
                    return False
                
                check = process_different_queris(max_length_db,new_logs)

                if check == False:
                    return False
                print("Do something here")

        return True

    return False

def process_different_queris(db_alias, logs):
    for query in logs:
        try:
            connection = connections[db_alias]
            connection.ensure_connection()

            with connection.cursor() as cursor:
                cursor.execute(query)


            append_to_logs(db_alias, query)

        except OperationalError:
            return False
    return True


def process_queries(db_alias, max_length,max_db):
    queries_to_process = LOGS[max_db][max_length:]

    for query in queries_to_process:
        try:
            connection = connections[db_alias]
            connection.ensure_connection()

            with connection.cursor() as cursor:
                cursor.execute(query)


            append_to_logs(db_alias, query)
            

        except OperationalError:
            return False
    return True

def query_processing(db_alias,query):
    try:
        connection = connections[db_alias]
        connection.ensure_connection()

        with connection.cursor() as cursor:
            cursor.execute(query)
            
        
        return True

    except OperationalError:
        return False
    
def reverse_insert_query(insert_query):

    parts = insert_query.split("VALUES")
    values_part = parts[1].strip().lstrip('(').rstrip(')')

    values = [val.strip("'").strip() for val in values_part.split(', ')]

    into_index = insert_query.find("INTO")
    opening_parenthesis_index = insert_query.find("(", into_index)
    table_name = insert_query[into_index + 4:opening_parenthesis_index].strip()
    

    print(table_name)

    # Construct the WHERE clause for deletion
    where_clause = " AND ".join([f"{column} = '{value}'" for column, value in zip(get_table_columns(table_name), values)])

    # Construct the DELETE query
    delete_query = f"DELETE FROM {table_name} WHERE {where_clause};"
    print(delete_query)
    return delete_query

def get_table_columns(table_name):
    if table_name == 'Clubs':
        return ["ClubID", "ClubName", "ClubDescription"]
    
    elif table_name == 'Items':
        return ["ItemID", "ItemName", "ItemDescription","ClubID"]
    
    elif table_name == 'Members':
        return ["MemberID", "MemberName", "ContactNumber","Email","ClubID"]
    
    elif table_name == 'State':
        return ["StatusID", "ItemID", "PurchaseDate","WarrantyYearsLeft","AmountRequired","OtherDetails"]
    
    elif table_name == 'InventoryDistribution':
        return ["TransactionID", "ItemID", "MemberID", "Quantity", "TransactionType", "TransactionDate", "Duration", "CurrentLocation"]

    return ["ClubID", "ClubName", "ClubDescription"]

@api_view(['POST'])
def savedata(request):
    try:
        
        global LOAD_BALANCER_COUNTER

        db_alias = READ_DATABASES[LOAD_BALANCER_COUNTER % len(READ_DATABASES)]
        LOAD_BALANCER_COUNTER += 1

        # delete_query = reverse_insert_query("INSERT INTO Items (ItemID, ItemName, ItemDescription, ClubID) VALUES (1, 'Chess Set', 'Standard chess set for club meetings', 1),")
        check = maintain_logs()

        if check == False:
            return Response({'error': 'Log Updation Failed'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # for db_alias, logs in LOGS.items():
        #     print(f"Logs for {db_alias}: {logs}")

        connection = connections[db_alias]
        connection.ensure_connection()
        

        query = request.data.get('name', '')


        with connection.cursor() as cursor:
            cursor.execute(query)
            # cursor.execute("SELECT * FROM HOSPITAL WHERE hospital_name LIKE 'A%'", [f'{hospital_name}%'])
            columns = [col[0] for col in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        log_entry = query
        append_to_logs(db_alias, log_entry)

        check = maintain_logs()

        print(f"Connected to: {connection.alias}")
        for db_alias, logs in LOGS.items():
            print(f"Logs for {db_alias}: {logs}")

        if check:
            return JsonResponse({'data': data}, status=status.HTTP_200_OK)
        
        else:
            return Response({'error': 'Log Updation Failed'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    except OperationalError as e:
        return Response({'error': f'Database connection failed: {e}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

@api_view(['POST'])
def writedata(request):
    try:
        success_count = 0
        total_databases = len(SELECT_MASTERS)
        query = request.data.get('name', '')
        succes_alias = []

        maintain_logs()

        for db_alias in SELECT_MASTERS:
            check = query_processing(db_alias,query)
            if check:
                success_count += 1
                succes_alias.append(db_alias)

            #     success_count += 1
            #     succes_alias.append(db_alias)

            
        # Append to logs only if successful on more than 50% of the databases
        if success_count > total_databases / 2:
            log_entry = query
            for db_alias in succes_alias:
                append_to_logs(db_alias, log_entry)

            for db_alias, logs in LOGS.items():
                print(f"Logs for {db_alias}: {logs}")

            return JsonResponse({'message': 'Write operation successful'}, status=status.HTTP_201_CREATED)
            
        else:
            # insert_query = "INSERT INTO your_table (column1, column2, column3) VALUES ('value1', 'value2', 'value3')"
            delete_query = reverse_insert_query(query)
            for db_alias in succes_alias:
                check = query_processing(db_alias,delete_query)

            return Response({'error': 'Write operation failed on more than 50% of databases'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    except OperationalError as e:
        return Response({'error': f'Database connection failed: {e}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)