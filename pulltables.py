import mysql.connector
import csv
import os
from mysql.connector import Error

# Database configuration
db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'database': 'main'
}

def connect_to_database():
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
    return None

def get_table_data(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        headers = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
        return headers, rows
    except Error as e:
        print(f"Error fetching data from table {table_name}: {e}")
    finally:
        if cursor:
            cursor.close()

def export_to_csv(table_name, headers, rows):
    output_dir = 'exported_data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    filename = os.path.join(output_dir, f"{table_name}.csv")
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Table {table_name} exported to {filename}")

def main():
    connection = connect_to_database()
    if not connection:
        return

    try:
        # Get list of tables from user
        tables_to_export = input("Enter table names to export (comma-separated): ").split(',')
        tables_to_export = [table.strip() for table in tables_to_export]

        for table in tables_to_export:
            headers, rows = get_table_data(connection, table)
            if headers and rows:
                export_to_csv(table, headers, rows)
            else:
                print(f"No data found for table {table}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()