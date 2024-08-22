import mysql.connector
import sys

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='main',
            user='root',
            password='password'
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        sys.exit(1)

def get_user_input():
    filename = 'ecom'
    keywords = 'ecommerce, shop, store, business, entrepreneur, shopify, Seven, Figures, dropshipping, brand, brands, marketing, selling, media, software'
    keywords = keywords.split(',')
    keywords = [keyword.strip().lower() for keyword in keywords]
    return filename, keywords

def construct_query(filename, keywords):
    keyword_regex = '|'.join(keywords)
    query = f"""
    SELECT *
    FROM users
    WHERE csv_filename = %s
    AND public_email IS NOT NULL
    AND public_email != ''
    AND LOWER(biography) REGEXP %s
    """
    return query, (filename, keyword_regex)

def execute_query(connection, query, params):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        print(f"Error executing query: {err}")
        return None
    finally:
        if cursor:
            cursor.close()

def main():
    connection = connect_to_database()
    if not connection:
        return

    filename, keywords = get_user_input()
    query, params = construct_query(filename, keywords)
    results = execute_query(connection, query, params)

    if results is not None:
        for row in results:
            print(row)
        print(f"Number of results: {len(results)}")

    connection.close()

if __name__ == "__main__":
    main()