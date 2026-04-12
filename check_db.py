import sqlite3

def validate_database_content():
    """
    Connects to the SQLite database and retrieves a sample of records 
    to verify the integrity of the data pipeline.
    """
    database_path = 'mercados_tech_full.db'
    
    try:
        # Establish connection to the local SQLite database
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()

        # Query to retrieve the first 5 records from the markets table
        # Selecting key fields to verify successful ingestion and transformation
        query = "SELECT market_id, question, price_yes FROM markets LIMIT 5"
        cursor.execute(query)
        rows = cursor.fetchall()

        # Define table headers for the console output
        header_id = "ID"
        header_question = "QUESTION"
        header_price = "PRICE YES"
        
        print(f"DATABASE VERIFICATION: {database_path}")
        print(f"{header_id:<10} | {header_question:<40} | {header_price}")
        print("-" * 70)

        # Iterate through result set and apply basic string formatting
        for row in rows:
            market_id = row[0]
            # Truncate long questions to maintain table alignment
            question_snippet = row[1][:38] if row[1] else "N/A"
            price_yes = row[2]
            
            print(f"{market_id:<10} | {question_snippet:<40} | {price_yes}")

        # Resource cleanup
        connection.close()
        print("-" * 70)
        print("Database validation completed successfully.")

    except sqlite3.OperationalError as e:
        print(f"Database error: {e}. Ensure the consumer has created the database file.")
    except Exception as e:
        print(f"An unexpected error occurred during database validation: {e}")

if __name__ == "__main__":
    validate_database_content()