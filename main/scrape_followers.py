import os
import pandas as pd
import subprocess
import sys

def select_csv_file():
    csv_files = [f for f in os.listdir('Files/') if f.endswith('.csv')]
    if not csv_files:
        print("No CSV files found in the Files/ directory.")
        sys.exit(1)

    print("Available CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"{i}. {file}")

    while True:
        try:
            choice = int(input("Enter the number of the CSV file you want to use: "))
            if 1 <= choice <= len(csv_files):
                return os.path.join('Files/', csv_files[choice - 1])
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def main():
    csv_file = select_csv_file()
    print(f"Selected file: {csv_file}")

    df = pd.read_csv(csv_file)
    user_id_column = 'User ID'

    if user_id_column not in df.columns:
        print(f"Error: '{user_id_column}' column not found in the CSV file.")
        sys.exit(1)

    for index, row in df.iterrows():
        user_id = row[user_id_column]
        print(f"Scraping followers for User ID: {user_id}")
        
        # Run v3_scraper.py with the current user_id and csv_filename
        subprocess.run([sys.executable, "Scrapers/v3_scraper.py", str(user_id), os.path.basename(csv_file)])

        print(f"Finished scraping for User ID: {user_id}")
        print("-" * 50)

if __name__ == "__main__":
    main()