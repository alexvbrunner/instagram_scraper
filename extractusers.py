import csv
import os
import re
from email_validator import verify_email
from dns.exception import Timeout as DNSTimeout
from tqdm import tqdm
import time

def remove_emojis(text):
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def verify_email_with_retry(email):
    for attempt in range(MAX_RETRIES):
        try:
            return verify_email(email)
        except DNSTimeout:
            if attempt < MAX_RETRIES - 1:
                print(f"DNS timeout while verifying: {email}. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Max retries reached for email: {email}")
                return False
        except Exception as e:
            print(f"Error verifying email {email}: {str(e)}")
            return False
    return False

# Get user input for email verification
verify_emails = False

def process_email(email, ig_handle, first_name):
    if verify_emails:
        print(f"\nVerifying email: {email}")
        if verify_email_with_retry(email):
            print(f"Email valid: {email}")
            return True
        else:
            print(f"Email invalid or verification failed: {email}")
            return False
    else:
        print(f"\nAssuming email is valid: {email}")
        return True

# Get user input for the input CSV file
input_filename = input("Enter the name of the input CSV file (without .csv extension): ")
input_file = os.path.join('exported_data', f'{input_filename}.csv')

# Create the output filename and path
output_filename = f'{input_filename}_extracted.csv'
output_file = os.path.join('extracted_data', output_filename)

# Ensure the extracted_data directory exists
os.makedirs('extracted_data', exist_ok=True)

print(f"Reading from: {input_file}")
print(f"Writing to: {output_file}")

# Set to store processed emails
processed_emails = set()

# If the output file already exists, load processed emails
if os.path.exists(output_file):
    with open(output_file, 'r', encoding='utf-8') as existing_outfile:
        reader = csv.DictReader(existing_outfile)
        processed_emails = set(row['E-Mail'] for row in reader)
    print(f"Loaded {len(processed_emails)} previously processed emails")

# Count total rows in the input file
with open(input_file, 'r', encoding='utf-8') as infile:
    total_rows = sum(1 for row in infile) - 1  # Subtract 1 to account for header

# Open the input CSV file and create/append to the output CSV file
with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'a', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = ['IG Handle', 'E-Mail', 'First Name']
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    
    # Write the header row if the file is new
    if outfile.tell() == 0:
        writer.writeheader()
        
        # Always write the first user
        writer.writerow({
            'IG Handle': 'fil',
            'E-Mail': 'filtrading68@gmail.com',
            'First Name': 'Filip'
        })
        processed_emails.add('filtrading68@gmail.com')

        # Always write the first user
        writer.writerow({
            'IG Handle': 'Tester',
            'E-Mail': '-',
            'First Name': 'Tester'
        })
        processed_emails.add('-')
    
    # Process each row in the input file with a progress bar
    for row in tqdm(reader, total=total_rows, desc="Processing rows"):
        ig_handle = row['username']
        email = row['public_email']
        
        # Only process rows with a non-empty email that hasn't been processed before
        if email and email not in processed_emails:
            full_name = row['full_name']
            
            # Extract first name (assuming it's the first word in full_name)
            # If no full_name, use the username
            first_name = full_name.split()[0] if full_name else ig_handle
            
            # Remove emojis from the first name
            first_name = remove_emojis(first_name)
            
            # Process the email (verify or assume valid)
            if process_email(email, ig_handle, first_name):
                # Write the extracted data to the output file
                writer.writerow({
                    'IG Handle': ig_handle,
                    'E-Mail': email,
                    'First Name': first_name
                })
                processed_emails.add(email)
        elif email in processed_emails:
            print(f"\nSkipping already processed email: {email}")
        else:
            print(f"\nSkipping row for {ig_handle} (no email provided)")

print(f"\nData extraction and validation complete. Valid emails saved to {output_file}")