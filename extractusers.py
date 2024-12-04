import csv
import os
import re
from email_validator import verify_email
from dns.exception import Timeout as DNSTimeout
from tqdm import tqdm
import time
import concurrent.futures
import threading
import itertools

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
verify_emails = True

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

def is_valid_name(name):
    # Check if the name contains only letters, spaces, and common punctuation
    return bool(re.match(r'^[a-zA-Z\s\'\-\.]+$', name))

# New function to process a batch of rows
def process_batch(batch, writer, processed_emails, lock):
    local_processed = set()
    local_results = []

    for row in batch:
        ig_handle = row['username']
        email = row['public_email']
        
        if email and email not in processed_emails and email not in local_processed:
            full_name = row['full_name']
            
            # Use the username if the full name is invalid or empty
            if full_name and is_valid_name(full_name):
                first_name = full_name.split()[0]
            else:
                first_name = ig_handle
            
            first_name = remove_emojis(first_name)
            
            if process_email(email, ig_handle, first_name):
                local_results.append({
                    'IG Handle': ig_handle,
                    'E-Mail': email,
                    'First Name': first_name
                })
                local_processed.add(email)
        elif email in processed_emails or email in local_processed:
            print(f"\nSkipping already processed email: {email}")
        else:
            print(f"\nSkipping row for {ig_handle} (no email provided)")

    with lock:
        writer.writerows(local_results)
        processed_emails.update(local_processed)

# Get user input for the input CSV file
input_filename = input("Enter the name of the input CSV file (without .csv extension): ")
input_file = os.path.join('exported_data', f'{input_filename}.csv')

# Create the output filename and path
output_filename = f'{input_filename}_extracted.csv'
output_file = os.path.join('extracted_data', output_filename)

# Ensure the extracted_data directory existsbu
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
    
    # Create a lock for thread-safe writing
    lock = threading.Lock()
    
    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Process rows in batches
        batch_size = 100
        futures = []
        
        for i in range(0, total_rows, batch_size):
            batch = list(itertools.islice(reader, batch_size))
            if not batch:
                break
            future = executor.submit(process_batch, batch, writer, processed_emails, lock)
            futures.append(future)
        
        # Use tqdm to show progress
        for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing batches"):
            pass

print(f"\nData extraction and validation complete. Valid emails saved to {output_file}")
