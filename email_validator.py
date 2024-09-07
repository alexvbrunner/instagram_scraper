import re
import dns.resolver
import smtplib
import csv
import tqdm
import concurrent.futures
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_email_syntax(email):
    """Check if the email address has a valid syntax."""
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(pattern, email) is not None

def check_mx_records(domain):
    """Check for MX records of the given domain."""
    try:
        mx_records = dns.resolver.resolve(domain, 'MX', lifetime=10)
        return any(mx_records)
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers) as e:
        logging.warning(f"MX record error for {domain}: {str(e)}")
        return False

def verify_smtp(email):
    """Verify if the email exists using SMTP protocol."""
    domain = email.split('@')[1]
    try:
        mx_record = dns.resolver.resolve(domain, 'MX', lifetime=10)[0].exchange.to_text()
        server = smtplib.SMTP(timeout=10)
        server.set_debuglevel(0)
        server.connect(mx_record)
        server.helo(server.local_hostname)
        server.mail('alex.brunner20@gmail.com')
        code, message = server.rcpt(email)
        server.quit()
        if code == 250:
            return True
        else:
            logging.warning(f"SMTP verification failed for {email}: {code} {message}")
            return False
    except Exception as e:
        logging.error(f"SMTP error for {email}: {str(e)}")
        return False

def verify_email(email, smtp_verify=False):
    """Run all verification steps."""
    if check_email_syntax(email):
        domain = email.split('@')[1]
        if check_mx_records(domain):
            if smtp_verify:
                return verify_smtp(email)
            else:
                return True  # Consider valid if syntax and MX records are okay
        else:
            logging.warning(f"No valid MX records found for {domain}")
    else:
        logging.warning(f"Invalid email syntax: {email}")
    return False

def verify_emails_from_csv(file_path, output_file_path, smtp_verify=False):
    """Read emails from a CSV file, verify each concurrently, and write results to a new CSV file."""
    results = []
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        emails = [row[0] for row in reader]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_email = {executor.submit(verify_email, email, smtp_verify): email for email in emails}
        for future in tqdm.tqdm(concurrent.futures.as_completed(future_to_email), desc="Verifying Emails"):
            email = future_to_email[future]
            result = future.result()
            status = "Valid" if result else "Invalid"
            results.append((email, status))

    with open(output_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Email", "Status"])
        writer.writerows(results)

# # Example usage
# input_csv_file_path = "emails4.csv"
# output_csv_file_path = "verified_emails.csv"
# verify_emails_from_csv(input_csv_file_path, output_csv_file_path, smtp_verify=False)
