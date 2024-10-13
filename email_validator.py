import re
import dns.resolver
import smtplib
import requests
from disposable_email_domains import blocklist
import hashlib
import csv
import json
from tqdm import tqdm
import concurrent.futures
import threading
from datetime import datetime, timedelta
import socket
import ssl
import whois
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ImprovedEmailVerifier:
    def __init__(self):
        self.mx_cache = {}
        self.free_providers = set(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
        self.role_accounts = set(['admin', 'info', 'support', 'sales', 'contact', 'webmaster'])

    def verify_email(self, email):
        results = {
            "email": email,
            "is_valid": True,
            "checks": {}
        }

        if not self._check_syntax(email):
            results["is_valid"] = False
            results["checks"]["syntax"] = {"passed": False, "message": "Invalid email syntax"}
            return results

        local_part, domain = email.split('@')

        results["checks"]["domain_length"] = self._check_domain_length(domain)
        if not results["checks"]["domain_length"]["passed"]:
            results["is_valid"] = False
            return results

        results["checks"]["disposable"] = {"passed": not self._is_disposable(domain), "message": "Disposable email domain" if self._is_disposable(domain) else "Not a disposable domain"}
        if not results["checks"]["disposable"]["passed"]:
            results["is_valid"] = False
            return results

        results["checks"]["free_provider"] = {"passed": True, "message": "Free email provider" if self._is_free_provider(domain) else "Not a free provider"}
        results["checks"]["role_account"] = {"passed": True, "message": "Role account" if self._is_role_account(local_part) else "Not a role account"}

        results["checks"]["dns_records"] = self._check_dns_records(domain)
        if not results["checks"]["dns_records"]["passed"]:
            results["is_valid"] = False
            return results

        results["checks"]["mx_records"] = self._check_mx_records(domain)
        if not results["checks"]["mx_records"]["passed"]:
            results["is_valid"] = False
            return results


        results["checks"]["gravatar"] = {"passed": True, "message": "Gravatar found" if self._has_gravatar(email) else "No Gravatar found"}

        suggested_email = self._typo_check(email)
        results["checks"]["typo"] = {"passed": suggested_email == email, "message": f"Suggested: {suggested_email}" if suggested_email != email else "No typo detected"}



        results["checks"]["domain_age"] = self._check_domain_age(domain)
        results["checks"]["dmarc"] = self._check_dmarc(domain)
        results["checks"]["spf"] = self._check_spf(domain)

        return results

    def _check_syntax(self, email):
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None

    def _check_domain_length(self, domain):
        is_valid = 3 <= len(domain) <= 255
        return {"passed": is_valid, "message": "Domain length valid" if is_valid else "Domain length invalid"}

    def _is_disposable(self, domain):
        return domain.lower() in blocklist

    def _is_free_provider(self, domain):
        return domain.lower() in self.free_providers

    def _is_role_account(self, local_part):
        return local_part.lower() in self.role_accounts

    def _check_dns_records(self, domain):
        try:
            dns.resolver.resolve(domain, 'A')
            return {"passed": True, "message": "Valid DNS records"}
        except:
            return {"passed": False, "message": "Invalid DNS records"}

    def _check_mx_records(self, domain):
        if domain in self.mx_cache:
            return {"passed": self.mx_cache[domain], "message": "Valid MX records" if self.mx_cache[domain] else "No valid MX records found"}

        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            self.mx_cache[domain] = len(mx_records) > 0
            return {"passed": self.mx_cache[domain], "message": "Valid MX records" if self.mx_cache[domain] else "No valid MX records found"}
        except:
            self.mx_cache[domain] = False
            return {"passed": False, "message": "No valid MX records found"}




    def _has_gravatar(self, email):
        hash = hashlib.md5(email.lower().encode()).hexdigest()
        url = f"https://www.gravatar.com/avatar/{hash}?d=404"
        response = requests.get(url)
        return response.status_code == 200

    def _typo_check(self, email):
        # This is a very basic typo check. A more comprehensive solution would use a library like 'pyspellchecker'
        common_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
        local_part, domain = email.split('@')
        for common_domain in common_domains:
            if domain.lower() != common_domain and len(domain) >= len(common_domain):
                if domain.lower()[:len(common_domain)] == common_domain[:len(domain)]:
                    return f"{local_part}@{common_domain}"
        return email


    def _check_domain_age(self, domain):
        try:
            domain_info = whois.whois(domain)
            creation_date = domain_info.creation_date
            if isinstance(creation_date, list):
                creation_date = min(creation_date)
            
            age = datetime.now() - creation_date
            is_old_enough = age > timedelta(days=180)  # Consider domains older than 6 months as valid
            
            return {"passed": is_old_enough, "message": f"Domain age: {age.days} days"}
        except:
            return {"passed": False, "message": "Unable to determine domain age"}

    def _check_dmarc(self, domain):
        try:
            dmarc_records = dns.resolver.resolve(f"_dmarc.{domain}", 'TXT')
            for record in dmarc_records:
                if 'v=DMARC1' in str(record):
                    return {"passed": True, "message": "DMARC record found"}
            return {"passed": False, "message": "No DMARC record found"}
        except:
            return {"passed": False, "message": "Unable to check DMARC record"}

    def _check_spf(self, domain):
        try:
            spf_records = dns.resolver.resolve(domain, 'TXT')
            for record in spf_records:
                if 'v=spf1' in str(record):
                    return {"passed": True, "message": "SPF record found"}
            return {"passed": False, "message": "No SPF record found"}
        except:
            return {"passed": False, "message": "Unable to check SPF record"}
        


def verify_email(email, smtp_verify=False):
    """Run all verification steps."""
    verifier = ImprovedEmailVerifier()
    result = verifier.verify_email(email)
    
    if smtp_verify and result['is_valid']:
        smtp_result = verify_smtp(email)
        result['checks']['smtp'] = {"passed": smtp_result, "message": "SMTP verification passed" if smtp_result else "SMTP verification failed"}
        result['is_valid'] = result['is_valid'] and smtp_result
    
    return result['is_valid']

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

def verify_emails_from_csv(file_path, output_file_path, smtp_verify=False):
    """Read emails from a CSV file, verify each concurrently, and write results to a new CSV file."""
    results = []
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        emails = [row[0] for row in reader]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_email = {executor.submit(verify_email, email, smtp_verify): email for email in emails}
        for future in tqdm(concurrent.futures.as_completed(future_to_email), total=len(emails), desc="Verifying Emails"):
            email = future_to_email[future]
            result = future.result()
            status = "Valid" if result else "Invalid"
            results.append((email, status))

    with open(output_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Email", "Status"])
        writer.writerows(results)

# Example usage
# input_csv_file_path = "emails4.csv"
# output_csv_file_path = "verified_emails.csv"
# verify_emails_from_csv(input_csv_file_path, output_csv_file_path, smtp_verify=False)