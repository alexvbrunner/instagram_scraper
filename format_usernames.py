import csv

def create_instagram_csv(input_file, output_file):
    with open(input_file, 'r') as f:
        usernames = f.read().splitlines()

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Username', 'Instagram Link'])

        for username in usernames:
            instagram_link = f'https://www.instagram.com/{username}/'
            writer.writerow([username, instagram_link])

    print(f"CSV file '{output_file}' has been created successfully.")

# Usage
input_file = 'input_usernames.txt'
output_file = 'instagram_links.csv'
create_instagram_csv(input_file, output_file)