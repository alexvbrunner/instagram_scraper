import csv

def extract_public_usernames(input_file, output_file):
    public_usernames = []

    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) >= 5:  # Ensure the row has at least 5 columns
                is_private = row[4].lower()  # 5th column (index 4) is 'is_private'
                if is_private == 'false':
                    if len(row) >= 12:  # Ensure the row has at least 12 columns
                        username = row[11]  # 12th column (index 11) is 'username'
                        public_usernames.append(username)

    with open(output_file, 'w', encoding='utf-8') as txtfile:
        for i, username in enumerate(public_usernames, 1):
            txtfile.write(f"{username}\n")

    print(f"Extracted {len(public_usernames)} public usernames to {output_file}")

if __name__ == "__main__":
    input_file = "followers_list.csv"
    output_file = "usernames.txt"
    extract_public_usernames(input_file, output_file)