def extract_usernames(input_file, output_file):
    # Read URLs from input file
    with open(input_file, 'r') as f:
        urls = f.readlines()
    
    # Extract usernames
    usernames = []
    for url in urls:
        # Clean the URL
        url = url.strip()
        if not url:
            continue
            
        # Extract username from URL
        try:
            # Split URL by '/' and get username part
            parts = url.split('/')
            username = parts[3]  # Instagram username is always after the 3rd slash
            
            # Remove query parameters if they exist
            if '?' in username:
                username = username.split('?')[0]
                
            usernames.append(username)
        except:
            print(f"Could not process URL: {url}")
    
    # Write usernames to output file
    with open(output_file, 'w') as f:
        for username in usernames:
            f.write(username + '\n')

    print(f"Processed {len(usernames)} usernames")

# Use the function
input_file = "linksnov13.txt"  # Your input file with URLs
output_file = "usernamesnov13.txt"       # Output file for usernames
extract_usernames(input_file, output_file)