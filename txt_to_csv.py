import csv



def extract_data_from_txt(input_file):

    output_file = f'{input_file}.csv'

    # Process the input text file and write to a CSV
    with open(f'Scrapes/{input_file}.txt', 'r') as txt_file, open(f'exported_data/{output_file}', 'w', newline='') as csv_file:
        reader = txt_file.readlines()
        writer = csv.writer(csv_file)
        
        # Write the header
        writer.writerow(['username', 'public_email', 'full_name'])
        
        # Process each line and write to the CSV
        for line in reader:
            # Skip empty lines
            if not line.strip():
                continue
            
            # Remove the number at the beginning and split by colon
            parts = line.split(':', 3)
            if len(parts) >= 4:
                ig_handle = parts[1].strip()
                email = parts[3].strip()
                first_name = parts[2].split()[0] if parts[2].strip() else ''
                writer.writerow([ig_handle, email, first_name])
            elif len(parts) == 3:  # Handling cases where Email is missing
                ig_handle = parts[1].strip()
                first_name = parts[2].split()[0] if parts[2].strip() else ''
                writer.writerow([ig_handle, '', first_name])

    print(f"Conversion complete. The CSV file is saved as {output_file}.")



def main():
    input_file = input("Enter the name of the file to convert (needs to be in Scrapes folder, without .txt): ")
    extract_data_from_txt(input_file)

if __name__ == "__main__":
    main()
