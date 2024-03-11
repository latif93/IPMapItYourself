import json
import os

def split_json_lines(file_path, lines_per_file=100, prefix='final_processed_batch', output_folder='final_processed'):
    # Ensure the output directory exists
    os.makedirs(output_folder, exist_ok=True)
    
    with open(file_path, 'r') as file:
        lines = file.readlines()

    total_files = len(lines) // lines_per_file + (1 if len(lines) % lines_per_file > 0 else 0)

    for i in range(total_files):
        # Construct the output file path within the specified folder
        output_file_path = os.path.join(output_folder, f'{prefix}{i + 1:02}.json')
        with open(output_file_path, 'w') as chunk_file:
            start = i * lines_per_file
            end = start + lines_per_file
            for line in lines[start:end]:
                chunk_file.write(line)

    print(f'Successfully split the data into {total_files} files in the "{output_folder}" folder.')

# Assuming the 'final_processed.json' file is located in the same folder as your script
# Adjust the file path as necessary
input_file_path = 'final_processed\\final_processed.json'
split_json_lines(input_file_path)

