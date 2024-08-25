import os
import shutil
import logging
from pathlib import Path

#Enable logging
logging.basicConfig(
    filename='file_processing.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)


def validate_file(file_path, record_length, field_lengths):
    #Validate that the fixed length ASCII flat file and ensure that it meets requirements.
    path = Path(file_path)

    #Check if file exists and a valid file
    if not path.exists() or not path.is_file():
        logging.error(f"File '{file_path}' does not exist or is not a valid file.")
        return False
    
    #check each line to validate that it meets the specified record length and field lengths
    with path.open('r') as file:
        for line_number, line in enumerate(file, start=1):
            line = line.rstrip('\n') #This is to remove newline characters

            if len(line) !=record_length:
                logging.error(f"Line {line_number} in '{file_path}' does not match the expected record length of {record_length}. Found length: {len(line)}")
                return False
            
            start_index = 0
            for field_index, field_length in enumerate(field_lengths):
                field = line[start_index:start_index + field_length]
                if len(field) != field_length:
                    logging.error(f"Field {field_index +1} in line {line_number} does not match the expected lenth of {field_length}. found length: {len(field)}.")
                    return False
                start_index += field_length

    logging.info(f"File '{file_path} passed all the validation checks.")
    return True


    
def process_file(input_file_path, output_file_path, record_length, field_lengths):
    #Process a fixed-length ASCII flat file and write the proceessed content to an output file. 

    input_path = Path(input_file_path)
    output_path = Path(output_file_path)

    #File is validated before the procesing begins
    if not validate_file(input_file_path, record_length, field_lengths):
        logging.error(f"Validation failed for file '{input_file_path}'. Processing stopped.")
        return
    
    try:
        #Reading the input file in chunks for scalability
        with input_path.open('r') as infile, output_path.open('w') as outfile:
            for line in infile:
                line = line.rstrip('\n')
                processed_fields = []

                start_index = 0
                for field_length in field_lengths:
                    field = line[start_index:start_index + field_length]
                    processed_field = field.strip().upper() #This represents the example of what happens to each record when processing. This is where we need to call the function that either stages the records in a staging DB or directly calls the LLM
                    processed_fields.append(processed_field)
                    start_index += field_length

                processed_line = ''.join(processed_fields)
                outfile.write(processed_line + '\n')

        logging.info(f"successfully processed file '{input_file_path}'.")

    except Exception as e:
        logging.error(f"Error processing file '{input_file_path}':{e}")
        return
    

def safe_file_copy(src_path, dest_path): 
    try:
        shutil.copy2(src_path, dest_path)
        logging.info(f"Successfully copied '{src_path}' to '{dest_path}'.")
    except FileNotFoundError:
        logging.error(f"source file '{src_path}' not found.")
    except PermissionError:
        logging.error(f"Permission denied when copying '{src_path}' to '{dest_path}'.")
    except Exception as e:
        logging.error(f"Unexpected error when copying '{src_path}' to '{dest_path}'")
    
def file_handle_main():
    input_file_path = '/Users/seshn/dev/filehandling/input/sample.txt'
    output_file_path = '/Users/seshn/dev/filehandling/output/outputsample.txt'

    safe_file_copy(input_file_path, '/Users/seshn/dev/filehandling/backup/backupsample.txt')

    record_length = 10
    field_lengths = [2,3,3,2]

    process_file(input_file_path, output_file_path, record_length, field_lengths)

if __name__=="__main__":
    file_handle_main()



