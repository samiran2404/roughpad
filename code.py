import boto3
import pandas

class ETL:
    def __init__(self):
        self.maximum_time_to_process = 800 # This value is in seconds
        self.start_time = current_time


    def process(self, bucket_name, S3_key, offset):
        client = boto3.client('s3')
        file = client.get_object(bucket, s3_key) # This is the streaming object creation
        current_offset = 0
        total_row_count = len(file) # This sets the total number of rows to process in the input CSV file.
        with pandas.read_csv(file, chunk_size=5000, skiprows=offset) as reader:# offset is 0 initially so file is processed from the beginning
            for chunk in reader: # Chunk processing started
                # The processing happens here
                current_offset += len(chunk)
                headers = str(csv_file.columns)
                execution_time = current_time - self.start_time # In seconds
                if execution_time > self.maximum_time_to_process:
                    break # This condition breaks the for loop of processing if lambda has neared its ending time.

        if current_offset < total_row_count # This checks if the file has been completed processing or it has ended due to continuity			
            # This condition is to pass on the current state to next lambda fucntion
            new_offset = offset + current_offset  # This line sets the new offset to be passed to next lambda fucntion
            response = boto3.client('lambda').invoke("lambda_fucntion", mode='asynchronous', event = {    
                                                                                                        "bucket_name": "s3_files_bucket",
                                                                                                        "s3_key": "s3_file_key",
                                                                                                        "offset": new_offset,
                                                                                                        "headers": headers
                                                                                                    })
            return {"status": "CONTINUED"}
        else:
            print("File processed successfully")
            return {"status": "SUCCESS"}
