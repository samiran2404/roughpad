def lambda_handler(event, context):
      bucket_name = event['bucket_name']
      key = event['s3_key']
      offset = event["offset"]
      etl = ETL()
      response = etl.process(bucket_name, key, offset)  # Actual     function where the process happens
      return response
