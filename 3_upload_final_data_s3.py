import boto3
import os

# AWS S3 Client Setup
s3_client = boto3.client('s3')

#Directory path
file_path = "/home/ec2-user/project/datasets/"

#s3 bucket name
bucket_name = "s3_bucket_name"

#List of matching files from directory
data_files = [file for file in os.listdir(file_path) if file.endswith("_final.csv")]

#Upload to S3
for file in data_files:
    #Construct the full path
    file_full_path = os.path.join(file_path,file)
    
    try:
        #Upload the file to the s3 bucket
        s3_client.upload_file(file_full_path, bucket_name, f"market_data/{file}")
        print(f"File {file} uploaded to {bucket_name}/market_data/{file}")
    except Exception as e:
        print(f"Failed to upload {file}: {str(e)}")

