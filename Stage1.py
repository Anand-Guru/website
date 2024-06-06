import boto3
import requests
import os
import pandas as pd
from decimal import Decimal
import json

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = "bucket-to-keep-csv"
REGION= "us-west-2"

url = "https://cwwp2.dot.ca.gov/data/d1/cctv/cctvStatusD01.csv" #URL for the CSV file
path = "/home/ubuntu/" 

s3_uri="s3://bucket-to-keep-csv/raw_images/" # URI of the Folder in the S3 bucket 

#spliting the URI to get the Prefix

arr=s3_uri.split('/')
bucket =arr[2]
prefix=""
for i in range(3,len(arr)-1):
    prefix=prefix+arr[i]+"/"

#AWS credential function
def aws_credential(resource, region=False):
    if region and resource == 'dynamodb':
        s3_client = boto3.resource(resource, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=region)    
    else:
        s3_client = boto3.client(resource, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    return s3_client

#AWS bucket function

def S3_path(csv_file,S3_BUCKET_NAME,folder_name):
    aws_cred = aws_credential(resource='s3')
    aws_cred.upload_file(csv_file, S3_BUCKET_NAME, f"{folder_name}/{csv_file}") #path to upload the CSV file

#Download the image from URL and upload to S3
def download_and_upload(url, file_name, S3_BUCKET_NAME):
    try:

        with requests.Session() as session:
            response = session.get(url)
            content = response.content.decode()
        with open(f'{path}StatusD01.csv', 'w') as f:
            f.write(content)

        if response.status_code == 200:
            aws_credential(resource='s3')
            S3_path(file_name,S3_BUCKET_NAME,folder_name='original_csv')
            print(f"Downloaded {file_name} from {url} and uploaded to S3 bucket {S3_BUCKET_NAME}")
            return response
        else:
            print(f"Error downloading {file_name}: {response.status_code}")
    except Exception as e:
        raise Exception(f"Error downloading and uploading {file_name}: {e}")
    

# Uploading the Images to S3 Bucket from the URLs in CSV


def change_image_name_and_upload():
   
    data_frame = pd.read_csv(f'{path}StatusD01.csv')
    aws_credential(resource='s3')
    for data in data_frame.iterrows():
        data = data[1]
        img_name = f"Img-{data['recordDate']}-{data['recordTime']}-{data['recordEpoch']}-{data['district']}.jpg"
        response = requests.get(data['currentImageURL'], stream=True)
        
        aws_credential(resource='s3').upload_fileobj(response.raw, 'bucket-to-keep-csv', f"raw_images/{img_name}")
    print('image upload completed')

#Adding colums to csv file
def list_s3_files_using_client(bucket, prefix):
    s3_client=aws_credential(resource="s3")
    response = s3_client.list_objects_v2(Bucket=bucket,  Prefix=prefix) # Featch Meta-data of all the files in the folder


    file = open(f"{path}StatusD01.csv", "r")
    data_frame = pd.read_csv(f"{path}StatusD01.csv")
    files = response.get("Contents")
    url_list = []
    for file in files[1:]: # Iterate through each files
        file_path=file['Key']
        object_url="https://"+bucket+".s3.amazonaws.com/"+file_path #create Object URL  Manually
        image_url = int(object_url.split("-")[-2])
         
        for row in data_frame.iterrows():
            row = dict(row[1])
            if image_url == row['recordEpoch']:
                row['rawimageURL'] = object_url #adding the image URL
                row['PredictedImageURL'] = ""
                row['Pothole'] = "false"
                row['Construction'] = "false"
                row['Debris'] = "false"
                row['FlashFlood'] = "false"
                row['Fog'] = "false"
                row['RoadKill'] = "false"

                url_list.append(row)
                break

    #creating new modified file
    df = pd.DataFrame(url_list)
    df.to_csv('StatusD01-modified.csv', index=False)
    print("CSV file modification done")

#Uploading the Modified CSV file to S3 BUCKET 

def upload_new_csv(S3_BUCKET_NAME,csv_file):
    aws_credential(resource='s3')
    S3_path(csv_file,S3_BUCKET_NAME,folder_name='modified_csv')
    print("csv file upload done") # Which file is uploaded to where ?

#function for inserting the data to dynamodb
    
def insert_dynamo_item(tablename,item_lst):
    dynamoTable = dynamodb.Table(tablename)
    
    for record in item_lst:
        item = json.loads(json.dumps(record), parse_float=Decimal)
        dynamoTable.put_item(Item=item)
    print("data uploaded to dynamodb")


# Remove the file from Local  
        
def delete_files():
    os.remove (f'{path}StatusD01.csv')
    os.remove (f'{path}StatusD01-modified.csv')
    print('CSV file deleted from local')

 
if __name__ == "__main__":
    download_and_upload(url, "StatusD01.csv", S3_BUCKET_NAME)
    change_image_name_and_upload()
    list_s3_files_using_client(bucket=bucket, prefix = prefix)
    upload_new_csv(S3_BUCKET_NAME,'StatusD01-modified.csv')

    #converting the CSV file to json format
    data_json = json.loads(pd.read_csv(f'{path}StatusD01-modified.csv').to_json(orient='records'))

    #Creating a list of Dictionaries and their table name.
    lst_Dicts = [{'item': data_json, 'table':'Landing.ai'}]

    #Connect to DynamoDb Function
    dynamodb = aws_credential('dynamodb', region=REGION)
    # dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=REGION)

    #Upload Content to DynamoDB
    
    insert_dynamo_item(tablename='Landing.ai', item_lst=data_json)

    delete_files()    
