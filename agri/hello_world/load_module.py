import io
import json
import datetime

import boto3

class LoadModule:
    def __init__(self):
        credentials = open("credentials.json", "r")
        credentials = json.load(credentials)
        self.__s3_client = boto3.resource("s3")
        self.__bucket_name = credentials["S3_BUCKET_NAME"]
        self.__trans_folder = credentials["TRANS_FOLDER"]

    def write_to_s3(self, df, name):
        destination = self.__trans_folder + \
            str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')) + '_' + name
        textStream = io.StringIO()
        df.to_csv(textStream)
        bucket = self.__s3_client.Bucket(self.__bucket_name)
        
        try:
            bucket.put_object(Key=destination, Body=textStream.getvalue())
            print("Success in loading")
        except Exception as e:
            print("Error in loading")
            print(e)
            return 0

