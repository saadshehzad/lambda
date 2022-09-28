import json
from io import BytesIO
import pandas as pd
import boto3


class ExtractionModul:
    def __init__(self):
        credentials = open('credentials.json', 'r')
        credentials = json.load(credentials)
        self.__s3_client = boto3.client("s3")
        self.__bucket_name = credentials["S3_BUCKET_NAME"]
        self.__raw_folder = credentials["RAW_FOLDER"]

    def read_farmer_file(self, farmer_file):
        farmer_file = (
            str(self.__raw_folder) + str(farmer_file)
        )
        try:
            farmer_file_content = self.__s3_client.get_object(
                Bucket=self.__bucket_name, Key=farmer_file
            )["Body"].read()
            df_farm_loc = pd.read_excel(farmer_file_content, sheet_name='Farm_Location')
            df_farm_field = pd.read_excel(farmer_file_content, sheet_name='Farm_Field')
            merged = pd.merge(df_farm_loc, df_farm_field,left_on='id',right_on='farm_no')
            return merged
        except Exception as e:
            print("error in reading farmer file")
            print(e)
            return 0



    def read_trade_file(self, trade_file):
        trade_file = (
            str(self.__raw_folder) + str(trade_file)
        )
        try:
            trade_file_content = self.__s3_client.get_object(
                Bucket=self.__bucket_name, Key=trade_file
            )["Body"].read()
            trade_file_content = BytesIO(trade_file_content)
            trades = pd.read_csv(trade_file_content)
            trades = trades[trades.price_type=='Fixed']
            trades = trades[trades.status=='In Position']
            return trades
        except Exception as e:
            print("error in reading trade file")
            print(e)
            return 0

    def read_tickets_file(self, tickets_file):
        tickets_file = (
            str(self.__raw_folder) + str(tickets_file)
        )
        try:
            tickets_file_content = self.__s3_client.get_object(
                Bucket=self.__bucket_name, Key=tickets_file
            )["Body"].read()
            tickets_file_content = BytesIO(tickets_file_content)
            print("Loadin is working")
            return tickets_file_content
        except Exception as e:
            print("error in reading tickets file")
            print(e)
            return 0