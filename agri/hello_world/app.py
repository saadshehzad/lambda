import json
import warnings
from extraction_module import ExtractionModul
from load_module import LoadModule
from transformation_module import get_classification_data

def lambda_handler(event, context):
    warnings.filterwarnings("ignore")

    # if type(event["body"]) == str:
    #     event["body"] = json.loads(event["body"])
    read_farmer_file = ExtractionModul().read_farmer_file(
        event["queryStringParameters"]["farmer_file"]
    )
    read_trade_file = ExtractionModul().read_trade_file(
        event["queryStringParameters"]["trade_file"]
    )
    transform = get_classification_data(read_farmer_file, read_trade_file)
    transform.fillna(0, inplace=True)
    LoadModule().write_to_s3(transform, "trades_classification.csv")

    return {
        "statusCode": 201,
        "body": json.dumps(
            {
                "message": "Successfully transformed",
            }
        ),
    }