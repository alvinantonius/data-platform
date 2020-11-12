import json
import requests
import boto3    
from datetime import datetime

s3 = boto3.resource('s3')

def main_handler(event, context):
    markets = event['markets']
    bucket_name = event['bucket_name']
    bucket_prefix = event['bucket_prefix']
    date_str = datetime.now().strftime("%Y-%m-%d")
    run_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    for m in markets:
        trade_file_key = f"{bucket_prefix}/trades/market={m}/date={date_str}/{run_datetime}.json"
        trades_data = pull_trades(m)
        save_file(trades_data, bucket_name, trade_file_key)
        depth_file_key = f"{bucket_prefix}/depths/market={m}/date={date_str}/{run_datetime}.json"
        depths_data = pull_depths(m)
        save_file(depths_data, bucket_name, depth_file_key)


def pull_trades(market):
    print(f"pulling trades from market {market}")
    res = requests.get(f"https://indodax.com/api/{market}/trades")
    print("Status Code = ", res.status_code)
    return res.text


def pull_depths(market):
    print(f"pulling depths from market {market}")
    res = requests.get(f"https://indodax.com/api/{market}/depth")
    print("Status Code = ", res.status_code)
    return res.text


def save_file(dumps, bucket_name, file_key):
    s3object = s3.Object(bucket_name, file_key)
    s3object.put(
        Body=(bytes(dumps.encode('UTF-8')))
    )


if __name__ == "__main__":
    main_handler({"markets":[]}, None)