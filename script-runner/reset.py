import os
import requests

DB_URL = "http://influxdb:8086"
DB_TOKEN = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_TOKEN")
DB_BUCKET = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_BUCKET")

def get_bucket_id():
    headers = {
        "Authorization": f"Token {DB_TOKEN}",
        "Accept": "application/json"
    }
    params = {"name": DB_BUCKET}
    response = requests.get(f"{DB_URL}/api/v2/buckets", headers=headers, params=params)
    response.raise_for_status()
    buckets = response.json().get("buckets", [])
    if not buckets:
        raise Exception(f"Bucket '{DB_BUCKET}' not found")
    return buckets[0]["id"]

def delete_bucket(bucket_id):
    headers = {
        "Authorization": f"Token {DB_TOKEN}",
        "Content-type": "application/json"
    }
    response = requests.delete(f"{DB_URL}/api/v2/buckets/{bucket_id}", headers=headers)
    response.raise_for_status()

if __name__ == "__main__":
    print(f"Delete bucket {DB_BUCKET}")
    bucket_id = get_bucket_id()
    delete_bucket(bucket_id)