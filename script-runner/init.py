import os
import requests

DB_URL = "http://influxdb:8086"
DB_ORG = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_ORG")
DB_BUCKET = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_BUCKET")
DB_TOKEN = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_TOKEN")
DB_RETENTION = int(os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_RETENTION_IN_SECONDS", "0"))

def get_org_id():
    headers = {
        "Authorization": f"Token {DB_TOKEN}",
        "Accept": "application/json"
    }
    response = requests.get(f"{DB_URL}/api/v2/orgs", headers=headers)
    response.raise_for_status()
    orgs = response.json().get("orgs", [])
    if not orgs:
        raise Exception("No organizations found")
    return orgs[0]["id"]

def exit_if_bucket_exists():
    headers = {
        "Authorization": f"Token {DB_TOKEN}",
        "Content-type": "application/json"
    }
    response = requests.get(f"{DB_URL}/api/v2/buckets", headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to check if bucket exists: {response.text}")
    else:
        buckets = response.json().get("buckets", [])
        if any(b["name"] == DB_BUCKET for b in buckets):
            print(f"Bucket '{DB_BUCKET}' already exists")
            exit(0)
        else:
            print(f"Bucket '{DB_BUCKET}' does not exist. Proceeding to create it")

def create_influxdb_bucket(org_id):
    headers = {
        "Authorization": f"Token {DB_TOKEN}",
        "Content-type": "application/json"
    }
    data = {
        "orgID": org_id,
        "name": DB_BUCKET,
        "retentionRules": [
            {
                "type": "expire",
                "everySeconds": DB_RETENTION,
                "shardGroupDurationSeconds": 0
            }
        ]
    }
    response = requests.post(f"{DB_URL}/api/v2/buckets", headers=headers, json=data)
    response.raise_for_status()

if __name__ == "__main__":
    org_id = get_org_id()
    exit_if_bucket_exists()
    create_influxdb_bucket(org_id)