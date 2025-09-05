import csv
import os
import sys
import datetime
import time
import requests

DB_URL = "http://influxdb:8086"
DB_ORG = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_ORG")
DB_BUCKET = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_BUCKET")
DB_TOKEN = os.getenv("DOCKER_SCRIPTRUNNER_INFLUXDB_TOKEN")

def process_csv(csv_file):
    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        prev_timestamp = None
        timestamp_increment = 1
        for i, row in enumerate(reader):
            if i % 100 == 0 and i != 0:
                print("pause...")
                time.sleep(1)

            timestamp_str = row[0]
            dt = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            timestamp = int(time.mktime(dt.timetuple()) * 1000)
            if timestamp == prev_timestamp:
                print(f"Duplicate timestamp detected, incrementing by {timestamp_increment} ms")
                timestamp += timestamp_increment
                timestamp_increment += 1
            else:
                prev_timestamp = timestamp
                timestamp_increment = 1

            buildId = row[1]
            project = row[3] if row[3] else "unknown"
            project = project.replace(" ", "-").replace("/", "-").replace("\\", "-").replace(",", "-")
            # before August format
            # isCi = str(row[5] == "CI Build")
            # durationMs = row[6]
            # cacheAvoidanceSavingsMs = row[7]
            # if len(row) > 12:
            #     testDistributionSavingsMs = row[12]
            # else:
            #     testDistributionSavingsMs = 0
            # August format
            isCi = str(row[6] == "CI Build")
            durationMs = row[7]
            cacheAvoidanceSavingsMs = row[8]
            if len(row) > 13:
                testDistributionSavingsMs = row[13]
            else:
                testDistributionSavingsMs = 0

            print(f"Adding record: {project} / {buildId} / {isCi} / {durationMs} / {cacheAvoidanceSavingsMs} / {testDistributionSavingsMs} {timestamp_str}")
            data = f"build,project={project} build_id=0,is_ci={isCi},duration_ms={durationMs},cache_avoidance_savings_ms={cacheAvoidanceSavingsMs},test_distribution_savings_ms={testDistributionSavingsMs} {timestamp}"
            headers = {"Authorization": f"Token {DB_TOKEN}"}
            params = {
                "org": DB_ORG,
                "db": DB_BUCKET,
                "precision": "ms"
            }
            response = requests.post(f"{DB_URL}/write", params=params, headers=headers, data=data)
            if response.status_code == 404:
                print(f"Adding record failed [{response.status_code}]")
                print(f"Bucket is probably missing, run init.py to create it")
                sys.exit(1)
                break
            if response.status_code != 204:
                print(f"Adding record failed [{response.status_code}]")
                sys.exit(1)
                break


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('USAGE: add-combined-data.py <PATH_TO_CSV>')
        sys.exit(1)
    process_csv(sys.argv[1])