# develocity-savings-monitor

_Grafana_ / _InfluxDB_ based solution to monitor Develocity savings.

In the background, _docker-compose_ is used to orchestrate the different components.
A third container (_runner_) is additionally spawned to ease the data population / deletion process.

# Usage

- Initialize the system (once):
```bash
docker compose up -d
docker exec runner python3 /home/runner/init.py
```

- Populate data into the system (with a csv export from the Realized build cache savings DRV dashboard):

```bash
docker exec runner python3 /home/runner/add.py <PATH_TO_CSV>
```

- Delete all data from the system (delete InfluxDB bucket):
```bash
docker exec runner python3 /home/runner/reset.py
```

- Turn off the system:
```bash
docker compose down
```

# Rendering

open the Grafana dashboard on http://localhost:3000/
