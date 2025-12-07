import random
from datetime import datetime, timedelta
import json

# Number of rows to generate
n_rows = 1500

# Helper functions
def random_device_id():
    return f"AERO-{random.randint(100, 499)}"

def random_error_code():
    return random.choice([None, "ERR_OVERHEAT", "WARN_TEMP"])

def random_status(error_code):
    if error_code == "ERR_OVERHEAT":
        return random.choice(["ERROR", "OK", "WARNING"])
    elif error_code == "WARN_TEMP":
        return random.choice(["WARNING", "OK", "ERROR"])
    else:
        return random.choice(["OK", "WARNING", "ERROR"])

def random_latitude():
    return round(random.uniform(40.68, 40.72), 8)

def random_longitude():
    return round(random.uniform(-74.02, -73.98), 8)

def random_pressure():
    return round(random.uniform(100, 180), 2)

def random_rpm():
    return random.randint(1270, 1870)

def random_temperature():
    return round(random.uniform(65, 120), 2)

def random_vibration():
    return round(random.uniform(1, 12), 2)

# Base timestamps
base_ingest_ts = datetime(2025, 12, 6, 6, 34, 26)
base_timestamp = datetime(2025, 1, 1, 10, 0, 0)

# Generate and save JSON Lines
with open("synthetic_device_data.jsonl", "w") as f:
    for i in range(n_rows):
        error_code = random_error_code()
        row = {
            "battery_level": round(random.uniform(20, 100), 2),
            "device_id": random_device_id(),
            "error_code": error_code,
            "ingest_ts": (base_ingest_ts + timedelta(seconds=0)).isoformat() + "Z",
            "latitude": random_latitude(),
            "longitude": random_longitude(),
            "pressure": random_pressure(),
            "rpm": random_rpm(),
            "status": random_status(error_code),
            "temperature": random_temperature(),
            "timestamp": (base_timestamp + timedelta(seconds=i)).isoformat() + "Z",
            "vibration": random_vibration()
        }
        f.write(json.dumps(row) + "\n")

print("Data saved to synthetic_device_data.jsonl in JSON Lines format")
