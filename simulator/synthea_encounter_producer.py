import csv
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, Iterable
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

# CONFIGURATION

# Load .env values
load_dotenv()

# 1) Event Hubs (Kafka) config 
EVENTHUBS_NAMESPACE = os.getenv("EVENTHUBS_NAMESPACE")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")

# 2) Local data folder config
#    Folder where your 6 CSVs currently live
DATA_FOLDER = r"./data/synthea_data"

PATIENTS_FILE = f"{DATA_FOLDER}/patients.csv"
ENCOUNTERS_FILE = f"{DATA_FOLDER}/encounters.csv"

# 3) CSV delimiter
CSV_DELIMITER = ","

# 4) Streaming config
SLEEP_SECONDS_BETWEEN_EVENTS = 1  # how fast to send events


# HELPER FUNCTIONS

def parse_birthdate(raw: str):
    """
    Try a few date formats for patient BIRTHDATE.
    Example from your sample: '2/17/2019'
    """
    if not raw:
        return None

    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def parse_encounter_start(raw: str):
    """
    Encounter START in Synthea is usually ISO-like: '2019-02-17T05:07:38Z'
    """
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def compute_age_at_encounter(birth_dt, encounter_dt) -> int:
    if not birth_dt or not encounter_dt:
        return None
    # rough age in years
    years = encounter_dt.year - birth_dt.year
    # adjust if birthday not yet reached in that year
    if (encounter_dt.month, encounter_dt.day) < (birth_dt.month, birth_dt.day):
        years -= 1
    return max(years, 0)


def inject_dirty_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    occasionally inject "dirty" data so we can clean it
    in the Silver layer (age > 100, future admission time, etc.).
    """
    # Example: 5% chance to mess up age
    import random

    rand = random.random()
    if rand < 0.05:
        # unrealistic age
        event["age"] = 150
    elif rand < 0.10:
        # future admission time
        event["admission_time"] = (datetime.now(timezone.utc).replace(microsecond=0) \
                                   .isoformat() + "Z")

    return event



# DATA LOADING

def load_patients(path: str) -> Dict[str, Dict[str, Any]]:
    """
    Load patients.csv into a dict keyed by Id.
    Columns from your sample:
    Id, BIRTHDATE, DEATHDATE, SSN, ..., GENDER, ...
    """
    patients = {}
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        print("Patients fieldnames:", reader.fieldnames)  # DEBUG
        for row in reader:
            pid = row.get("Id")
            if not pid:
                continue
            patients[pid] = {
                "birthdate_raw": row.get("BIRTHDATE", ""),
                "gender": row.get("GENDER", ""),
                "race": row.get("RACE", ""),
                "ethnicity": row.get("ETHNICITY", ""),
                "city": row.get("CITY", ""),
                "state": row.get("STATE", ""),
                "zip": row.get("ZIP", ""),
            }
    return patients


def load_encounters(path: str) -> Iterable[Dict[str, Any]]:
    """
    Load encounters.csv as a list of dictionaries.
    Columns from your sample:
    Id, START, STOP, PATIENT, ORGANIZATION, PROVIDER, PAYER, ENCOUNTERCLASS, ...
    """
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for row in reader:
            yield row


# KAFKA PRODUCER SETUP

def create_producer() -> KafkaProducer:
    """
    Create a KafkaProducer configured for Azure Event Hubs Kafka endpoint.
    """
    producer = KafkaProducer(
        bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=CONNECTION_STRING,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    return producer


# EVENT GENERATION

def encounter_events(patients_map: Dict[str, Dict[str, Any]],
                     encounters_iter: Iterable[Dict[str, Any]]):
    """
    Generator that iterates over encounters and yields event dicts ready for sending.
    After reaching the end, it loops again (infinite generator).
    """
    encounters_list = list(encounters_iter)
    if not encounters_list:
        raise RuntimeError("No encounters found in encounters.csv")

    idx = 0
    n = len(encounters_list)

    while True:
        enc = encounters_list[idx]
        idx = (idx + 1) % n  # loop back

        patient_id = enc.get("PATIENT")
        patient = patients_map.get(patient_id, {})

        birth_dt = parse_birthdate(patient.get("birthdate_raw", ""))
        start_dt = parse_encounter_start(enc.get("START", ""))

        age = compute_age_at_encounter(birth_dt, start_dt)

        # Build event payload
        event = {
            "encounter_id": enc.get("Id"),
            "patient_id": patient_id,
            "gender": patient.get("gender"),
            "age": age,
            "department": enc.get("ENCOUNTERCLASS"),
            "admission_time": enc.get("START"),
            "discharge_time": enc.get("STOP"),
            "organization_id": enc.get("ORGANIZATION"),
            "provider_id": enc.get("PROVIDER"),
            "payer_id": enc.get("PAYER"),
            "base_encounter_cost": safe_float(enc.get("BASE_ENCOUNTER_COST")),
            "total_claim_cost": safe_float(enc.get("TOTAL_CLAIM_COST")),
            "payer_coverage": safe_float(enc.get("PAYER_COVERAGE")),
        }

        yield inject_dirty_data(event)


def safe_float(value: Any):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


# MAIN

if __name__ == "__main__":
    print("Loading patients and encounters from Synthea CSVs...")
    patients_map = load_patients(PATIENTS_FILE)
    encounters_iter = load_encounters(ENCOUNTERS_FILE)
    events = encounter_events(patients_map, encounters_iter)

    print(f"Loaded {len(patients_map)} patients.")
    producer = create_producer()
    print("Kafka producer created. Starting to send events to Event Hubs...")

    try:
        max_events = 1000   # 🔹 how many events to send
        count = 0

        for event in events:
            if count >= max_events:
                break

            producer.send(EVENT_HUB_NAME, event)
            count += 1

            # Print only first 10 and then every 100th event
            if count <= 10 or count % 100 == 0:
                print(f"[{count}/{max_events}] Sent to Event Hub: {event}")

            time.sleep(SLEEP_SECONDS_BETWEEN_EVENTS)

        print(f"Finished sending {max_events} events. Exiting.")
    except KeyboardInterrupt:
        print("Stopping producer (Keyboard Interrupt).")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")
