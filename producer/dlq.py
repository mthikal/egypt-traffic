import json
import logging
from pathlib import Path
from datetime import datetime

DLQ_FILE = Path("/tmp/traffic_dlq.jsonl")

def write_dlq(source, payload, error):
    record = {
        "ts": datetime.utcnow().isoformat(),
        "source": source,
        "error": str(error),
        "payload": payload,
    }

    with DLQ_FILE.open("a") as f:
        f.write(json.dumps(record) + "\n")

    logging.error(f"DLQ: {record}")
