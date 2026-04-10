from datetime import datetime, timezone
import random
import uuid
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify(status="ok")

@app.route("/data")
def data():
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for _ in range(random.randint(2, 6)):
        record = {
            "record_id": str(uuid.uuid4()),
            "event_ts": now,
            "value": round(random.uniform(10.0, 200.0), 2),
            "source": "fake-api",
        }
        if random.random() < 0.1:
            record.pop("value")
        records.append(record)

    return jsonify(data=records, generated_at=now)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
