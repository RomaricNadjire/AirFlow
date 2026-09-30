import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT = Path("/home/romaric/Documents/training/AirFlow/training_project/hadoop-spark/data/large_transactions.csv")

ROWS = 1_000_000

countries = ["TG", "BJ", "GH", "CI", "SN", "NG", "CM"]
start_date = datetime(2026, 1, 1)

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with OUTPUT.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    writer.writerow([
        "transaction_id",
        "customer_id",
        "product_id",
        "amount",
        "country",
        "timestamp"
    ])

    for i in range(1, ROWS + 1):
        timestamp = start_date + timedelta(
            seconds=random.randint(0, 31 * 24 * 3600)
        )

        writer.writerow([
            i,
            random.randint(1, 100_000),
            random.randint(1, 10_000),
            round(random.uniform(5, 2000), 2),
            random.choice(countries),
            timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        ])

print(f"Dataset généré : {OUTPUT}")
print(f"Nombre de lignes : {ROWS:,}")