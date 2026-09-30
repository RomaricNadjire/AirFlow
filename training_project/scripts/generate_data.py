import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


BASE_DIR = Path(
    "/home/romaric/Documents/training/AirFlow/training_project"
)

RAW_DIR = BASE_DIR / "data/raw"


def generate_day(day, number_of_transactions, start_id):
    date_str = day.strftime("%Y-%m-%d")
    output = RAW_DIR / f"{date_str}.csv"

    countries = ["TG", "BJ", "GH", "CI"]

    with output.open("w", newline="") as file:
        writer = csv.writer(file)

        writer.writerow([
            "transaction_id",
            "customer_id",
            "product_id",
            "amount",
            "country",
            "timestamp",
        ])

        for i in range(number_of_transactions):
            transaction_id = start_id + i

            customer_id = random.randint(1, 200)
            product_id = random.randint(1, 50)
            amount = round(random.uniform(5, 500), 2)
            country = random.choice(countries)

            timestamp = day + timedelta(
                seconds=random.randint(0, 86399)
            )

            writer.writerow([
                transaction_id,
                customer_id,
                product_id,
                amount,
                country,
                timestamp.isoformat(),
            ])

    print(f"{output} -> {number_of_transactions} transactions")


def main():

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    start_date = datetime(2026, 9, 1)

    daily_volumes = [
        1000,
        1200,
        900,
        1500,
        1100,
        5000
    ]

    current_id = 1

    for day_offset, volume in enumerate(daily_volumes):

        day = start_date + timedelta(days=day_offset)

        generate_day(
            day=day,
            number_of_transactions=volume,
            start_id=current_id,
        )

        current_id += volume


if __name__ == "__main__":
    main()