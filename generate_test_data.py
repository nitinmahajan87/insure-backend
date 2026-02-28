import pandas as pd
from datetime import datetime, date
import uuid
import os


def generate_bulk_additions(file_name="bulk_additions.csv", num_records=5):
    """Generates a CSV for the Batch Additions endpoint."""
    data = []

    for i in range(1, num_records + 1):
        record = {
            "transaction_id": f"TXN-ADD-{uuid.uuid4().hex[:6].upper()}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "employee_code": f"WIP_EMP_{100 + i}",
            "first_name": ["Rahul", "Priya", "Amit", "Sonia", "Vikram"][i % 5],
            "last_name": ["Sharma", "Kaur", "Singh", "Mehta", "Verma"][i % 5],
            "date_of_birth": date(1985 + (i % 10), (i % 12) + 1, 15).strftime("%Y-%m-%d"),
            "date_of_joining": date.today().strftime("%Y-%m-%d"),
            "gender": "Male" if i % 2 == 0 else "Female",
            "relationship": "Self",
            "sum_insured": 500000.0
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_csv(file_name, index=False)
    print(f"✅ Generated {file_name} with {num_records} records.")


def generate_bulk_deletions(file_name="bulk_deletions.csv", num_records=3):
    """Generates a CSV for the Batch Deletions endpoint."""
    data = []

    for i in range(1, num_records + 1):
        # Using employee codes that likely exist from your previous tests
        record = {
            "transaction_id": f"TXN-DEL-{uuid.uuid4().hex[:6].upper()}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "employee_code": f"WIP_EMP_{100 + i}",
            "member_id": f"MEM-{1000 + i}",
            "date_of_leaving": date.today().strftime("%Y-%m-%d")
        }
        data.append(record)

    df = pd.DataFrame(data)
    df.to_csv(file_name, index=False)
    print(f"✅ Generated {file_name} with {num_records} records.")


if __name__ == "__main__":
    # Ensure you have pandas installed: pip install pandas
    generate_bulk_additions("bulk_additions_v2.csv", num_records=10)
    generate_bulk_deletions("bulk_deletions_v2.csv", num_records=5)