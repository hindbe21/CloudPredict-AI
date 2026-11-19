import csv
import io
from pathlib import Path
from zipfile import ZipFile

root = Path(__file__).resolve().parents[1]

archive = root / "data" / "raw" / "data_Q4_2025.zip"
output = root / "data" / "raw" / "backblaze_multiday.csv"

columns = [
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure"
]

dates = [
    "2025-10-01.csv",
    "2025-11-01.csv",
    "2025-12-01.csv"
]

with ZipFile(archive) as zip_file:
    entries = {
        Path(name).name: name
        for name in zip_file.namelist()
        if name.endswith(".csv")
    }

    missing = [date for date in dates if date not in entries]
    if missing:
        raise RuntimeError("CSV absents du ZIP : " + ", ".join(missing))

    with output.open("w", newline="", encoding="utf-8") as result:
        writer = csv.writer(result)
        writer.writerow(columns)

        for date in dates:
            count = 0
            failures = 0

            with zip_file.open(entries[date]) as raw:
                text = io.TextIOWrapper(
                    raw, encoding="utf-8-sig", newline=""
                )
                reader = csv.reader(text)

                header = next(reader)
                indexes = [header.index(c) for c in columns]

                for row in reader:
                    if len(row) <= max(indexes):
                        continue

                    selected = [row[i] for i in indexes]
                    writer.writerow(selected)

                    count += 1
                    if selected[4].strip() == "1":
                        failures += 1

            print(
                date,
                "| observations :", count,
                "| pannes :", failures
            )

print("Fichier cree :", output)
print("Taille Mo :", round(output.stat().st_size / 1048576, 2))
