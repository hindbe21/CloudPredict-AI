
import argparse
import csv
import hashlib
import io
import re
import zipfile
from functools import lru_cache
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--percent", type=int, default=20)
parser.add_argument("--days", type=int, default=0)
args = parser.parse_args()

if not 1 <= args.percent <= 100:
    parser.error("--percent doit etre compris entre 1 et 100")

if args.days < 0:
    parser.error("--days doit etre positif ou nul")

columns = [
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure"
]

for smart in [5, 9, 187, 188, 194, 197, 198]:
    columns.extend([
        f"smart_{smart}_raw",
        f"smart_{smart}_normalized"
    ])


@lru_cache(maxsize=500000)
def keep_disk(serial_number):
    digest = hashlib.blake2b(
        serial_number.encode("utf-8"),
        digest_size=8
    ).digest()

    value = int.from_bytes(digest, "big")
    return value % 100 < args.percent


output = Path(args.output)
output.mkdir(parents=True, exist_ok=False)

total_read = 0
total_saved = 0
total_failures = 0
total_selected_failures = 0
missing_serials = 0

missing = {c: 0 for c in columns if c.startswith("smart_")}

with zipfile.ZipFile(args.input) as archive:

    files = sorted(
        [
            f for f in archive.infolist()
            if re.fullmatch(
                r"\d{4}-\d{2}-\d{2}\.csv",
                Path(f.filename).name
            )
        ],
        key=lambda f: Path(f.filename).name
    )

    if args.days:
        files = files[:args.days]

    if not files:
        raise ValueError("Aucun CSV journalier trouve")

    print("=== CLOUDPREDICT AI - HISTORIQUE ML ===")
    print("Fichiers a traiter :", len(files))
    print("Colonnes conservees :", len(columns))
    print("Taux de selection :", args.percent, "%")

    for file in files:

        filename = Path(file.filename).name
        rows_read = 0
        rows_saved = 0
        failures = 0
        selected_failures = 0

        with archive.open(file) as raw:

            with io.TextIOWrapper(
                raw, encoding="utf-8-sig", newline=""
            ) as source:

                reader = csv.reader(source)
                header = next(reader)

                absent = [c for c in columns if c not in header]

                if absent:
                    raise ValueError(
                        f"{filename}: colonnes absentes {absent}"
                    )

                indices = [header.index(c) for c in columns]
                serial_idx = header.index("serial_number")
                failure_idx = header.index("failure")

                with (output / filename).open(
                    "w", encoding="utf-8", newline=""
                ) as target:

                    writer = csv.writer(target)
                    writer.writerow(columns)

                    for row in reader:

                        if not row:
                            continue

                        if len(row) != len(header):
                            raise ValueError(
                                f"Ligne invalide dans {filename}"
                            )

                        rows_read += 1
                        failed = row[failure_idx] == "1"

                        if failed:
                            failures += 1

                        serial = row[serial_idx].strip()

                        if not serial:
                            missing_serials += 1
                            continue

                        if not keep_disk(serial):
                            continue

                        values = [row[i] for i in indices]
                        writer.writerow(values)

                        rows_saved += 1

                        for col, value in zip(columns, values):
                            if col in missing and not value.strip():
                                missing[col] += 1

                        if failed:
                            selected_failures += 1

        total_read += rows_read
        total_saved += rows_saved
        total_failures += failures
        total_selected_failures += selected_failures

        print(
            f"{filename} | "
            f"Lues: {rows_read} | "
            f"Gardees: {rows_saved} | "
            f"Pannes: {selected_failures}/{failures}",
            flush=True
        )

print("\n=== RESULTATS ===")
print("Observations lues :", total_read)
print("Observations conservees :", total_saved)
print("Pannes originales :", total_failures)
print("Pannes conservees :", total_selected_failures)
print("Identifiants absents exclus :", missing_serials)

print("\n=== VALEURS SMART MANQUANTES ===")

if total_saved:
    for col, count in missing.items():
        print(f"{col}: {100 * count / total_saved:.2f}%")

print("\nDossier de sortie :", output)
print("SUCCES : HISTORIQUE PREPARE")
