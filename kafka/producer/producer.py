import argparse, csv, hashlib, io, json, sys, time, zipfile
from itertools import islice
from pathlib import Path
from confluent_kafka import Producer

TOPIC = "cloudpredict.metrics.raw"
INT_COLS = {"capacity_bytes", "failure"}
STR_COLS = {"date", "serial_number", "model"}
# Champs dont le pipeline ML a besoin
ML_FIELDS = ["date", "serial_number", "model", "capacity_bytes"] + [
    f"smart_{i}_{kind}" for i in (5, 9, 194, 197, 198) for kind in ("raw", "normalized")]


def keep_disk(serial_number, percent):
    """Meme selection deterministe par disque que prepare_history.py :
    un disque retenu l'est pour tous les jours."""
    digest = hashlib.blake2b(serial_number.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % 100 < percent


def select_events(rows, disk_percent=0, start="", end=""):
    """Convertit les lignes, filtre par dates et par disque, verifie les champs ML."""
    for row in rows:
        date = row.get("date") or ""
        if (start and date < start) or (end and date > end):
            continue
        if disk_percent and not keep_disk((row.get("serial_number") or "").strip(), disk_percent):
            continue
        absent = [c for c in ML_FIELDS if c not in row]
        if absent:
            raise ValueError(f"Champs ML absents du CSV : {absent}")
        yield convert(row)


def convert(row):
    """Convertit les chaînes CSV en types JSON propres (vide -> None)."""
    out = {}
    for k, v in row.items():
        if v is None or v == "":
            out[k] = None
        elif k in STR_COLS:
            out[k] = v
        elif k in INT_COLS:
            out[k] = int(float(v))
        else:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
    return out


def read_csv(text_stream, per_file, every):
    """Lit un CSV en ne parsant que les lignes gardées (beaucoup plus rapide)."""
    header = next(csv.reader([text_stream.readline()]))
    lines = islice(text_stream, 0, None, every) if every > 1 else text_stream
    if per_file:
        lines = islice(lines, per_file)
    for row in csv.reader(lines):
        if row:
            yield dict(zip(header, row))


def in_range(name, start, end):
    """Ignore sans les lire les CSV journaliers hors de la periode demandee."""
    day = Path(name).stem
    if len(day) != 10 or day[4] != "-":
        return True
    return not ((start and day < start) or (end and day > end))


def iter_rows(path: Path, per_file: int = 0, every: int = 1, start: str = "", end: str = ""):
    """Lit un CSV, un ZIP ou les CSV d'un dossier en streaming.
    every > 1 : garde 1 ligne sur N (échantillon réparti sur tous les modèles).
    per_file > 0 : garde au plus N lignes par CSV (après le filtre every)."""
    if path.is_dir():
        paths = sorted(p for p in path.rglob("*")
                       if p.is_file() and p.suffix.lower() == ".csv"
                       and "__MACOSX" not in p.parts)
        if not paths:
            raise ValueError(f"Aucun CSV trouve dans {path}")
        for csv_path in (p for p in paths if in_range(p.name, start, end)):
            yield from iter_rows(csv_path, per_file, every)
    elif path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            names = sorted(n for n in zf.namelist()
                           if n.endswith(".csv") and not n.startswith("__MACOSX"))
            for name in (n for n in names if in_range(n, start, end)):
                with zf.open(name) as f:
                    yield from read_csv(io.TextIOWrapper(f, encoding="utf-8", newline=""),
                                        per_file, every)
    else:
        with open(path, newline="", encoding="utf-8") as f:
            yield from read_csv(f, per_file, every)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Chemin d'un CSV, ZIP ou dossier de CSV")
    ap.add_argument("--limit", type=int, default=100, help="0 = tout envoyer")
    ap.add_argument("--per-file", type=int, default=0,
                    help="N lignes max par CSV (échantillon multi-jours), 0 = tout")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--delay", type=float, default=0.0, help="pause entre messages (s)")
    ap.add_argument("--every", type=int, default=1, help="1 ligne sur N (échantillon réparti)")
    ap.add_argument("--disk-percent", type=int, default=0,
                    help="garde tout l'historique de N %% des disques (0 = tous)")
    ap.add_argument("--start", default="", help="premiere date incluse, AAAA-MM-JJ")
    ap.add_argument("--end", default="", help="derniere date incluse, AAAA-MM-JJ")
    ap.add_argument("--dry-run", help="ecrit les messages dans ce fichier, sans Kafka")
    args = ap.parse_args()
    if not 0 <= args.disk_percent <= 100:
        ap.error("--disk-percent doit etre entre 0 et 100")
    if args.disk_percent and args.every > 1:
        ap.error("--disk-percent et --every sont incompatibles (historique fragmente)")

    events = select_events(iter_rows(Path(args.file), args.per_file, args.every,
                                     args.start, args.end),
                           args.disk_percent, args.start, args.end)
    if args.dry_run:
        count, disks, dates = 0, set(), set()
        with open(args.dry_run, "w", encoding="utf-8") as out:
            for event in events:
                out.write(json.dumps(event) + "\n")
                count += 1
                disks.add(event["serial_number"])
                dates.add(event["date"])
                if args.limit and count >= args.limit:
                    break
        print(f"Essai: {count} messages, {len(disks)} disques, {len(dates)} jours -> {args.dry_run}")
        return

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50})
    sent = 0
    errors = 0

    def on_delivery(err, msg):
        nonlocal errors
        if err:
            errors += 1
            print(f"Erreur livraison: {err}", file=sys.stderr)

    t0 = time.time()
    for event in events:
        while True:
            try:
                producer.produce(
                    TOPIC,
                    key=(event.get("serial_number") or "").encode(),
                    value=json.dumps(event).encode(),
                    callback=on_delivery,
                )
                break
            except BufferError:      # file locale pleine : on laisse Kafka vider
                producer.poll(0.5)
        sent += 1
        producer.poll(0)
        if args.delay:
            time.sleep(args.delay)
        if sent % 1000 == 0:
            print(f"{sent} messages envoyés...")
        if args.limit and sent >= args.limit:
            break

    producer.flush()
    print(f"Terminé: {sent} messages, {errors} erreurs, {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
