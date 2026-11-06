import argparse, csv, io, json, sys, time, zipfile
from pathlib import Path
from confluent_kafka import Producer

TOPIC = "cloudpredict.metrics.raw"
INT_COLS = {"capacity_bytes", "failure"}
STR_COLS = {"date", "serial_number", "model"}


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


def iter_rows(path: Path):
    """Lit un CSV, un ZIP ou les CSV d'un dossier en streaming."""
    if path.is_dir():
        paths = sorted(p for p in path.rglob("*")
                       if p.is_file() and p.suffix.lower() == ".csv"
                       and "__MACOSX" not in p.parts)
        if not paths:
            raise ValueError(f"Aucun CSV trouve dans {path}")
        for csv_path in paths:
            yield from iter_rows(csv_path)
    elif path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            names = sorted(n for n in zf.namelist()
                           if n.endswith(".csv") and not n.startswith("__MACOSX"))
            for name in names:
                with zf.open(name) as f:
                    yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
    else:
        with open(path, newline="", encoding="utf-8") as f:
            yield from csv.DictReader(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Chemin d'un CSV, ZIP ou dossier de CSV")
    ap.add_argument("--limit", type=int, default=100, help="0 = tout envoyer")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--delay", type=float, default=0.0, help="pause entre messages (s)")
    args = ap.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 50})
    sent = 0
    errors = 0

    def on_delivery(err, msg):
        nonlocal errors
        if err:
            errors += 1
            print(f"Erreur livraison: {err}", file=sys.stderr)

    t0 = time.time()
    for row in iter_rows(Path(args.file)):
        event = convert(row)
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
