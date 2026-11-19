import argparse
from collections import Counter
from pathlib import Path
from producer import iter_rows, convert


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--limit", type=int, default=100000, help="0 = tout lire")
    ap.add_argument("--per-file", type=int, default=0,
                    help="N lignes max par CSV (échantillon multi-jours), 0 = tout")
    ap.add_argument("--every", type=int, default=1,
                    help="1 ligne sur N (échantillon réparti)")
    a = ap.parse_args()

    n = failures = dups = missing_serial = 0
    seen, serials, dates = set(), set(), set()
    models, nulls = Counter(), Counter()
    failed_models = Counter()

    for row in iter_rows(Path(a.file), a.per_file, a.every):
        e = convert(row)
        n += 1
        if n % 100000 == 0:
            print(f"{n} lignes lues...", flush=True)
        sn, d = e.get("serial_number"), e.get("date")
        if not sn:
            missing_serial += 1
        serials.add(sn)
        dates.add(d)
        models[e.get("model")] += 1
        f = e.get("failure") or 0
        failures += f
        if f:
            failed_models[e.get("model")] += 1
        if (d, sn) in seen:
            dups += 1
        seen.add((d, sn))
        for k, v in e.items():
            if v is None:
                nulls[k] += 1
        if a.limit and n >= a.limit:
            break

    print(f"Lignes lues        : {n}")
    print(f"Disques distincts  : {len(serials)}")
    print(f"Dates              : {min(dates)} -> {max(dates)} ({len(dates)} jours)")
    print(f"Pannes (failure=1) : {failures}")
    print(f"Serial manquant    : {missing_serial}")
    print(f"Doublons date+serie: {dups}")
    print(f"Modèles distincts  : {len(models)}")
    print("Top modèles        :", models.most_common(5))
    print("Modèles en panne   :", failed_models.most_common(5))
    print("Colonnes les plus nulles (%):")
    for k, c in nulls.most_common(8):
        print(f"  {k}: {100*c/n:.1f}")


if __name__ == "__main__":
    main()