import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "producer"))
from producer import convert


def test_convert():
    row = {"date": "2024-01-01", "serial_number": "ABC", "model": "ST4000",
           "capacity_bytes": "4000787030016", "failure": "0",
           "smart_1_raw": "", "smart_5_raw": "12"}
    e = convert(row)
    assert e["failure"] == 0
    assert e["capacity_bytes"] == 4000787030016
    assert e["smart_1_raw"] is None
    assert e["smart_5_raw"] == 12.0
    assert e["serial_number"] == "ABC"