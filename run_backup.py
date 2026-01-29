import subprocess
import sys
import os
from pathlib import Path

def run(cmd):
    print("Running:", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)

if __name__ == "__main__":
    # Cloud Run only allows writing to /tmp
    os.environ.setdefault("SYNC_DATA_PATH", "/tmp/sync_data.json")

    # repo root scripts (your current structure)
    here = Path(__file__).resolve().parent
    shopify = here / "shopify_sync.py"
    loader = here / "load_to_bigquery.py"

    if not shopify.exists():
        raise FileNotFoundError(f"Missing {shopify}. Check Docker COPY context / paths.")
    if not loader.exists():
        raise FileNotFoundError(f"Missing {loader}. Check Docker COPY context / paths.")

    run([sys.executable, str(shopify)])
    run([sys.executable, str(loader)])

    print("✅ Backup pipeline completed", flush=True)
