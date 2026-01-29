import subprocess
import sys
import os
from pathlib import Path

def run(cmd):
    print("Running:", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)

if __name__ == "__main__":
    os.environ.setdefault("SYNC_DATA_PATH", "/tmp/sync_data.json")

    here = Path(__file__).resolve().parent

    # Support both repo layouts:
    candidates = [
        (here / "shopify_sync.py", here / "load_to_bigquery.py"),         # root layout
        (here / "src" / "shopify_sync.py", here / "src" / "load_to_bigquery.py"),  # src/ layout
    ]

    for shopify, loader in candidates:
        if shopify.exists() and loader.exists():
            run([sys.executable, str(shopify)])
            run([sys.executable, str(loader)])
            print("✅ Backup pipeline completed", flush=True)
            raise SystemExit(0)

    # If none found, print a directory listing to help debug build context
    print("❌ Could not find shopify_sync.py + load_to_bigquery.py", flush=True)
    print("Contents of /app:", sorted([p.name for p in here.iterdir()]), flush=True)
    if (here / "src").exists():
        print("Contents of /app/src:", sorted([p.name for p in (here / "src").iterdir()]), flush=True)
    raise FileNotFoundError("Missing expected scripts in container.")
