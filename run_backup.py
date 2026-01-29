import subprocess
import sys

def run(cmd: list[str]) -> None:
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)

if __name__ == "__main__":
    run([sys.executable, "src/shopify_sync.py"])
    run([sys.executable, "src/load_to_bigquery.py"])
    print("✅ Backup pipeline completed")
