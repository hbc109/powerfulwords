from pathlib import Path
import subprocess
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
APP_PATH = BASE_DIR / "app" / "dashboard" / "streamlit_app.py"

def main():
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(APP_PATH),
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]
    subprocess.run(cmd, check=True)

if __name__ == "__main__":
    main()
