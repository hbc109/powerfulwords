from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.config.settings import load_source_registry
from app.db.database import init_db, get_connection
from app.db.repository import insert_source

def main() -> None:
    init_db()
    registry = load_source_registry()
    conn = get_connection()
    for row in registry.get("sources", []):
        insert_source(conn, row)
    conn.commit()
    conn.close()
    print(f"Loaded {len(registry.get('sources', []))} sources into SQLite.")

if __name__ == "__main__":
    main()
