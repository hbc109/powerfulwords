from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.database import get_connection

def main():
    conn = get_connection()
    cur = conn.execute(
        '''
        SELECT score_date, commodity, topic, narrative_score, official_confirmation_score,
               news_breadth_score, chatter_score, crowding_score
        FROM daily_narrative_scores
        ORDER BY score_date DESC, ABS(narrative_score) DESC
        LIMIT 30
        '''
    )
    rows = cur.fetchall()
    for r in rows:
        print(r)
    conn.close()

if __name__ == "__main__":
    main()
