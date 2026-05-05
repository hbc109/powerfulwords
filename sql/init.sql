CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_bucket TEXT NOT NULL,
    access_mode TEXT NOT NULL,
    cost_level TEXT NOT NULL,
    credibility_tier INTEGER NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS documents (
    document_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(source_id),
    source_bucket TEXT NOT NULL,
    file_path TEXT,
    title TEXT,
    source_name TEXT NOT NULL,
    publisher_or_channel TEXT,
    language TEXT,
    region TEXT,
    commodity TEXT NOT NULL DEFAULT 'crude_oil',
    subtheme TEXT,
    access_mode TEXT,
    cost_level TEXT,
    rights_note TEXT,
    published_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checksum TEXT,
    quality_tier INTEGER,
    rumor_flag BOOLEAN DEFAULT FALSE,
    verification_status TEXT DEFAULT 'unverified',
    raw_text TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES documents(document_id),
    chunk_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    token_estimate INTEGER,
    metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS narrative_events (
    event_id TEXT PRIMARY KEY,
    document_id TEXT REFERENCES documents(document_id),
    chunk_id TEXT REFERENCES chunks(chunk_id),
    event_time TIMESTAMP NOT NULL,
    commodity TEXT NOT NULL,
    theme TEXT,
    topic TEXT NOT NULL,
    direction TEXT NOT NULL,
    source_bucket TEXT NOT NULL,
    source_name TEXT NOT NULL,
    credibility REAL NOT NULL,
    novelty REAL NOT NULL,
    breadth REAL,
    persistence REAL,
    crowding REAL,
    price_confirmation REAL,
    verification_status TEXT NOT NULL,
    horizon TEXT NOT NULL,
    rumor_flag BOOLEAN NOT NULL DEFAULT FALSE,
    confidence REAL,
    entities_json TEXT,
    regions_json TEXT,
    asset_candidates_json TEXT,
    evidence_text TEXT NOT NULL,
    evidence_spans_json TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS daily_narrative_scores (
    score_date DATE NOT NULL,
    commodity TEXT NOT NULL,
    theme TEXT,
    topic TEXT NOT NULL,
    narrative_score REAL NOT NULL,
    raw_score REAL,
    event_count INTEGER,
    breadth REAL,
    persistence REAL,
    source_divergence REAL,
    official_confirmation_score REAL,
    news_breadth_score REAL,
    chatter_score REAL,
    crowding_score REAL,
    PRIMARY KEY (score_date, commodity, topic)
);

CREATE TABLE IF NOT EXISTS daily_theme_scores (
    score_date DATE NOT NULL,
    commodity TEXT NOT NULL,
    theme TEXT NOT NULL,
    narrative_score REAL NOT NULL,
    raw_score REAL,
    event_count INTEGER,
    subtheme_count INTEGER,
    breadth REAL,
    persistence REAL,
    source_divergence REAL,
    top_subthemes_json TEXT,
    PRIMARY KEY (score_date, commodity, theme)
);

CREATE TABLE IF NOT EXISTS market_prices (
    price_time TIMESTAMP NOT NULL,
    symbol TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    PRIMARY KEY (price_time, symbol)
);

CREATE TABLE IF NOT EXISTS daily_regimes (
    regime_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    close REAL,
    rsi14 REAL,
    adx14 REAL,
    atr14 REAL,
    atr_ratio REAL,
    bb_pctb REAL,
    sma50 REAL,
    sma50_slope_5d_pct REAL,
    regime_tags TEXT NOT NULL,
    primary_regime TEXT NOT NULL,
    PRIMARY KEY (regime_date, symbol)
);
