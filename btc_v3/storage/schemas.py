from __future__ import annotations

# Independent strategy research schema. Historical account tables are never migrated or removed.

MARKET_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    slug TEXT PRIMARY KEY,
    event_id TEXT,
    condition_id TEXT,
    round_start_us INTEGER NOT NULL,
    round_end_us INTEGER NOT NULL,
    question TEXT,
    rules_text TEXT,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    first_observed_at_us INTEGER NOT NULL,
    last_observed_at_us INTEGER NOT NULL,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_markets_round_end ON markets(round_end_us);

CREATE TABLE IF NOT EXISTS market_tokens (
    slug TEXT NOT NULL REFERENCES markets(slug),
    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),
    token_id TEXT NOT NULL,
    first_observed_at_us INTEGER NOT NULL,
    last_observed_at_us INTEGER NOT NULL,
    PRIMARY KEY (slug, side)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_market_tokens_token ON market_tokens(token_id);

CREATE TABLE IF NOT EXISTS market_metadata_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    source_ts_us INTEGER,
    received_at_us INTEGER NOT NULL,
    fee_rate TEXT,
    fee_exponent INTEGER,
    tick_size TEXT,
    min_order_notional TEXT,
    min_order_size TEXT,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_metadata_slug_recv ON market_metadata_versions(slug, received_at_us);

CREATE TABLE IF NOT EXISTS book_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),
    source_ts_us INTEGER,
    server_ts_us INTEGER,
    received_at_us INTEGER NOT NULL,
    receive_sequence INTEGER NOT NULL,
    sync_generation INTEGER NOT NULL,
    bids_json TEXT NOT NULL,
    asks_json TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_book_cp_token_recv ON book_checkpoints(token_id, received_at_us);

CREATE TABLE IF NOT EXISTS book_deltas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),
    source_ts_us INTEGER,
    server_ts_us INTEGER,
    received_at_us INTEGER NOT NULL,
    receive_sequence INTEGER NOT NULL,
    sync_generation INTEGER NOT NULL,
    delta_json TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_book_delta_token_recv ON book_deltas(token_id, received_at_us);

CREATE TABLE IF NOT EXISTS best_quote_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('UP','DOWN')),
    received_at_us INTEGER NOT NULL,
    best_bid TEXT,
    best_bid_size TEXT,
    best_ask TEXT,
    best_ask_size TEXT,
    spread TEXT,
    sync_generation INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_best_quote_slug_recv ON best_quote_observations(slug, received_at_us);

CREATE TABLE IF NOT EXISTS market_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    token_id TEXT,
    source_trade_id TEXT,
    source_ts_us INTEGER,
    received_at_us INTEGER NOT NULL,
    price TEXT NOT NULL,
    size TEXT NOT NULL,
    taker_side TEXT,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_market_trades_slug_ts ON market_trades(slug, received_at_us);

CREATE TABLE IF NOT EXISTS fee_schedule_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    observed_at_us INTEGER NOT NULL,
    fee_model TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    source TEXT NOT NULL,
    raw_payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_fee_slug_obs ON fee_schedule_versions(slug, observed_at_us);

CREATE TABLE IF NOT EXISTS market_resolutions (
    slug TEXT PRIMARY KEY,
    resolved_side TEXT NOT NULL CHECK(resolved_side IN ('UP','DOWN')),
    resolution_ts_us INTEGER,
    observed_at_us INTEGER NOT NULL,
    source TEXT NOT NULL,
    official_final_value TEXT,
    price_to_beat TEXT,
    raw_payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS storage_health_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at_us INTEGER NOT NULL,
    db_bytes INTEGER NOT NULL,
    wal_bytes INTEGER NOT NULL,
    raw_bytes INTEGER NOT NULL,
    free_bytes INTEGER NOT NULL,
    db_growth_bytes_per_hour REAL,
    estimated_12h_db_growth_bytes REAL,
    warning_level TEXT NOT NULL,
    details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_storage_health_obs ON storage_health_snapshots(observed_at_us);

CREATE TABLE IF NOT EXISTS collector_sessions (
    session_id TEXT PRIMARY KEY,
    component TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    last_healthy_at_us INTEGER,
    fatal INTEGER NOT NULL DEFAULT 0 CHECK(fatal IN (0,1))
);

CREATE TABLE IF NOT EXISTS feed_gap_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL,
    slug TEXT,
    detected_at_us INTEGER NOT NULL,
    recovered_at_us INTEGER,
    reason TEXT NOT NULL,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS collector_status (
    component TEXT PRIMARY KEY,
    session_id TEXT,
    slug TEXT,
    state TEXT NOT NULL,
    updated_at_us INTEGER NOT NULL,
    last_message_at_us INTEGER,
    last_book_at_us INTEGER,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_capture_manifest (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL,
    utc_day TEXT NOT NULL,
    path TEXT NOT NULL,
    first_received_at_us INTEGER,
    last_received_at_us INTEGER,
    record_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(component, utc_day, path)
);
"""

EXTERNAL_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS coinbase_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_ts_us INTEGER,
    received_at_us INTEGER NOT NULL,
    receive_sequence INTEGER NOT NULL,
    price TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_coinbase_recv ON coinbase_ticks(received_at_us);
CREATE INDEX IF NOT EXISTS idx_coinbase_source ON coinbase_ticks(source_ts_us);
CREATE INDEX IF NOT EXISTS idx_coinbase_causal
    ON coinbase_ticks(COALESCE(source_ts_us, received_at_us), received_at_us, id);

CREATE TABLE IF NOT EXISTS kraken_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_ts_us INTEGER,
    received_at_us INTEGER NOT NULL,
    receive_sequence INTEGER NOT NULL,
    price TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kraken_recv ON kraken_ticks(received_at_us);
CREATE INDEX IF NOT EXISTS idx_kraken_source ON kraken_ticks(source_ts_us);
CREATE INDEX IF NOT EXISTS idx_kraken_causal
    ON kraken_ticks(COALESCE(source_ts_us, received_at_us), received_at_us, id);

CREATE TABLE IF NOT EXISTS settlement_reference_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_ts_us INTEGER,
    received_at_us INTEGER NOT NULL,
    receive_sequence INTEGER NOT NULL,
    value TEXT NOT NULL,
    source TEXT NOT NULL,
    stream_id TEXT,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_settlement_ref_recv ON settlement_reference_observations(received_at_us);
CREATE INDEX IF NOT EXISTS idx_settlement_ref_source ON settlement_reference_observations(source_ts_us);
CREATE INDEX IF NOT EXISTS idx_settlement_ref_causal
    ON settlement_reference_observations(source_ts_us, received_at_us, id);

CREATE TABLE IF NOT EXISTS price_references (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    round_start_us INTEGER NOT NULL,
    round_end_us INTEGER NOT NULL,
    price_to_beat TEXT NOT NULL,
    reference_ts_us INTEGER,
    observed_at_us INTEGER NOT NULL,
    source TEXT NOT NULL,
    source_stream TEXT,
    mapping_method TEXT NOT NULL,
    raw_payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_price_ref_slug_obs ON price_references(slug, observed_at_us);
CREATE INDEX IF NOT EXISTS idx_price_ref_exact_causal
    ON price_references(slug, reference_ts_us, observed_at_us, id);

CREATE TABLE IF NOT EXISTS external_collector_sessions (
    session_id TEXT PRIMARY KEY,
    component TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    last_healthy_at_us INTEGER,
    fatal INTEGER NOT NULL DEFAULT 0 CHECK(fatal IN (0,1))
);

CREATE TABLE IF NOT EXISTS external_gap_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL,
    detected_at_us INTEGER NOT NULL,
    recovered_at_us INTEGER,
    reason TEXT NOT NULL,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS external_status (
    component TEXT PRIMARY KEY,
    session_id TEXT,
    state TEXT NOT NULL,
    updated_at_us INTEGER NOT NULL,
    last_message_at_us INTEGER,
    last_tick_at_us INTEGER,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    details_json TEXT NOT NULL
);
"""

FORWARD_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    experiment_name TEXT NOT NULL,
    tournament_config_hash TEXT NOT NULL,
    tournament_config_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at_us INTEGER NOT NULL,
    frozen_at_us INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('ACTIVE','STOPPED','COMPLETE'))
);
CREATE INDEX IF NOT EXISTS idx_experiments_name ON experiments(experiment_name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_experiments_hash ON experiments(tournament_config_hash);

CREATE TABLE IF NOT EXISTS strategy_configs (
    strategy_config_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
    strategy_family TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    config_json TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    UNIQUE(experiment_id, config_hash)
);

CREATE TABLE IF NOT EXISTS rounds (
    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
    slug TEXT NOT NULL,
    round_start_us INTEGER NOT NULL,
    round_end_us INTEGER NOT NULL,
    first_seen_at_us INTEGER NOT NULL,
    final_resolution TEXT,
    resolution_observed_at_us INTEGER,
    PRIMARY KEY (experiment_id, slug)
);

CREATE TABLE IF NOT EXISTS decision_states (
    decision_state_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    slug TEXT NOT NULL,
    decision_ts_us INTEGER NOT NULL,
    seconds_remaining REAL NOT NULL,
    state_hash TEXT NOT NULL,
    causality_cutoff_us INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    UNIQUE(experiment_id, slug, decision_ts_us)
);

CREATE TABLE IF NOT EXISTS feature_vectors (
    feature_vector_id TEXT PRIMARY KEY,
    decision_state_id TEXT NOT NULL REFERENCES decision_states(decision_state_id),
    feature_version TEXT NOT NULL,
    features_json TEXT NOT NULL,
    UNIQUE(decision_state_id, feature_version)
);

CREATE TABLE IF NOT EXISTS model_outputs (
    model_output_id TEXT PRIMARY KEY,
    decision_state_id TEXT NOT NULL REFERENCES decision_states(decision_state_id),
    model_config_hash TEXT NOT NULL,
    base_p_up REAL NOT NULL CHECK(base_p_up >= 0 AND base_p_up <= 1),
    base_p_down REAL NOT NULL CHECK(base_p_down >= 0 AND base_p_down <= 1),
    diagnostics_json TEXT NOT NULL,
    UNIQUE(decision_state_id, model_config_hash)
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    strategy_config_id TEXT NOT NULL REFERENCES strategy_configs(strategy_config_id),
    slug TEXT NOT NULL,
    decision_state_id TEXT NOT NULL REFERENCES decision_states(decision_state_id),
    action TEXT NOT NULL CHECK(action IN ('BUY_UP','BUY_DOWN','SKIP')),
    reason_code TEXT NOT NULL,
    final_p_up REAL CHECK(final_p_up >= 0 AND final_p_up <= 1),
    final_p_down REAL CHECK(final_p_down >= 0 AND final_p_down <= 1),
    selected_side TEXT CHECK(selected_side IN ('UP','DOWN') OR selected_side IS NULL),
    selected_edge REAL,
    effective_cost TEXT,
    created_at_us INTEGER NOT NULL,
    UNIQUE(experiment_id, strategy_config_id, slug)
);

CREATE TABLE IF NOT EXISTS strategy_signals (
    signal_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
    signal_name TEXT NOT NULL,
    signal_value REAL NOT NULL,
    signal_strength REAL,
    adjustment REAL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_orders (
    order_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL UNIQUE REFERENCES decisions(decision_id),
    intended_side TEXT NOT NULL CHECK(intended_side IN ('UP','DOWN')),
    shares TEXT NOT NULL,
    decision_ts_us INTEGER NOT NULL,
    scheduled_execution_ts_us INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('SCHEDULED','FILLED','REJECTED')),
    fee_parameters_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES paper_orders(order_id),
    execution_ts_us INTEGER NOT NULL,
    shares TEXT NOT NULL,
    vwap TEXT NOT NULL,
    worst_price TEXT NOT NULL,
    notional TEXT NOT NULL,
    fee TEXT NOT NULL,
    book_state_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_rejects (
    reject_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES paper_orders(order_id),
    rejected_at_us INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_settlements (
    settlement_id TEXT PRIMARY KEY,
    fill_id TEXT NOT NULL UNIQUE REFERENCES paper_fills(fill_id),
    settled_at_us INTEGER NOT NULL,
    resolved_side TEXT NOT NULL CHECK(resolved_side IN ('UP','DOWN')),
    payout TEXT NOT NULL,
    fee TEXT NOT NULL,
    net_pnl TEXT NOT NULL,
    resolution_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS skip_events (
    skip_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL UNIQUE REFERENCES decisions(decision_id),
    reason_code TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS causality_audits (
    audit_id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    strategy_config_id TEXT,
    slug TEXT,
    audited_at_us INTEGER NOT NULL,
    passed INTEGER NOT NULL CHECK(passed IN (0,1)),
    details_json TEXT NOT NULL
);
"""

RESEARCH_SCHEMA = r"""
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    dataset_hash TEXT NOT NULL UNIQUE,
    causality_tier TEXT NOT NULL,
    start_us INTEGER,
    end_us INTEGER,
    created_at_us INTEGER NOT NULL,
    manifest_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    source_name TEXT NOT NULL,
    provider TEXT,
    symbol TEXT,
    source_uri TEXT,
    checksum TEXT,
    transformations_json TEXT NOT NULL,
    limitations_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS split_definitions (
    split_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    name TEXT NOT NULL,
    train_start_us INTEGER,
    train_end_us INTEGER,
    validation_start_us INTEGER,
    validation_end_us INTEGER,
    holdout_start_us INTEGER,
    holdout_end_us INTEGER,
    definition_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS holdout_registry (
    split_id TEXT PRIMARY KEY REFERENCES split_definitions(split_id),
    first_exposed_at_us INTEGER,
    exposure_reason TEXT
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    backtest_run_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    split_id TEXT NOT NULL REFERENCES split_definitions(split_id),
    config_hash TEXT NOT NULL,
    config_json TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    finished_at_us INTEGER,
    status TEXT NOT NULL,
    metrics_json TEXT
);

CREATE TABLE IF NOT EXISTS optimization_studies (
    study_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(dataset_id),
    split_id TEXT NOT NULL REFERENCES split_definitions(split_id),
    objective_definition TEXT NOT NULL,
    seed INTEGER NOT NULL,
    created_at_us INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS optimization_trials (
    study_id TEXT NOT NULL REFERENCES optimization_studies(study_id),
    trial_number INTEGER NOT NULL,
    config_hash TEXT NOT NULL,
    config_json TEXT NOT NULL,
    dataset_hash TEXT NOT NULL,
    split_name TEXT NOT NULL,
    metrics_json TEXT,
    status TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    PRIMARY KEY (study_id, trial_number)
);

CREATE TABLE IF NOT EXISTS pairwise_results (
    pairwise_result_id TEXT PRIMARY KEY,
    backtest_run_id TEXT NOT NULL REFERENCES backtest_runs(backtest_run_id),
    baseline_config_hash TEXT NOT NULL,
    challenger_config_hash TEXT NOT NULL,
    horizon_seconds INTEGER NOT NULL,
    cohort_name TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    bootstrap_json TEXT
);

CREATE TABLE IF NOT EXISTS forensic_wallets (
    wallet TEXT PRIMARY KEY,
    label TEXT,
    requested_address TEXT NOT NULL,
    first_synced_at_us INTEGER NOT NULL,
    last_synced_at_us INTEGER NOT NULL,
    profile_stats_json TEXT NOT NULL,
    volume_stats_json TEXT NOT NULL,
    pnl_stats_json TEXT NOT NULL,
    gamma_profile_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS forensic_activity (
    activity_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    event_type TEXT NOT NULL,
    condition_id TEXT,
    token_id TEXT,
    side TEXT,
    outcome TEXT,
    price TEXT,
    size TEXT,
    usdc_size TEXT,
    timestamp_us INTEGER,
    transaction_hash TEXT,
    title TEXT,
    slug TEXT,
    event_slug TEXT,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_forensic_activity_wallet_time ON forensic_activity(wallet,timestamp_us);
CREATE INDEX IF NOT EXISTS idx_forensic_activity_condition ON forensic_activity(condition_id);
CREATE INDEX IF NOT EXISTS idx_forensic_activity_tx ON forensic_activity(transaction_hash);

CREATE TABLE IF NOT EXISTS forensic_trades (
    trade_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    condition_id TEXT,
    token_id TEXT,
    side TEXT,
    outcome TEXT,
    price TEXT,
    size TEXT,
    timestamp_us INTEGER,
    transaction_hash TEXT,
    title TEXT,
    slug TEXT,
    event_slug TEXT,
    maker_address TEXT,
    taker_address TEXT,
    attribution TEXT,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_forensic_trades_wallet_time ON forensic_trades(wallet,timestamp_us);
CREATE INDEX IF NOT EXISTS idx_forensic_trades_condition ON forensic_trades(condition_id);
CREATE INDEX IF NOT EXISTS idx_forensic_trades_tx ON forensic_trades(transaction_hash);

CREATE TABLE IF NOT EXISTS forensic_rounds (
    wallet TEXT NOT NULL,
    slug TEXT NOT NULL,
    round_start_us INTEGER NOT NULL,
    round_end_us INTEGER NOT NULL,
    first_action_us INTEGER,
    last_action_us INTEGER,
    buy_up_shares REAL NOT NULL,
    buy_down_shares REAL NOT NULL,
    sell_up_shares REAL NOT NULL,
    sell_down_shares REAL NOT NULL,
    buy_up_cost REAL NOT NULL,
    buy_down_cost REAL NOT NULL,
    trade_actions INTEGER NOT NULL,
    redeem_actions INTEGER NOT NULL,
    classification TEXT NOT NULL,
    traded_both_sides INTEGER NOT NULL CHECK(traded_both_sides IN (0,1)),
    had_sell INTEGER NOT NULL CHECK(had_sell IN (0,1)),
    had_redeem INTEGER NOT NULL CHECK(had_redeem IN (0,1)),
    final_60s_trade_actions INTEGER NOT NULL,
    details_json TEXT NOT NULL,
    PRIMARY KEY(wallet,slug)
);

CREATE TABLE IF NOT EXISTS forensic_reports (
    report_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS forensic_onchain_events (
    event_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    source_action TEXT NOT NULL,
    transaction_hash TEXT,
    block_number INTEGER,
    timestamp_us INTEGER,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_forensic_onchain_wallet_time ON forensic_onchain_events(wallet,timestamp_us);

CREATE TABLE IF NOT EXISTS forensic_live_sessions (
    session_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    requested_wallet TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER,
    state TEXT NOT NULL,
    target_event_count INTEGER NOT NULL DEFAULT 0,
    config_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS forensic_live_samples (
    sample_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES forensic_live_sessions(session_id),
    wallet TEXT NOT NULL,
    sample_ts_us INTEGER NOT NULL,
    slug TEXT,
    round_start_us INTEGER,
    round_end_us INTEGER,
    seconds_remaining REAL,
    state TEXT NOT NULL,
    unavailable_reason TEXT,
    ptb REAL,
    official_twap REAL,
    coinbase_price REAL,
    kraken_price REAL,
    predictor_price REAL,
    p_up_mc REAL,
    p_up_analytic REAL,
    distance_dollars REAL,
    returns_json TEXT NOT NULL,
    volatility_json TEXT NOT NULL,
    lab_json TEXT NOT NULL,
    book_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_forensic_live_samples_session_time
    ON forensic_live_samples(session_id,sample_ts_us);
CREATE INDEX IF NOT EXISTS idx_forensic_live_samples_wallet_time
    ON forensic_live_samples(wallet,sample_ts_us);

CREATE TABLE IF NOT EXISTS forensic_target_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES forensic_live_sessions(session_id),
    wallet TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    trade_ts_us INTEGER,
    detected_at_us INTEGER NOT NULL,
    observable_lag_us INTEGER,
    slug TEXT,
    side TEXT,
    outcome TEXT,
    price TEXT,
    size TEXT,
    attribution TEXT,
    pre_trade_sample_id TEXT REFERENCES forensic_live_samples(sample_id),
    detection_sample_id TEXT REFERENCES forensic_live_samples(sample_id),
    current_executable_price TEXT,
    copy_price_delta TEXT,
    market_context_matches INTEGER NOT NULL CHECK(market_context_matches IN (0,1)),
    raw_json TEXT NOT NULL,
    UNIQUE(session_id,trade_id)
);
CREATE INDEX IF NOT EXISTS idx_forensic_target_events_wallet_time
    ON forensic_target_events(wallet,trade_ts_us);

CREATE TABLE IF NOT EXISTS forensic_copy_quotes (
    quote_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES forensic_target_events(event_id),
    session_id TEXT NOT NULL REFERENCES forensic_live_sessions(session_id),
    wallet TEXT NOT NULL,
    delay_ms INTEGER NOT NULL,
    quote_ts_us INTEGER NOT NULL,
    slug TEXT,
    side TEXT,
    outcome TEXT,
    target_price TEXT,
    executable_price TEXT,
    price_delta TEXT,
    market_context_matches INTEGER NOT NULL CHECK(market_context_matches IN (0,1)),
    details_json TEXT NOT NULL,
    UNIQUE(event_id,delay_ms)
);
CREATE INDEX IF NOT EXISTS idx_forensic_copy_quotes_wallet_delay
    ON forensic_copy_quotes(wallet,delay_ms);



CREATE TABLE IF NOT EXISTS forensic_latency_sessions (
    session_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    requested_wallet TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER,
    state TEXT NOT NULL,
    poll_count INTEGER NOT NULL DEFAULT 0,
    target_event_count INTEGER NOT NULL DEFAULT 0,
    rate_limit_count INTEGER NOT NULL DEFAULT 0,
    service_unavailable_count INTEGER NOT NULL DEFAULT 0,
    config_json TEXT NOT NULL,
    verdict_json TEXT
);

CREATE TABLE IF NOT EXISTS forensic_latency_polls (
    poll_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES forensic_latency_sessions(session_id),
    seq INTEGER NOT NULL,
    request_started_us INTEGER NOT NULL,
    response_received_us INTEGER NOT NULL,
    rtt_us INTEGER NOT NULL,
    status_code INTEGER,
    retry_after_ms INTEGER,
    trace_id TEXT,
    row_count INTEGER NOT NULL,
    newest_trade_ts_us INTEGER,
    error_text TEXT,
    UNIQUE(session_id,seq)
);
CREATE INDEX IF NOT EXISTS idx_forensic_latency_polls_session_time
    ON forensic_latency_polls(session_id,response_received_us);

CREATE TABLE IF NOT EXISTS forensic_latency_events (
    latency_event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES forensic_latency_sessions(session_id),
    wallet TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    trade_ts_us INTEGER,
    api_first_seen_us INTEGER NOT NULL,
    api_request_started_us INTEGER NOT NULL,
    api_rtt_us INTEGER NOT NULL,
    slug TEXT,
    token_id TEXT,
    side TEXT,
    outcome TEXT,
    price TEXT,
    size TEXT,
    transaction_hash TEXT,
    market_trade_id INTEGER,
    market_received_at_us INTEGER,
    market_source_ts_us INTEGER,
    market_to_wallet_us INTEGER,
    correlation_quality TEXT NOT NULL,
    correlation_candidate_count INTEGER NOT NULL,
    detection_sample_id TEXT REFERENCES forensic_live_samples(sample_id),
    current_executable_price TEXT,
    copy_price_delta TEXT,
    raw_json TEXT NOT NULL,
    UNIQUE(session_id,trade_id)
);
CREATE INDEX IF NOT EXISTS idx_forensic_latency_events_wallet_time
    ON forensic_latency_events(wallet,api_first_seen_us);

CREATE TABLE IF NOT EXISTS forensic_tournaments (
    tournament_id TEXT PRIMARY KEY,
    created_at_us INTEGER NOT NULL,
    config_json TEXT NOT NULL,
    manifest_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallet_forward_sessions (
    session_id TEXT PRIMARY KEY,
    wallet TEXT NOT NULL,
    started_at_us INTEGER NOT NULL,
    ended_at_us INTEGER,
    state TEXT NOT NULL,
    config_json TEXT NOT NULL,
    source_stats_json TEXT NOT NULL,
    summary_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallet_forward_source_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES wallet_forward_sessions(session_id),
    economic_key TEXT NOT NULL,
    source TEXT NOT NULL,
    source_row_id TEXT NOT NULL,
    seen_at_us INTEGER NOT NULL,
    request_started_us INTEGER,
    rtt_us INTEGER,
    status_code INTEGER,
    raw_json TEXT NOT NULL,
    UNIQUE(session_id,economic_key,source,source_row_id)
);
CREATE INDEX IF NOT EXISTS idx_wallet_forward_source_seen
    ON wallet_forward_source_observations(session_id,seen_at_us);

CREATE TABLE IF NOT EXISTS wallet_forward_signals (
    signal_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES wallet_forward_sessions(session_id),
    economic_key TEXT NOT NULL,
    source TEXT NOT NULL,
    source_first_seen_us INTEGER NOT NULL,
    trade_ts_us INTEGER,
    slug TEXT,
    token_id TEXT,
    side TEXT,
    outcome TEXT,
    target_price TEXT,
    target_size TEXT,
    transaction_hash TEXT,
    publication_lag_lower_us INTEGER,
    publication_lag_upper_us INTEGER,
    detection_sample_id TEXT REFERENCES forensic_live_samples(sample_id),
    raw_json TEXT NOT NULL,
    UNIQUE(session_id,economic_key)
);
CREATE INDEX IF NOT EXISTS idx_wallet_forward_signals_time
    ON wallet_forward_signals(session_id,source_first_seen_us);

CREATE TABLE IF NOT EXISTS wallet_signal_strategy_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES wallet_forward_sessions(session_id),
    signal_id TEXT NOT NULL REFERENCES wallet_forward_signals(signal_id),
    strategy TEXT NOT NULL,
    decided_at_us INTEGER NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT,
    context_json TEXT NOT NULL,
    UNIQUE(session_id,signal_id,strategy)
);
CREATE INDEX IF NOT EXISTS idx_wallet_signal_strategy_decisions_session
    ON wallet_signal_strategy_decisions(session_id,strategy,decided_at_us);

-- Append-only causal event history. The legacy table retains only latest state.
CREATE TABLE IF NOT EXISTS wallet_strategy_decision_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    strategy TEXT NOT NULL,
    decided_at_us INTEGER NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT,
    context_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wallet_strategy_decision_events_session
    ON wallet_strategy_decision_events(session_id,strategy,decided_at_us);


CREATE TABLE IF NOT EXISTS wallet_forward_orders (
    order_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES wallet_forward_sessions(session_id),
    strategy TEXT NOT NULL,
    signal_id TEXT REFERENCES wallet_forward_signals(signal_id),
    slug TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    style TEXT NOT NULL,
    price_limit TEXT,
    shares TEXT NOT NULL,
    created_at_us INTEGER NOT NULL,
    scheduled_at_us INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    metadata_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wallet_forward_orders_session_strategy
    ON wallet_forward_orders(session_id,strategy,created_at_us);

CREATE TABLE IF NOT EXISTS wallet_forward_fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES wallet_forward_orders(order_id),
    session_id TEXT NOT NULL REFERENCES wallet_forward_sessions(session_id),
    strategy TEXT NOT NULL,
    slug TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL,
    fill_ts_us INTEGER NOT NULL,
    shares TEXT NOT NULL,
    price TEXT NOT NULL,
    notional TEXT NOT NULL,
    fee TEXT NOT NULL,
    liquidity_role TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wallet_forward_fills_session_strategy
    ON wallet_forward_fills(session_id,strategy,fill_ts_us);

CREATE TABLE IF NOT EXISTS wallet_forward_rounds (
    session_id TEXT NOT NULL REFERENCES wallet_forward_sessions(session_id),
    strategy TEXT NOT NULL,
    slug TEXT NOT NULL,
    round_start_us INTEGER NOT NULL,
    round_end_us INTEGER NOT NULL,
    up_shares TEXT NOT NULL,
    down_shares TEXT NOT NULL,
    up_cost TEXT NOT NULL,
    down_cost TEXT NOT NULL,
    sell_proceeds TEXT NOT NULL,
    fees TEXT NOT NULL,
    settled_side TEXT,
    settled_at_us INTEGER,
    pnl TEXT,
    details_json TEXT NOT NULL,
    PRIMARY KEY(session_id,strategy,slug)
);
CREATE INDEX IF NOT EXISTS idx_wallet_forward_rounds_session_strategy
    ON wallet_forward_rounds(session_id,strategy,round_start_us);

"""
