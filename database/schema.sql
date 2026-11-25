-- =============================================================================
-- Real-Time E-Commerce Data Pipeline - PostgreSQL Schema
-- =============================================================================

-- 1. Curated, validated, and deduplicated e-commerce events
CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id            VARCHAR(64) PRIMARY KEY,
    event_type          VARCHAR(32) NOT NULL,
    event_time          TIMESTAMPTZ NOT NULL,
    user_id             VARCHAR(32) NOT NULL,
    session_id          VARCHAR(64),
    product_id          VARCHAR(32) NOT NULL,
    product_name        VARCHAR(255) NOT NULL,
    category            VARCHAR(100) NOT NULL,
    quantity            INTEGER NOT NULL,
    unit_price          NUMERIC(12, 2) NOT NULL,
    total_amount        NUMERIC(12, 2) NOT NULL,
    recognized_revenue  NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    currency            VARCHAR(8) NOT NULL DEFAULT 'USD',
    order_id            VARCHAR(64),
    payment_status      VARCHAR(32),
    device_type         VARCHAR(32),
    country             VARCHAR(16),
    is_payment_event    BOOLEAN NOT NULL DEFAULT FALSE,
    is_order_event      BOOLEAN NOT NULL DEFAULT FALSE,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_event_time ON ecommerce_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON ecommerce_events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_product_id ON ecommerce_events (product_id);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON ecommerce_events (user_id);

-- 2. Event-time windowed overall KPIs (upserted idempotently per window)
CREATE TABLE IF NOT EXISTS windowed_metrics (
    window_start         TIMESTAMPTZ PRIMARY KEY,
    window_end           TIMESTAMPTZ NOT NULL,
    total_events         BIGINT NOT NULL,
    total_orders         BIGINT NOT NULL,
    successful_payments  BIGINT NOT NULL,
    cancelled_orders     BIGINT NOT NULL,
    total_revenue        NUMERIC(14, 2) NOT NULL,
    active_users         BIGINT NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_windowed_metrics_end ON windowed_metrics (window_end DESC);

-- 3. Event-time windowed product-level performance
CREATE TABLE IF NOT EXISTS product_window_metrics (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    product_id        VARCHAR(32) NOT NULL,
    product_name      VARCHAR(255) NOT NULL,
    category          VARCHAR(100) NOT NULL,
    view_count        BIGINT NOT NULL,
    cart_add_count    BIGINT NOT NULL,
    order_count       BIGINT NOT NULL,
    units_sold        BIGINT NOT NULL,
    product_revenue   NUMERIC(14, 2) NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, product_id)
);

CREATE INDEX IF NOT EXISTS idx_product_metrics_product ON product_window_metrics (product_id);

-- 4. Event-time windowed event-type distribution
CREATE TABLE IF NOT EXISTS event_type_window_metrics (
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    event_type          VARCHAR(32) NOT NULL,
    event_count         BIGINT NOT NULL,
    recognized_revenue  NUMERIC(14, 2) NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, event_type)
);

-- 5. Dead-letter queue audit log for invalid/malformed records
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id                SERIAL PRIMARY KEY,
    raw_key           TEXT,
    raw_value         TEXT,
    validation_error  VARCHAR(64) NOT NULL,
    kafka_topic       VARCHAR(128),
    kafka_partition   INTEGER,
    kafka_offset      BIGINT,
    failed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dlq_kafka_coords
    ON dead_letter_events (kafka_topic, kafka_partition, kafka_offset)
    WHERE kafka_topic IS NOT NULL;
