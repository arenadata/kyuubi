-- ADH-8176: Spark Connect HA - shared token store
CREATE TABLE IF NOT EXISTS spark_connect_tokens(
    token_id   VARCHAR(36)  NOT NULL,
    username   VARCHAR(255) NOT NULL,
    created_at BIGINT       NOT NULL,
    expires_at BIGINT       NOT NULL,
    PRIMARY KEY (token_id)
);

CREATE INDEX IF NOT EXISTS sc_token_expires_idx ON spark_connect_tokens(expires_at);
