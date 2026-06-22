SELECT '< ADH-8176: Spark Connect HA - shared token store' AS ' ';

CREATE TABLE IF NOT EXISTS spark_connect_tokens(
    token_id   VARCHAR(36)  NOT NULL,
    username   VARCHAR(255) NOT NULL,
    created_at BIGINT       NOT NULL,
    expires_at BIGINT       NOT NULL,
    PRIMARY KEY (token_id)
);

COMMENT ON COLUMN spark_connect_tokens.token_id IS 'the UUID token';
COMMENT ON COLUMN spark_connect_tokens.username IS 'the authenticated username';
COMMENT ON COLUMN spark_connect_tokens.created_at IS 'token creation time in ms';
COMMENT ON COLUMN spark_connect_tokens.expires_at IS 'token expiry time in ms';

CREATE INDEX IF NOT EXISTS sc_token_expires_idx ON spark_connect_tokens(expires_at);
