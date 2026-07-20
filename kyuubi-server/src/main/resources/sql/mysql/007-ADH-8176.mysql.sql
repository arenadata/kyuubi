SELECT '< ADH-8176: Spark Connect HA - shared token store' AS ' ';

CREATE TABLE IF NOT EXISTS spark_connect_tokens(
    token_id   VARCHAR(36)  NOT NULL COMMENT 'the UUID token',
    username   VARCHAR(255) NOT NULL COMMENT 'the authenticated username',
    created_at BIGINT       NOT NULL COMMENT 'token creation time in ms',
    expires_at BIGINT       NOT NULL COMMENT 'token expiry time in ms',
    PRIMARY KEY (token_id),
    INDEX sc_token_expires_idx(expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
