CREATE TABLE IF NOT EXISTS playback_logs (
    playback_id BIGSERIAL PRIMARY KEY,
    isrc VARCHAR(12) NOT NULL,
    author VARCHAR(200),
    title VARCHAR(200),
    copyright_holder VARCHAR(200),
    station_id VARCHAR(40) NOT NULL,
    duration_seconds SMALLINT NOT NULL, --Preserve after metrics calculation for possible additional uses
    listener_count INTEGER NOT NULL, --Preserve after metrics calculation for possible additional uses
    listened_seconds BIGINT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS playback_logs_dedup_idx
    ON playback_logs (md5(isrc || station_id || timestamp::text));
