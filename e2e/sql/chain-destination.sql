CREATE TABLE dst.events_v3
(
    id UInt64,
    name String,
    amount UInt32,
    event_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY id
