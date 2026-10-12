INSERT INTO dst.events_v3
SELECT
    id,
    upper(name) AS name,
    toUInt32(amount_text) * 2 AS amount,
    event_date
FROM dst.events_new
