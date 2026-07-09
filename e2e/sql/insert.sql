INSERT INTO dst.events_new
SELECT
    id,
    name,
    concat(toString(amount), substring(JSONCleanPostHogEventProperties(concat('{"id":', toString(id), '}')), 1, 0)) AS amount_text,
    event_date,
    1 AS migrated
FROM src.events
