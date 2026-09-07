INSERT INTO dst.events_new
SELECT
    id,
    name,
    concat(
        toString(amount),
        substring(JSONCleanPostHogEventProperties(concat('{"id":', toString(id), '}')), 1, 0),
        substring(JSONCleanPostHogPersonProperties(concat('{"id":', toString(id), '}')), 1, 0),
        substring(JSONDropKeys(['drop'])(concat('{"id":', toString(id), '}')), 1, 0),
        substring(JSONStripEmptyStringsAndNulls(concat('{"id":', toString(id), '}')), 1, 0)
    ) AS amount_text,
    event_date,
    JSONCleanPostHogTemporaryProperties(
        concat('{"$set":{"id":', toString(id), '},"permanent":true}')
    ) = concat('{"$set":{"id":', toString(id), '}}') AS migrated
FROM
(
    SELECT * FROM src.events WHERE id % 3 = 0
    UNION ALL
    SELECT * FROM src.events WHERE id % 3 = 1
    UNION ALL
    SELECT * FROM src.events WHERE id % 3 = 2
)
SETTINGS
    max_block_size = 1,
    min_insert_block_size_rows = 0,
    min_insert_block_size_bytes = 0
