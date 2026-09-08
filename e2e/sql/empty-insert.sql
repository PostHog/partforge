INSERT INTO dst.events_new (id, name, amount_text, event_date, migrated)
SELECT id, name, toString(amount), event_date, 1 FROM src.events WHERE 0
