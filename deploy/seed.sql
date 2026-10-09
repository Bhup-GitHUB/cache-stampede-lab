CREATE TABLE IF NOT EXISTS items (id bigint PRIMARY KEY, payload text NOT NULL);
INSERT INTO items SELECT i, 'record-' || i FROM generate_series(1,10000) i ON CONFLICT DO NOTHING;
