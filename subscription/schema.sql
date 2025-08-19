--schema.sql
-- Township-level manifest: drives “new file updated” checks
CREATE TABLE IF NOT EXISTS assessor_manifest (
  town_code         INTEGER PRIMARY KEY,
  pass              INTEGER NOT NULL,
  last_update       DATE    NOT NULL,
  head_pins_count   INTEGER NOT NULL,
  detail_pins_count INTEGER NOT NULL,
  layout_version    INTEGER NOT NULL,
  zip_sha256        TEXT    NOT NULL,
  imported_at       TIMESTAMPTZ DEFAULT now()
);

-- Add after table definitions
ALTER TABLE IF EXISTS assessor_header_v22
  ADD CONSTRAINT chk_header_keypin_14
  CHECK (key_pin ~ '^[0-9]{14}$');

ALTER TABLE IF EXISTS assessor_detail_v22
  ADD CONSTRAINT chk_detail_key_14
  CHECK (key_pin ~ '^[0-9]{14}$'),
  ADD CONSTRAINT chk_detail_pin_14
  CHECK (pin     ~ '^[0-9]{14}$');

-- Header: one row per KEY (Head) PIN (layout v22)
CREATE TABLE IF NOT EXISTS assessor_header_v22 (
  town_code INTEGER NOT NULL,
  key_pin   TEXT    NOT NULL,   -- normalized 14-digit
  class     TEXT,
  addr      TEXT,
  -- add more v22 columns as needed:
  raw_json  JSONB,              -- keep original row for safety
  PRIMARY KEY (key_pin)
);
CREATE INDEX IF NOT EXISTS idx_header_town ON assessor_header_v22 (town_code);

-- Detail: associations (key_pin -> associated pin)
CREATE TABLE IF NOT EXISTS assessor_detail_v22 (
  town_code INTEGER NOT NULL,
  key_pin   TEXT    NOT NULL,
  pin       TEXT    NOT NULL,   -- associated pin (normalized)
  unit_no   TEXT,
  raw_json  JSONB,
  PRIMARY KEY (pin)             -- direct lookup by any PIN is fast
);
CREATE INDEX IF NOT EXISTS idx_detail_town_key ON assessor_detail_v22 (town_code, key_pin);

