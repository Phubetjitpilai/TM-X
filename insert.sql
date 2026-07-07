USE tmx_db;

-- ── sessions.number_alpl → parts.number_alpl ────────────────────────────────
SET @cname := (
  SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE
  WHERE TABLE_SCHEMA = 'tmx_db' AND TABLE_NAME = 'sessions'
    AND COLUMN_NAME = 'number_alpl' AND REFERENCED_TABLE_NAME = 'parts'
  LIMIT 1
);
SET @drop_sql := CONCAT('ALTER TABLE sessions DROP FOREIGN KEY ', @cname);
PREPARE stmt FROM @drop_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE sessions
  ADD CONSTRAINT sessions_number_alpl_fk
  FOREIGN KEY (number_alpl) REFERENCES parts(number_alpl)
  ON UPDATE CASCADE;

-- ── measurements.number_alpl → parts.number_alpl ────────────────────────────
SET @cname2 := (
  SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE
  WHERE TABLE_SCHEMA = 'tmx_db' AND TABLE_NAME = 'measurements'
    AND COLUMN_NAME = 'number_alpl' AND REFERENCED_TABLE_NAME = 'parts'
  LIMIT 1
);
SET @drop_sql2 := CONCAT('ALTER TABLE measurements DROP FOREIGN KEY ', @cname2);
PREPARE stmt2 FROM @drop_sql2;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

ALTER TABLE measurements
  ADD CONSTRAINT measurements_number_alpl_fk
  FOREIGN KEY (number_alpl) REFERENCES parts(number_alpl)
  ON UPDATE CASCADE;

-- ── ตรวจสอบผล ────────────────────────────────────────────────────────────
SELECT TABLE_NAME, CONSTRAINT_NAME, UPDATE_RULE
FROM information_schema.REFERENTIAL_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = 'tmx_db' AND REFERENCED_TABLE_NAME = 'parts';