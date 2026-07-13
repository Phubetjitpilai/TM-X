-- seed_lookup_data.sql
-- Insert ข้อมูลตั้งต้นให้ตาราง lookup (operator/owner/handler/vendor/category/package_size)
-- ใช้ INSERT IGNORE เพื่อให้รันซ้ำได้โดยไม่ error (ชื่อซ้ำจะถูกข้าม เพราะทุกตารางมี UNIQUE
-- อยู่แล้วบนคอลัมน์ชื่อ — ดู init.sql)

USE tmx_db;

-- "3.255x3.255" ยาว 11 ตัวอักษร เกิน VARCHAR(10) เดิมของ package_size.package_size
-- (ตัวอื่นทั้งหมดยาวไม่เกิน 9) ขยายคอลัมน์เป็น VARCHAR(20) ก่อน insert กันข้อมูลถูกตัด/error
ALTER TABLE package_size MODIFY package_size VARCHAR(20) NOT NULL UNIQUE;

-- ── Operator ─────────────────────────────────────────────────────────────
INSERT IGNORE INTO operator (operator_name) VALUES
  ('Ball'),
  ('Bon');

-- ── Owner ────────────────────────────────────────────────────────────────
INSERT IGNORE INTO owner (owner_name) VALUES
  ('Messi'),
  ('Ronaldo');

-- ── Handler ──────────────────────────────────────────────────────────────
INSERT IGNORE INTO handler (handler_name) VALUES
  ('HT9046'),
  ('HT1028C');

-- ── Vendor ───────────────────────────────────────────────────────────────
INSERT IGNORE INTO vendor (vendor_name) VALUES
  ('A'),
  ('B'),
  ('C');

-- ── Category ─────────────────────────────────────────────────────────────
INSERT IGNORE INTO category (category_name) VALUES
  ('ALPL'),
  ('Lead breaker');

-- ── Package Size ─────────────────────────────────────────────────────────
-- upper_tol = 0.005 และ lower_tol = 0 ทุกตัวตามที่ระบุ, template_name = '201' ทุกตัว
INSERT IGNORE INTO package_size (package_size, nominal_x, nominal_y, upper_tol, lower_tol, template_name) VALUES
  ('10x6.5',      10,    6.5,   0.005, 0, '201'),
  ('3.05x7.25',   3.05,  7.25,  0.005, 0, '201'),
  ('3.255x3.255', 3.255, 3.255, 0.005, 0, '201'),
  ('3.25x7.40',   3.25,  7.40,  0.005, 0, '201'),
  ('3.5x3.75',    3.5,   3.75,  0.005, 0, '201'),
  ('3.5x3',       3.5,   3,     0.005, 0, '201'),
  ('3.5x4.6',     3.5,   4.6,   0.005, 0, '201'),
  ('3x2.5',       3,     2.5,   0.005, 0, '201'),
  ('3x3',         3,     3,     0.005, 0, '201'),
  ('3x4',         3,     4,     0.005, 0, '201'),
  ('4.25x4.25',   4.25,  4.25,  0.005, 0, '201'),
  ('4.5x5.75',    4.5,   5.75,  0.005, 0, '201'),
  ('4x4',         4,     4,     0.005, 0, '201'),
  ('4x5',         4,     5,     0.005, 0, '201'),
  ('5.16x5.16',   5.16,  5.16,  0.005, 0, '201'),
  ('5x5',         5,     5,     0.005, 0, '201'),
  ('6.55x4.3',    6.55,  4.3,   0.005, 0, '201'),
  ('6x6',         6,     6,     0.005, 0, '201'),
  ('7x7',         7,     7,     0.005, 0, '201'),
  ('8x8',         8,     8,     0.005, 0, '201'),
  ('9x15',        9,     15,    0.005, 0, '201'),
  ('9x9',         9,     9,     0.005, 0, '201');