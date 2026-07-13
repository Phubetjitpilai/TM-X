-- mysql-init/init.sql
-- Auto-executed by MySQL Docker container on first startup.
-- How to run: docker compose up -d

CREATE DATABASE IF NOT EXISTS tmx_db;
USE tmx_db;

-- Drop in FK-safe order (children first, lookup tables last)
DROP TABLE IF EXISTS measurements;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS parts;
DROP TABLE IF EXISTS package_size;
DROP TABLE IF EXISTS operator;
DROP TABLE IF EXISTS owner;
DROP TABLE IF EXISTS vendor;
DROP TABLE IF EXISTS handler;
DROP TABLE IF EXISTS category;

-- ===== Lookup tables (created first — parts/measurements reference these) =====

CREATE TABLE operator (
  operator_id   INT AUTO_INCREMENT PRIMARY KEY,
  operator_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE owner (
  owner_id   INT AUTO_INCREMENT PRIMARY KEY,
  owner_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE vendor (
  vendor_id   INT AUTO_INCREMENT PRIMARY KEY,
  vendor_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE handler (
  handler_id   INT AUTO_INCREMENT PRIMARY KEY,
  handler_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE category (
  category_id   INT AUTO_INCREMENT PRIMARY KEY,
  category_name VARCHAR(100) NOT NULL UNIQUE
);

-- package_size เก็บ nominal X/Y + tolerance เดียวที่ใช้ร่วมกันทั้งสองแกน
-- และ template_name (โปรแกรมวัดที่ผูกกับขนาด package นี้)
CREATE TABLE package_size (
  package_size_id INT AUTO_INCREMENT PRIMARY KEY,
  package_size    VARCHAR(10) NOT NULL UNIQUE,
  nominal_x       FLOAT NOT NULL,
  nominal_y       FLOAT NOT NULL,
  upper_tol       FLOAT NOT NULL,
  lower_tol       FLOAT NOT NULL,
  template_name   VARCHAR(100)
);

-- ===== Core tables =====

CREATE TABLE parts (
  part_id          INT AUTO_INCREMENT PRIMARY KEY,
  number_alpl      INT UNIQUE,
  part_number      VARCHAR(50),
  handler_id       INT,
  description      TEXT,
  vendor_id        INT,
  po_number        BIGINT,
  category_id      INT,
  package_size_id  INT,
  owner_id         INT,
  FOREIGN KEY (package_size_id) REFERENCES package_size(package_size_id),
  FOREIGN KEY (category_id)     REFERENCES category(category_id),
  FOREIGN KEY (vendor_id)       REFERENCES vendor(vendor_id),
  FOREIGN KEY (handler_id)      REFERENCES handler(handler_id),
  FOREIGN KEY (owner_id)        REFERENCES owner(owner_id)
);

-- ON UPDATE CASCADE: ให้แก้ ALPL ใน parts ได้แม้จะมีประวัติ session/measurement
-- ผูกอยู่แล้ว (แก้ผ่าน edit.html ได้) — ค่า number_alpl ใน sessions/measurements
-- จะถูกอัปเดตตามอัตโนมัติ ไม่ใช่ถูก MySQL ปฏิเสธแบบ RESTRICT (default)
-- หมายเหตุ: sessions/measurements อ้างอิง parts ผ่าน number_alpl (ไม่ใช่ part_id)
-- เพราะ main.py ทั้งไฟล์ query/insert สองตารางนี้ด้วยคอลัมน์ number_alpl ตรงๆ
-- ทุกจุด (เช่น "INSERT INTO sessions (number_alpl, ...)") — number_alpl ใน
-- parts มี UNIQUE constraint จึงใช้เป็นเป้าหมายของ FOREIGN KEY ได้เหมือน PK
-- queue_state: สำเนา JSON ของคิว ALPL (session_queues ใน memory ของ backend)
-- เขียนทับทุกครั้งที่มีการเปลี่ยนแปลง ใช้กู้คืนคิวกลับเข้า memory ถ้า backend
-- restart กลาง session ที่ยัง running อยู่ (ดู migrate_add_reliability.sql)
CREATE TABLE sessions (
  session_id     INT          AUTO_INCREMENT PRIMARY KEY,
  number_alpl    INT          NOT NULL,
  state          VARCHAR(20)  NOT NULL DEFAULT 'idle',
  target_count   INT          NOT NULL DEFAULT 1,
  measured_count INT          NOT NULL DEFAULT 0,
  queue_state    JSON         NULL,
  last_seen      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  started_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ended_at       DATETIME     NULL,
  FOREIGN KEY (number_alpl) REFERENCES parts(number_alpl) ON UPDATE CASCADE
);

-- client_uuid: UUID ที่ Agent สร้างต่อการวัด 1 ครั้ง ใช้กัน insert ซ้ำตอน retry
-- POST /api/measurements (ดู migrate_add_reliability.sql สำหรับรายละเอียด)
CREATE TABLE measurements (
  measurement_id INT          AUTO_INCREMENT PRIMARY KEY,
  session_id     INT          NOT NULL,
  number_alpl    INT          NOT NULL,
  value_x        FLOAT        NOT NULL,
  value_y        FLOAT        NOT NULL,
  result         VARCHAR(10)  NOT NULL,
  note           TEXT,
  client_uuid    VARCHAR(36)  NULL UNIQUE,
  measure_type   VARCHAR(10)  NOT NULL,
  operator_id    INT          NOT NULL,
  image_path     VARCHAR(255),
  image_upload_failed TINYINT(1) NOT NULL DEFAULT 0,
  timestamp      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (session_id)  REFERENCES sessions(session_id),
  FOREIGN KEY (number_alpl) REFERENCES parts(number_alpl) ON UPDATE CASCADE,
  FOREIGN KEY (operator_id) REFERENCES operator(operator_id)
);