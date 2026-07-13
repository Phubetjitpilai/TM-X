# TM-X Measurement System

ระบบอัตโนมัติสำหรับตรวจวัดมิติ (Dimensional Inspection) ของชิ้นงาน ALPL (IPM) ที่ใช้จัดแนว IC Lead กับ Contact Pin บนเครื่องทดสอบ 4 แพลตฟอร์ม (HT9046, HT1028C, MT9510, DE8880) โดยมาแทนที่กระบวนการแมนนวลที่ทำผ่าน KEYENCE TM-X5065 Vision Controller เดิม

ความคลาดเคลื่อนที่ยอมรับได้ (Tolerance) อยู่ในระดับ ±0.01–0.02 mm

โปรเจกต์นี้เป็นทั้งระบบใช้งานจริงในห้อง PM Kit ของ Analog Devices (ADI) Thailand และเป็นผลงาน Capstone ของนักศึกษาฝึกงาน

---

## สถาปัตยกรรมระบบ

```
┌──────────────┐   HTTP (REST)    ┌──────────────────┐   SSE (server→client)   ┌──────────────┐
│   Frontend   │ ───────────────▶ │  Backend-server    │ ─────────────────────▶ │  Operator     │
│ (index.html) │ ◀─── SSE ─────── │     (FastAPI)      │                         │  Browser      │
└──────────────┘                  └──────────────────┘
                                          │   │
                                  MySQL   │   │  MinIO (presigned URL)
                                  (data)  │   │  (images)
                                          ▼   ▼
                                   ┌──────────────┐
                                   │   MySQL DB   │   ┌────────────┐
                                   │  + MinIO     │   │   MinIO    │
                                   └──────────────┘   └────────────┘
                                          ▲
                                          │ HTTP (POST /api/measurements, heartbeat)
                                          │
                                   ┌──────────────────────┐   TCP / FTP (mock ปัจจุบัน)   ┌────────────────┐
                                   │ Backend-pc_station/    │ ─────────────────────────▶ │ KEYENCE TM-X5065│
                                   │     agent.py            │ ◀───────────────────────── │  + MCU (Serial) │
                                   └──────────────────────┘                              └────────────────┘
```

- **Frontend → Backend**: HTTP request ปกติ (POST /api/session/start, /api/session/stop ฯลฯ)
- **Backend → Frontend**: Server-Sent Events (SSE) ทางเดียว ผ่าน `/api/stream`
- **Backend ↔ Agent**: HTTP — Backend สั่ง Agent ผ่าน `POST /command` (action: start/stop), Agent ส่งค่าที่วัดได้กลับผ่าน `POST /api/measurements` และ heartbeat ผ่าน `POST /api/heartbeat`
- **Agent ↔ TM-X Controller**: TCP (สั่งวัด/ขอค่า) + FTP (ดึงรูปภาพ) — **ปัจจุบันยังไม่ได้ต่อจริง รันอยู่ใน Mock Mode**
- **Backend → MinIO**: ใช้ presigned URL ให้ Agent อัปโหลด/Dashboard ดูรูปโดยตรง ไม่ผ่าน Backend

---

## โครงสร้างโปรเจกต์

```
TM-X_Project/
├── Backend-server/              # FastAPI backend (Single Source of Truth)
│   ├── main.py
│   └── requirements.txt
├── Backend-pc_station/          # Agent ที่รันอยู่บน PC หน้างาน คุยกับ TM-X/MCU
│   └── agent.py
├── Frontend/                    # Web Dashboard สำหรับ Operator (Vanilla JS)
│   ├── index.html               # หน้าหลัก: Live Telemetry, Part Entry, Camera Preview
│   ├── edit.html                # Database Editor (Parts/Measurements CRUD)
│   └── export.html              # หน้า Export ข้อมูล (CSV)
├── mysql-init/
│   └── init.sql                 # Schema เริ่มต้น (รันอัตโนมัติตอน MySQL container start ครั้งแรก)
├── image_ALPL/                  # แหล่งภาพอ้างอิงของแต่ละ ALPL (ใช้เทียบ/แสดงผล)
├── Store_image_temporary/       # โฟลเดอร์พักภาพที่ Agent อ่าน ก่อน upload ขึ้น MinIO แล้วลบทิ้ง
├── TM-X_simulation/             # โปรแกรมจำลอง TM-X Controller (สำหรับเทสต์ตอนยังไม่มีฮาร์ดแวร์จริง)
│   ├── tm-x.py
│   └── requirements.txt
├── docker-compose.yml           # MySQL + MinIO containers
└── .env                         # Config กลาง (DB, MinIO, Agent, TM-X, FTP, Heartbeat)
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Backend | Python 3.11, FastAPI, Pydantic, httpx, pymysql, pandas, sse-starlette, python-dotenv |
| Database | MySQL (ผ่าน Docker) |
| Image Storage | MinIO (S3-compatible, ผ่าน presigned URL) |
| Frontend | Vanilla JavaScript, HTML, CSS (ไม่ใช้ Framework, single-file ต่อหน้า) |
| Realtime | Server-Sent Events (SSE) |
| Hardware (เป้าหมาย ยังไม่ต่อจริง) | KEYENCE TM-X5065 (TCP), FTP สำหรับภาพ, MCU ผ่าน Serial |
| Infra | Docker Compose |
| Reporting (แยกจากระบบนี้) | Power BI (DAX, ตาราง `combined_3_fixed`) |

**สำคัญ**: ต้องใช้ Python 3.11 เท่านั้น (3.14 ใช้ไม่ได้กับ dependency บางตัว)

---

## Database Schema (`mysql-init/init.sql`)

3 ตาราง ความสัมพันธ์: `parts (1) → (N) sessions (1) → (N) measurements`

- **`parts`**: PK = `number_alpl` (1 ALPL = 1 vendor/handler/package เสมอ) เก็บ nominal และ tolerance แยกราย "แกน" (`upper_tol_x`, `lower_tol_x`, `upper_tol_y`, `lower_tol_y`) ไม่ใช่ tolerance ตัวเดียวใช้ร่วมกัน
- **`sessions`**: 1 รอบการวัด state = `idle | running | stopped | timeout`, มี `target_count`/`measured_count` ใช้เช็คว่าวัดครบหรือยัง
- **`measurements`**: ผลวัดแต่ละชิ้น มี `value_x`, `value_y`, `result` (OK/NG), `measure_type` (IPM/New), `image_path` (object key ใน MinIO)
  - หมายเหตุ: column ชื่อ **`Oparetor`** สะกดผิดโดยตั้งใจ/คงไว้ตามของเดิม ห้ามแก้ schema เป็น `Operator` โดยไม่คุยกันก่อน เพราะโค้ดทุกจุดอ้างอิงชื่อนี้อยู่

---

## Backend (`Backend-server/main.py`)

รันด้วย:
```bash
cd Backend-server
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

หลักการออกแบบที่สำคัญ (ต้องเข้าใจก่อนแก้โค้ด):

1. **FastAPI คือ Single Source of Truth** — สถานะทั้งหมด (session, measurement) อยู่ที่ backend เท่านั้น Agent ไม่เขียนลง DB ตรงๆ เลย
2. **`session_queues` (in-memory dict)** — เก็บคิว ALPL + ตำแหน่งปัจจุบันของแต่ละ session (โหมด IPM/New) เพราะ schema ตาราง `sessions` เก็บแค่ `number_alpl` ตัวเดียว (ALPL แรกของคิว)
   - **ความเสี่ยงที่รู้อยู่แล้ว**: ถ้า server restart กลางที่ session กำลัง running คิวนี้จะหาย ยังไม่มีการ persist ลง DB
3. **Stop flow ไม่สมมาตร (asymmetry)** — กด Stop จากเว็บ จะอัปเดต DB (`state='stopped'`) + แจ้ง Agent แต่ปุ่ม Stop ทางกายภาพที่ MCU ตอนนี้แค่ flip flag ใน memory ฝั่ง Agent เท่านั้น **ไม่ได้อัปเดต DB** — เป็น gap สถาปัตยกรรมที่ต้อง flag ไว้เวลา present งาน
4. **SSE เป็นทางเดียว** server → client เท่านั้น (`/api/stream`) ฝั่ง frontend ยังคงใช้ HTTP POST ปกติในการส่งคำสั่งไป backend (ไม่ใช่ bidirectional)
5. **MeasurementType: IPM vs New**
   - `IPM`: ALPL ลงทะเบียนไว้แล้วใน `parts` → query หา `template_name` อย่างเดียว ไม่ insert part ใหม่
   - `New`: ลงทะเบียน part ใหม่ + วัดในรอบเดียว → ต้อง insert `parts` ก่อน insert `sessions` เสมอ (เพราะมี FOREIGN KEY)
6. **Agent ไม่รู้ว่ากำลังวัด ALPL ตัวไหนในคิว** — แค่ส่ง `value_x`/`value_y` มาเรื่อยๆ Backend เป็นคนจับคู่ ALPL จากตำแหน่งใน `session_queues` เอง (ยกเว้น manual session แบบเก่าที่ไม่มี entry ใน `session_queues` จะใช้ `req.number_alpl` ที่ Agent ส่งมาตรงๆ)
7. **Heartbeat checker** (background task) — ตรวจทุก `HEARTBEAT_INTERVAL` วินาที ถ้า session ไหน `state='running'` แต่ `last_seen` เก่ากว่า `HEARTBEAT_TIMEOUT` → เปลี่ยนเป็น `timeout` และ broadcast ผ่าน SSE

### Endpoint หลัก
- `GET /api/stream` — SSE stream
- `GET /api/session/state`, `POST /api/session/start`, `POST /api/session/stop`
- `POST /api/heartbeat`
- `GET/POST/PATCH/DELETE /api/parts`, `/api/parts/{id}`
- `GET/POST /api/measurements`, `PATCH /api/measurements/{id}/image`, `DELETE /api/measurements/{id}`
- `POST /api/upload-url`, `GET /api/image-url/{measurement_id}` — presigned URL ของ MinIO
- `GET /api/export/csv` — export พร้อม filter (ใช้ filter ชุดเดียวกับ `list_measurements`)

---

## Agent (`Backend-pc_station/agent.py`)

รันด้วย:
```bash
cd Backend-pc_station
pip install -r requirements.txt
python agent.py
```

- **รันอยู่ใน Mock Mode ถาวร (ตอนนี้)** — `mock_measurement_flow()` generate `value_x`/`value_y` แบบสุ่มแทนการรอ TM-X/MCU จริง แทนที่ flow เดิมไปเลย ไม่ได้ทำเป็น toggle flag
- ฟังก์ชันสื่อสารกับฮาร์ดแวร์จริง (`_ensure_tcp`, `tcp_write`, `tcp_readline`, `_init_serial`, `send_serial`, `serial_reader_loop`) **ยังเก็บไว้ครบ** เผื่อย้อนกลับไปต่อฮาร์ดแวร์จริงในอนาคต แต่ตอนนี้ไม่ได้ถูกเรียกใช้จาก flow หลัก
- รับคำสั่งจาก Backend ผ่าน HTTP server ของตัวเอง (`POST /command`, action: `start`/`stop`) — ฟัง port ตาม `AGENT_PORT`
- ส่ง heartbeat กลับ backend ทุก `HEARTBEAT_INTERVAL` วินาที พร้อม `session_id` ปัจจุบัน
- รูปภาพ: อ่านจาก `Store_image_temporary/` → ขอ presigned URL จาก backend → PUT ตรงเข้า MinIO → patch `image_path` กลับ backend → ลบไฟล์ทิ้งหลังจบ session
- `/simulate/object-ready` — endpoint ไว้ test ตอนยังไม่มีฮาร์ดแวร์ (ตอนนี้ไม่มีความหมายแล้วเพราะ mock flow ไม่รอสัญญาณนี้)

---

## Frontend (`Frontend/`)

Single-file vanilla JS ทุกหน้า ธีมสว่าง (light theme)

- **`index.html`** — Dashboard หลัก: Live Telemetry (รับผ่าน SSE), Part Entry modal (เลือกโหมด IPM/New แล้วกด Start), Camera Preview, System Diagnostics, Stats card (scope เฉพาะ session ปัจจุบัน)
- **`edit.html`** — Database Editor จัดการตาราง Parts/Measurements (Add/Edit/Delete ผ่าน modal forms)
- **`export.html`** — หน้า export ข้อมูลเป็น CSV

---

## Environment Variables (`.env`)

| ตัวแปร | ใช้ที่ | คำอธิบาย |
|---|---|---|
| `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_PORT` | Backend | การเชื่อมต่อ MySQL |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Backend, Agent | การเชื่อมต่อ MinIO |
| `BACKEND_URL` | Agent | URL ของ Backend (สำหรับ Agent ยิง measurement/heartbeat กลับ) |
| `AGENT_PORT` | Backend, Agent | Port ที่ Agent HTTP server ฟังอยู่ |
| `SERIAL_PORT`, `SERIAL_BAUD` | Agent | การเชื่อมต่อ MCU ผ่าน Serial (ยังไม่ใช้จริง) |
| `TMX_HOST`, `TMX_PORT` | Agent | TCP ของ TM-X Controller (ยังไม่ใช้จริง) |
| `TMX_FTP_HOST/PORT/USER/PASS/DIR` | Agent | FTP ของ TM-X สำหรับดึงรูป (ยังไม่ใช้จริง) |
| `TEMP_IMAGE_DIR`, `IMAGE_SOURCE_DIR` | Agent | โฟลเดอร์พักภาพ/ภาพอ้างอิง |
| `HEARTBEAT_INTERVAL`, `HEARTBEAT_TIMEOUT` | Backend, Agent | ความถี่ heartbeat / timeout threshold |

> **อย่า commit `.env` จริงขึ้น repo** — ไฟล์ตัวอย่างควรเป็น `.env.example` ที่ไม่มีรหัสผ่านจริง

---

## วิธีรันทั้งระบบ (Local Dev)

```bash
# 1. เริ่ม MySQL + MinIO
docker compose up -d

# 2. รัน Backend (terminal ที่ 1)
cd Backend-server
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# 3. รัน Agent (terminal ที่ 2)
cd Backend-pc_station
pip install -r requirements.txt
python agent.py

# 4. เปิด Frontend
# เปิดไฟล์ Frontend/index.html ผ่าน browser โดยตรง หรือเสิร์ฟผ่าน static server
```

ตรวจ Python version ก่อนเสมอ: ต้องเป็น **3.11**

---

## Known Issues / งานที่เหลือ

- [ ] `session_queues` ไม่ persist ลง DB — หายเมื่อ server restart กลาง session
- [ ] Stop flow ไม่สมมาตร — ปุ่มกายภาพไม่อัปเดต DB เหมือนปุ่มบนเว็บ
- [ ] `edit.html` ยังเป็น frontend-only mockup (มี `console.log` payload ไว้ ยังไม่เรียก API จริง) — มี TODO marker ค้างอยู่
- [ ] ยังไม่ได้ต่อกลับ TM-X/MCU จริง (`agent.py` รันอยู่ใน Mock Mode ถาวร)
- [ ] db-editor topbar navigation links (Home, Edit, Export) ยังไม่ครบ
- [ ] Power BI dashboard (`combined_3_fixed`) พัฒนาแยกขนานไปกับ Web Frontend

---

## Conventions สำหรับแก้โค้ด

- คอมเมนต์ในโค้ดเขียนเป็นภาษาไทย ให้คงสไตล์นี้ต่อเวลาแก้ไฟล์เดิม
- ห้ามแก้ชื่อ column `Oparetor` เป็น `Operator` โดยไม่ migrate และอัปเดตทุกจุดที่อ้างอิง
- Tolerance อยู่ใน schema แบบแยกแกน X/Y เสมอ (`upper_tol_x`, `lower_tol_x`, `upper_tol_y`, `lower_tol_y`) — ห้ามย่อกลับไปเป็น tolerance เดียวใช้ร่วม
- Frontend แต่ละหน้าเป็น single HTML file (ไม่ split เป็นหลาย JS/CSS file) — ให้คงรูปแบบนี้เวลาต่อเติม
- ก่อนแก้ตรรกะ session/queue ใน `main.py` ให้อ่าน docstring ภาษาไทยในฟังก์ชันที่เกี่ยวข้องก่อนเสมอ (อธิบายเหตุผลเชิงสถาปัตยกรรมไว้ละเอียด)