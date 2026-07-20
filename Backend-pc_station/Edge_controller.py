# Backend-pc_station/agent_mock_up.py
# Mock Agent สำหรับทดสอบ flow ทั้งระบบโดยไม่ต้องมี TM-X/MCU จริงต่ออยู่
# How to run:
#   cd Backend-pc_station
#   pip install -r requirements.txt
#   python agent_mock_up.py
#
# หมายเหตุ (architecture ใหม่): เลิกใช้ MinIO แล้ว — โค้ดส่วนอัปโหลดรูป
# (presigned URL / upload_image เดิม) ถูกตัดออกทั้งหมด รูปภาพจะเก็บเป็นไฟล์
# ในโฟลเดอร์บนเครื่อง PC แทน (เช่น Store_image_temporary) แต่ดีไซน์การจัดเก็บ
# ถาวรยังไม่ fix — mock agent ตัวนี้จึง "ไม่แตะรูปเลย" วัดค่าอย่างเดียว
# ตอนดีไซน์รูปเสร็จค่อยกลับมาเติม logic ย้าย/แจ้ง path ผ่าน
# PATCH /api/measurements/{id}/image (endpoint ฝั่ง backend ยังอยู่ครบ)

import asyncio
import logging
import os
import random
import time
import uuid
from typing import Optional

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel, validator

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Config ────────────────────────────────────────────────────────────────────
BACKEND_URL    = os.getenv("BACKEND_URL",   "http://localhost:8000")
AGENT_PORT     = int(os.getenv("AGENT_PORT",    9998))
SERIAL_PORT    = os.getenv("SERIAL_PORT",   "COM3")
SERIAL_BAUD    = int(os.getenv("SERIAL_BAUD",   9600))
TMX_HOST       = os.getenv("TMX_HOST",      "127.0.0.1")
TMX_PORT       = int(os.getenv("TMX_PORT",      5000))
TEMP_IMAGE_DIR = os.getenv("TEMP_IMAGE_DIR", "./Store_image_temporary")
HB_INTERVAL    = int(os.getenv("HEARTBEAT_INTERVAL", 5))

# Resolve path relative to project root
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not os.path.isabs(TEMP_IMAGE_DIR):
    TEMP_IMAGE_DIR = os.path.join(_root, TEMP_IMAGE_DIR.lstrip("./"))

#logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [Agent] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Agent state (in-memory) ───────────────────────────────────────────────────
current_session_id:    Optional[int] = None
current_template_name: Optional[str] = None
current_target_count:  Optional[int] = None
current_number_alpl:   Optional[int] = None
is_running:            bool           = False
_state_lock       = asyncio.Lock()
_seen_images:  set = set()
_object_queue: asyncio.Queue = asyncio.Queue()

# ── TCP connection to TM-X ────────────────────────────────────────────────────
# หมายเหตุ: ฟังก์ชัน TCP/Serial ทั้งหมดด้านล่างนี้ (_ensure_tcp, tcp_write,
# tcp_readline, _init_serial, send_serial) ยังเก็บไว้เหมือนเดิมทุกอย่าง
# เผื่อย้อนกลับไปต่อ TM-X/MCU จริงในอนาคต — แต่ตอนนี้ "ไม่ได้ถูกเรียกใช้แล้ว"
# จาก flow หลัก (ดู start_flow ด้านล่างที่เปลี่ยนไปใช้ mock_measurement_flow
# แทน) เพราะตอนนี้ยังไม่มี TM-X/MCU จริงให้ต่อ
_tcp_reader: Optional[asyncio.StreamReader] = None
_tcp_writer: Optional[asyncio.StreamWriter] = None
_tcp_write_lock = asyncio.Lock()


async def _ensure_tcp() -> None:
    global _tcp_reader, _tcp_writer
    if _tcp_writer is None or _tcp_writer.is_closing():
        try:
            _tcp_reader, _tcp_writer = await asyncio.open_connection(TMX_HOST, TMX_PORT)
            log.info("TCP: Connected to TM-X at %s:%d", TMX_HOST, TMX_PORT)
        except OSError as exc:
            log.error("TCP connection failed: %s", exc)
            raise


async def tcp_write(cmd: str) -> None:
    async with _tcp_write_lock:
        await _ensure_tcp()
        _tcp_writer.write((cmd + "\n").encode())
        await _tcp_writer.drain()
    log.info("TCP >>>: %r", cmd)


async def tcp_readline() -> str:
    line = await _tcp_reader.readline()
    data = line.decode().strip()
    log.info("TCP <<<: %r", data)
    return data


# ── Serial port ───────────────────────────────────────────────────────────────
_serial_conn = None


def _init_serial() -> None:
    global _serial_conn
    try:
        import serial
        _serial_conn = serial.Serial(SERIAL_PORT, SERIAL_BAUD, timeout=1)
        log.info("Serial: Connected to %s at %d baud", SERIAL_PORT, SERIAL_BAUD)
    except Exception as exc:
        log.warning("Serial: Cannot open %s: %s — running without serial", SERIAL_PORT, exc)


def send_serial(cmd: str) -> None:
    if _serial_conn and _serial_conn.is_open:
        _serial_conn.write((cmd + "\n").encode())
        log.info("Serial >>>: %r", cmd)
    else:
        log.info("Serial (mock) >>>: %r", cmd)


# ── Image helpers (dormant — รอดีไซน์การจัดเก็บรูปแบบ local folder) ───────────
# _get_new_image_sync/get_new_image/_cleanup_temp_images เก็บไว้เผื่อกลับมาใช้
# ตอน implement การจัดเก็บรูปจริง — mock flow ด้านล่างไม่เรียกใช้แล้ว (เดิมใช้
# หารูปเพื่ออัปโหลดขึ้น MinIO ซึ่งเลิกใช้แล้ว) การตัดออกจาก flow ทำให้แต่ละ
# รอบวัดไม่ต้องรอ poll หารูปถึง 30 วิ ด้วย — เทสต์เร็วขึ้นมาก
def _get_new_image_sync() -> Optional[str]:
    """Poll Store_image_temporary for a new image file (not yet seen this session)."""
    if not os.path.isdir(TEMP_IMAGE_DIR):
        log.error("Image directory not found: %s", TEMP_IMAGE_DIR)
        return None

    deadline = time.time() + 30
    while time.time() < deadline:
        files = [
            f for f in os.listdir(TEMP_IMAGE_DIR)
            if f.lower().endswith(".jpg") and f not in _seen_images
        ]
        if files:
            newest = sorted(files)[-1]
            _seen_images.add(newest)
            full_path = os.path.join(TEMP_IMAGE_DIR, newest)
            log.info("Image found: %s", full_path)
            return full_path
        time.sleep(0.5)

    log.warning("No new image found in %s within 30 s", TEMP_IMAGE_DIR)
    return None


async def get_new_image() -> Optional[str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_new_image_sync)


def _cleanup_temp_images() -> None:
    global _seen_images
    if not os.path.isdir(TEMP_IMAGE_DIR):
        log.info("Cleanup: directory not found: %s — skipping", TEMP_IMAGE_DIR)
        return
    removed = 0
    for f in os.listdir(TEMP_IMAGE_DIR):
        path = os.path.join(TEMP_IMAGE_DIR, f)
        if os.path.isfile(path):
            try:
                os.remove(path)
                removed += 1
            except Exception as exc:
                log.warning("Cleanup: could not remove %s: %s", f, exc)
    _seen_images = set()
    log.info("Cleanup: removed %d file(s) from %s", removed, TEMP_IMAGE_DIR)


# ── Pydantic validation for measurement values ────────────────────────────────
class MeasurementData(BaseModel):
    value_x: float
    value_y: float

    @validator("value_x", "value_y")
    def must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Measurement value must be > 0")
        return v


# ── Mock measurement: gen ค่า value_x/value_y แบบสุ่ม แทนการรอ TM-X จริง ──────
def _generate_mock_measurement() -> "MeasurementData":
    """สุ่มค่า value_x/value_y แบบมั่วๆ ไว้ใช้ทดสอบ flow ทั้งระบบโดยไม่ต้องมี
    TM-X/MCU จริงต่ออยู่ — สุ่มในช่วงที่กว้างพอจะได้เห็นทั้งผล OK และ NG
    คละกันบ้าง (ไม่ได้ดูค่า tolerance จริงของ part เลย เพราะ Agent ไม่รู้
    และไม่จำเป็นต้องรู้ — backend เป็นคนตัดสิน OK/NG เองอยู่แล้ว)
    """
    return MeasurementData(value_x=round(random.uniform(1.0, 20.0), 3),
                            value_y=round(random.uniform(1.0, 20.0), 3))


async def mock_single_measurement(index: int) -> bool:
    """ทำ 1 รอบของการ "วัด" แบบ mock: gen ค่า → POST ไป backend (retry ได้ถึง
    3 ครั้งถ้าเจอปัญหาเชื่อมต่อ/backend ล่มชั่วคราว)

    ส่วนรูปภาพ: ตัดออกทั้งหมดแล้ว (เดิมหารูปจาก TEMP_IMAGE_DIR แล้วอัปโหลดขึ้น
    MinIO) — architecture ใหม่เก็บรูปใน local folder แต่ดีไซน์ยังไม่ fix
    TODO: เติม logic จัดเก็บรูป + PATCH /api/measurements/{id}/image เมื่อดีไซน์เสร็จ

    คืนค่า True ถ้าบันทึกลง backend สำเร็จ, False ถ้าล้มเหลวจนต้องข้ามชิ้นนี้ไป
    (mock_measurement_flow เอาไปนับว่าวัด "สำเร็จจริง" กี่ชิ้นจาก target_count —
    ไม่ใช่แค่ "ลองแล้ว" กี่ชิ้น เพื่อไม่ให้ข้อความสรุปท้าย flow โกหกว่าครบ)
    """
    global current_session_id, current_number_alpl

    m = _generate_mock_measurement()
    log.info("Mock measurement #%d: gen ค่า x=%.3f, y=%.3f", index, m.value_x, m.value_y)

    # client_uuid: สร้างครั้งเดียวต่อการวัดนี้ แล้วใช้ตัวเดิมซ้ำทุกครั้งที่ retry
    # ด้านล่าง — เป็น idempotency key ให้ backend เช็คได้ว่าเป็น request เดิม
    # ที่ retry มา ไม่ใช่การวัดครั้งใหม่ (กัน insert ซ้ำ/นับ measured_count ซ้ำ
    # ถ้ารอบก่อน backend บันทึกสำเร็จไปแล้วจริงๆ แต่ response หลุดหายกลับมาไม่ถึง
    # Agent — ดู main.py create_measurement ที่ dedup ด้วย client_uuid นี้อยู่แล้ว)
    client_uuid = str(uuid.uuid4())
    data = None
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{BACKEND_URL}/api/measurements",
                    json={
                        "session_id":  current_session_id,
                        "number_alpl": current_number_alpl,  # backend จะ override ด้วยค่าจากคิวเองอยู่แล้ว
                        "value_x":     m.value_x,
                        "value_y":     m.value_y,
                        "client_uuid": client_uuid,
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
            break
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status < 500:
                # 4xx = backend ปฏิเสธ request นี้ตรงๆ (เช่น session ไม่ได้
                # running แล้ว/timeout ไปแล้วจาก heartbeat_checker) — retry ซ้ำ
                # ไปก็ไม่มีทางสำเร็จ ยกเลิกทันที ไม่ต้องเสียเวลาลองอีก
                #
                # พิมพ์ response body (มักเป็น {"detail": "..."} จาก FastAPI
                # HTTPException) ออกมาด้วยเสมอ — เดิม log แค่ str(exc) ของ httpx
                # ซึ่งมีแค่ "Client error '400 Bad Request' for url '...'" ไม่มี
                # เหตุผลจริงจาก backend เลย ทำให้ debug ไม่ได้ว่า reject เพราะอะไร
                try:
                    detail = exc.response.json().get("detail", exc.response.text)
                except Exception:
                    detail = exc.response.text
                log.error("Mock measurement #%d: backend ปฏิเสธ (HTTP %d): %s", index, status, detail)
                print(f"❌ Backend ปฏิเสธ (HTTP {status}): {detail}")
                break
            log.warning("Mock measurement #%d: POST attempt %d/3 failed (HTTP %d): %s",
                        index, attempt, status, exc)
        except Exception as exc:
            log.warning("Mock measurement #%d: POST attempt %d/3 failed: %s", index, attempt, exc)
        if attempt < 3:
            await asyncio.sleep(2)

    if data is None:
        # แจ้งเตือนให้เห็นชัดๆ ที่ terminal ทันที (เหมือน print "✅ ได้รับคำสั่ง
        # Start"/"✅ Done" ที่มีอยู่แล้ว) ไม่ใช่แค่ log เฉยๆ เพราะ operator ที่
        # เฝ้าหน้าจออยู่ควรรู้ทันทีว่าชิ้นนี้ข้อมูลหาย ต้องไปจัดการเอง
        print(f"❌ ชิ้นที่ {index}/{current_target_count}: บันทึกไม่สำเร็จ "
              f"(x={m.value_x:.3f}, y={m.value_y:.3f}) — ข้ามไปชิ้นถัดไป")
        log.error(
            "Mock measurement #%d: POST /api/measurements ล้มเหลวหลังลองครบ 3 ครั้ง "
            "(หรือถูกปฏิเสธ) — ค่า x=%.3f, y=%.3f ของชิ้นนี้ไม่ถูกบันทึก",
            index, m.value_x, m.value_y,
        )
        return False

    measurement_id = data["measurement_id"]
    log.info(
        "Mock measurement #%d: measurement_id=%d ALPL=%s result=%s measured=%d/%d status=%s",
        index, measurement_id, data.get("number_alpl"), data["result"],
        data["measured"], data["target"], data["status"],
    )

    return True


async def mock_measurement_flow() -> None:
    """แทนที่ flow เดิมทั้งหมด (LOAD_TEMPLATE ผ่าน TCP, รอ TEMPLATE_OK, ส่ง
    START_CMD ผ่าน Serial, รอ OBJECT_READY จาก MCU ทีละครั้ง) ด้วย loop ที่
    gen ค่า value_x/value_y มั่วๆ เอง แล้วยิงเข้า backend ตาม target_count
    ครั้ง ไม่ต้องมี TM-X/MCU จริงต่ออยู่เลย

    Print "ได้รับคำสั่ง Start" + template_name ก่อนเริ่ม แล้ว print "Done"
    ตอนจบครบทุกตัวตามที่ตกลงกันไว้
    """
    global is_running

    print(f"✅ ได้รับคำสั่ง Start — session_id={current_session_id}, template_name={current_template_name!r}")
    log.info("Mock start: session=%s template=%r target_count=%s",
              current_session_id, current_template_name, current_target_count)

    # นับ "สำเร็จจริง" แยกจาก "ลองแล้วกี่รอบ" — ถ้าบางชิ้น POST ล้มเหลวถาวร
    # (ดู mock_single_measurement) จะข้ามไปชิ้นถัดไปแทนที่จะหยุดทั้ง session
    # แต่ต้องรู้ตัวเลขจริงตอนสรุปท้าย flow ไม่ใช่โกหกว่า "วัดครบ N ชิ้นแล้ว"
    # ทั้งที่จริงมีบางชิ้นข้อมูลหายไป
    succeeded = 0
    target = current_target_count or 0
    for i in range(1, target + 1):
        if not is_running:
            log.warning("Mock flow: ถูกสั่ง stop กลางทาง (รอบที่ %d/%d) — หยุดเลย", i, current_target_count)
            return
        if await mock_single_measurement(i):
            succeeded += 1

    async with _state_lock:
        is_running = False

    if succeeded == target:
        print(f"✅ Done — วัดครบ {target} ชิ้นแล้ว (session_id={current_session_id})")
    else:
        print(f"⚠️ Done — วัดสำเร็จ {succeeded}/{target} ชิ้น "
              f"({target - succeeded} ชิ้นบันทึกไม่สำเร็จ ดู log ด้านบน) "
              f"(session_id={current_session_id})")
    log.info("Mock flow done: session=%s สำเร็จ %d/%d", current_session_id, succeeded, target)


# ── Object-ready consumer loop ────────────────────────────────────────────────
# หมายเหตุ: loop นี้ (กับ serial_reader_loop ด้านล่าง) ยังรันอยู่เหมือนเดิม
# แต่ตอนนี้ "ไม่มีอะไรมา put เข้า _object_queue แล้ว" เพราะ mock_measurement_flow
# ไม่ได้รอสัญญาณจาก Serial อีกต่อไป — เก็บไว้เผื่อย้อนกลับไปใช้ของจริง
async def object_ready_consumer() -> None:
    """Single consumer — processes OBJECT_READY one at a time from the queue."""
    log.info("Object-ready consumer started")
    while True:
        await _object_queue.get()
        if not is_running or current_session_id is None:
            log.warning("Consumer: dequeued signal but no active session — discarding")
            _object_queue.task_done()
            continue
        qsize = _object_queue.qsize()
        if qsize:
            log.debug("Consumer: queue has %d more signal(s) pending", qsize)
        _object_queue.task_done()


# ── Start / Stop flows ────────────────────────────────────────────────────────
async def start_flow() -> None:
    """เดิมฟังก์ชันนี้คุยกับ TM-X จริงผ่าน TCP (LOAD_TEMPLATE) แล้วส่ง
    START_CMD ผ่าน Serial ไปที่ MCU — ตอนนี้แทนที่ด้วย mock_measurement_flow()
    ไปเลยตามที่ตกลงกันไว้ (ไม่ต้องมี TM-X/MCU จริงต่ออยู่) ฟังก์ชัน tcp_write/
    tcp_readline/send_serial ด้านบนยังเก็บไว้เผื่อย้อนกลับมาใช้ทีหลัง
    """
    await mock_measurement_flow()


async def stop_flow() -> None:
    global is_running
    log.info("Stop flow: หยุด mock measurement flow")
    async with _state_lock:
        is_running = False


# ── Serial reader loop ────────────────────────────────────────────────────────
# หมายเหตุ: เก็บไว้เหมือนเดิม ไม่ได้ใช้งานจริงตอนนี้ (ไม่มี Serial connection
# เพราะ _init_serial จะ warn แล้วไม่ตั้งค่า _serial_conn ถ้าไม่มีพอร์ตจริง)
async def serial_reader_loop() -> None:
    if _serial_conn is None:
        log.info("Serial: reader loop disabled (no port)")
        return

    loop = asyncio.get_event_loop()
    log.info("Serial: reader loop started")
    while True:
        try:
            raw = await loop.run_in_executor(None, _serial_conn.readline)
            line = raw.decode("utf-8", errors="ignore").strip()
            if not line:
                continue
            log.info("Serial <<<: %r", line)

            if line == "OBJECT_READY":
                await _object_queue.put("OBJECT_READY")
                log.debug("Serial: queued OBJECT_READY (queue size=%d)", _object_queue.qsize())
            elif line == "BUTTON_PRESSED:START":
                if is_running and current_template_name:
                    asyncio.create_task(start_flow())
                else:
                    log.info("Serial: START button — no active session, ignoring")
            elif line == "BUTTON_PRESSED:STOP":
                asyncio.create_task(stop_flow())
            else:
                log.debug("Serial: unrecognised message %r", line)
        except Exception as exc:
            log.error("Serial read error: %s", exc)
            await asyncio.sleep(1)


# ── Heartbeat loop ────────────────────────────────────────────────────────────
async def heartbeat_loop() -> None:
    while True:
        await asyncio.sleep(HB_INTERVAL)
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{BACKEND_URL}/api/heartbeat",
                    json={"session_id": current_session_id},
                    timeout=5,
                )
            log.debug("Heartbeat sent (session=%s)", current_session_id)
        except Exception as exc:
            log.warning("Heartbeat error: %s", exc)


# ── FastAPI HTTP server (command endpoint) ────────────────────────────────────
http_app = FastAPI(title="TM-X Agent (Mock)")


class CommandRequest(BaseModel):
    action:        str
    session_id:    Optional[int] = None
    template_name: Optional[str] = None
    target_count:  Optional[int] = None
    number_alpl:   Optional[int] = None


@http_app.post("/command")
async def command(req: CommandRequest):
    global current_session_id, current_template_name, current_target_count
    global current_number_alpl, is_running

    log.info("Command received: %s", req.action)

    if req.action == "start":
        current_session_id    = req.session_id
        current_template_name = req.template_name
        current_target_count  = req.target_count
        current_number_alpl   = req.number_alpl
        is_running            = True
        asyncio.create_task(start_flow())
        return {"ok": True}

    if req.action == "stop":
        asyncio.create_task(stop_flow())
        return {"ok": True}

    return {"error": "Unknown action"}


@http_app.post("/simulate/object-ready")
async def simulate_object_ready():
    """Simulate an OBJECT_READY signal from MCU — for testing without real hardware.

    หมายเหตุ: endpoint นี้ไม่มีความหมายแล้วตอนนี้ เพราะ mock_measurement_flow
    ไม่รอสัญญาณ OBJECT_READY อีกต่อไป (มันวน loop เองตาม target_count เลย) —
    เก็บไว้เผื่อย้อนกลับไปใช้ของจริงในอนาคต
    """
    if not is_running or current_session_id is None:
        log.warning("Simulate: rejected — no active session")
        return {"error": "No active session"}
    await _object_queue.put("OBJECT_READY")
    qsize = _object_queue.qsize()
    log.info("Simulate: queued OBJECT_READY (queue size=%d)", qsize)
    return {"ok": True, "session_id": current_session_id, "queue_size": qsize}


# ── Main: run everything concurrently ─────────────────────────────────────────
async def main() -> None:
    _init_serial()

    server = uvicorn.Server(
        uvicorn.Config(http_app, host="127.0.0.1", port=AGENT_PORT, log_level="info")
    )

    await asyncio.gather(
        server.serve(),
        heartbeat_loop(),
        serial_reader_loop(),
        object_ready_consumer(),
    )


if __name__ == "__main__":
    asyncio.run(main())
