# Backend-pc_station/agent.py
# How to run:
#   cd Backend-pc_station
#   pip install -r requirements.txt
#   python agent.py

import asyncio
import json
import logging
import os
import random
import re
import socket
import time
import uuid
from collections import Counter
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

# AGENT_MODE = "mock" (ค่าเริ่มต้น เหมือนเดิม ไม่กระทบของเดิม) หรือ "real"
# (ต่อ TM-X จริงผ่าน TCP ตาม tcp.py) — สลับได้จาก .env โดยไม่ต้องแก้โค้ด
# ตรงกับ gap ที่ค้างไว้ใน "On the horizon": ทำ Mock Mode ให้เป็น toggleable flag
AGENT_MODE  = os.getenv("AGENT_MODE", "mock").strip().lower()
TMX_ROUNDS  = int(os.getenv("TMX_ROUNDS", 5))  # จำนวนรอบ trigger+GM ต่อ 1 ชิ้นงาน (หาฐานนิยม)

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
_pending_uploads: list = []
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


# ── Synchronous TCP to TM-X (ของจริง — ใช้ตอน AGENT_MODE=real) ───────────────
# หมายเหตุ: พอร์ตมาจาก tcp.py ตรงๆ (socket แบบ blocking, terminator \r,
# settimeout 5s) เพราะเป็นโค้ดที่ทดสอบกับเครื่องจริงแล้วว่าใช้งานได้ — ของเดิม
# (_ensure_tcp/tcp_write/tcp_readline ด้านบน) ใช้ asyncio stream + terminator \n
# ซึ่งไม่ตรงกับ protocol จริงของ TM-X เลยไม่แตะ ปล่อยไว้เผื่ออนาคต
#
# ฟังก์ชัน sync ด้านล่างนี้ทุกตัวเป็น blocking I/O ทั้งหมด (เหมือน tcp.py) จึง
# ต้องเรียกผ่าน run_in_executor เท่านั้น (ดู wrapper async ด้านล่างสุดของบล็อกนี้)
# ห้ามเรียกตรงๆ จาก coroutine เพราะจะบล็อก FastAPI/heartbeat ทั้งตัวไปด้วย
_tmx_sock: Optional[socket.socket] = None
TMX_BUFFER_SIZE = 1024


def _tmx_connect_sync() -> None:
    global _tmx_sock
    if _tmx_sock is not None:
        try:
            _tmx_sock.close()
        except Exception:
            pass
    _tmx_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _tmx_sock.settimeout(5.0)
    log.info("TM-X (real): Connecting to %s:%d...", TMX_HOST, TMX_PORT)
    _tmx_sock.connect((TMX_HOST, TMX_PORT))
    log.info("TM-X (real): Connected successfully")


def _tmx_send_command_sync(command: str) -> Optional[str]:
    """ส่งคำสั่งไป TM-X แล้วรอรับ 1 ข้อความตอบกลับ (เหมือน send_command ใน tcp.py)
    คืนค่า None ถ้าล้มเหลวไม่ว่าจุดไหน (ทั้งตอนส่งและตอนรับ) — ไม่ raise exception
    ออกไปเด็ดขาด เพื่อให้ผู้เรียกจัดการ "ล้มเหลว" ได้แบบเดียวกันหมดทุกกรณี
    (เดิม sendall() ไม่ได้ wrap try/except เลย — ถ้า socket หลุดตอนส่ง exception
    จะทะลุขึ้นไปแบบไม่ถูกจับ ทำให้ session พังกลางคันโดยไม่มี log ที่ชัดเจน)
    """
    if _tmx_sock is None:
        log.error("TM-X (real): ส่ง %r ไม่ได้ — socket ยังไม่ได้เชื่อมต่อ", command)
        return None

    cmd_to_send = command + "\r"
    try:
        _tmx_sock.sendall(cmd_to_send.encode("ascii"))
    except Exception as exc:
        log.error("TM-X (real): ส่งคำสั่ง %r ไม่สำเร็จ (สาย/socket มีปัญหา): %s", command, exc)
        return None

    time.sleep(0.1)
    try:
        response = _tmx_sock.recv(TMX_BUFFER_SIZE).decode("ascii").strip()
        log.info("TM-X (real) >>> %s | <<< %s", command, response)
        return response
    except Exception as exc:
        log.error("TM-X (real): อ่าน response ของ %r ไม่สำเร็จ: %s", command, exc)
        return None


def _tmx_send_command_with_reconnect(command: str, retries: int = 1) -> Optional[str]:
    """เหมือน _tmx_send_command_sync แต่ถ้าล้มเหลว (สื่อว่า socket หลุด/สายสะดุด)
    จะพยายาม reconnect แล้วส่งคำสั่งเดิมซ้ำอีก `retries` ครั้งก่อนยอมแพ้คืน None

    เดิมถ้า socket หลุดกลาง session ไม่มีใครพยายามต่อใหม่เลย — ชิ้นงานที่เหลือ
    ทั้งหมดจะ fail รัวๆ ทีละชิ้นไปจนกว่า operator จะสังเกตแล้วสั่ง Stop/Start
    session ใหม่เอง ฟังก์ชันนี้ทำให้สาย LAN สะดุดแวบเดียว (ไม่ใช่เครื่องเสียจริง)
    กู้กลับมาทำงานต่อได้เองโดย session ไม่ต้องขาดตอน
    """
    response = _tmx_send_command_sync(command)
    if response is not None:
        return response

    for attempt in range(1, retries + 1):
        log.warning("TM-X (real): %r ล้มเหลว — ลอง reconnect รอบที่ %d/%d", command, attempt, retries)
        try:
            _tmx_connect_sync()
        except Exception as exc:
            log.error("TM-X (real): reconnect ไม่สำเร็จ (รอบที่ %d/%d): %s", attempt, retries, exc)
            continue
        response = _tmx_send_command_sync(command)
        if response is not None:
            log.info("TM-X (real): reconnect สำเร็จ — %r ทำงานต่อได้ปกติ", command)
            return response

    log.error("TM-X (real): %r ล้มเหลวถาวรหลัง reconnect ครบ %d ครั้ง", command, retries)
    return None


def _is_error_response(resp: Optional[str]) -> bool:
    """เช็คว่า response จาก TM-X เป็น error หรือส่งไม่สำเร็จ — TM-X ตอบ error
    กลับมาในรูปแบบ "ER,<คำสั่ง>,<โค้ด>" เสมอ (เช่น "ER,PW,22") เดิมโค้ดเช็คแค่
    ตอนโหลด PW จุดเดียว ส่วน T1/GM ไม่เช็คเลย ถ้า error หลุดผ่านไปจะถูกเอาไป
    split(',') ปนกับค่าที่วัดได้แบบเงียบๆ (เช่น "ER,GM,03" → เผลอเอา "03" ไป
    เป็นค่าวัดจริงได้ เพราะ "03" ไม่ตรงกับ placeholder "9999.999" ที่กรองอยู่)
    """
    return resp is None or resp.startswith("ER")


def _tmx_disconnect_sync() -> None:
    global _tmx_sock
    if _tmx_sock is not None:
        try:
            _tmx_sock.close()
        except Exception:
            pass
        _tmx_sock = None
        log.info("TM-X (real): Connection closed")


async def tmx_connect() -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _tmx_connect_sync)


async def tmx_send_command(command: str) -> Optional[str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _tmx_send_command_with_reconnect, command)


async def tmx_disconnect() -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _tmx_disconnect_sync)


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


# ── Read new image from Store_image_temporary ─────────────────────────────────
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


# ── Pydantic validation for measurement values ────────────────────────────────
class MeasurementData(BaseModel):
    value_x: float
    value_y: float

    @validator("value_x", "value_y")
    def must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("Measurement value must be > 0")
        return v


# ── Image upload to MinIO with retry ─────────────────────────────────────────
async def upload_image(image_path: str, measurement_id: int) -> None:
    filename = os.path.basename(image_path)
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient() as client:
                # 1) Get presigned PUT URL
                r = await client.post(
                    f"{BACKEND_URL}/api/upload-url",
                    json={"filename": filename, "measurement_id": measurement_id},
                    timeout=10,
                )
                r.raise_for_status()
                data = r.json()
                presigned_url = data["presigned_url"]
                object_key    = data["object_key"]

                # 2) PUT image bytes directly to MinIO
                with open(image_path, "rb") as fh:
                    image_bytes = fh.read()
                put_r = await client.put(presigned_url, content=image_bytes, timeout=30)
                put_r.raise_for_status()

                # 3) Update image_path in backend
                await client.patch(
                    f"{BACKEND_URL}/api/measurements/{measurement_id}/image",
                    json={"image_path": object_key},
                    timeout=10,
                )

            log.info("Image uploaded: %s (measurement #%d)", object_key, measurement_id)
            return
        except Exception as exc:
            log.warning("Upload attempt %d/3 failed: %s", attempt, exc)
            await asyncio.sleep(2)

    log.error("Image upload failed after 3 attempts: %s", image_path)

    # แจ้ง backend ว่ารูปของ measurement นี้อัปโหลดไม่สำเร็จ (ล้มเหลวครบ 3 ครั้ง)
    # เดิมพอ retry ครบแล้วแค่ log.error ในเครื่อง Agent เฉยๆ — measurement จะมี
    # image_path เป็น NULL ตลอดไปโดยไม่มีใครใน backend/web รู้เรื่องเลยว่าเกิด
    # อะไรขึ้น (แยกไม่ออกจากกรณี "ยังไม่มีรูปเพราะเป็น manual add") ตอนนี้ยิง
    # PATCH บอก backend ตรงๆ ว่า upload_failed=True เพื่อให้ web ขึ้น badge เตือน
    # ในตาราง (ดู main.py update_image + index.html imgCell)
    try:
        async with httpx.AsyncClient() as client:
            await client.patch(
                f"{BACKEND_URL}/api/measurements/{measurement_id}/image",
                json={"image_path": None, "upload_failed": True},
                timeout=10,
            )
    except Exception as exc:
        log.error(
            "แจ้ง backend ว่า image upload failed ก็ยังไม่สำเร็จอีก (measurement #%d): %s",
            measurement_id, exc,
        )


# ── Cleanup Store_image_temporary ────────────────────────────────────────────
def _cleanup_temp_images() -> None:
    global _seen_images
    if not os.path.isdir(TEMP_IMAGE_DIR):
        log.error("Cleanup: directory not found: %s", TEMP_IMAGE_DIR)
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


# ── Real measurement: อ่านค่าจาก TM-X จริงผ่าน TCP (AGENT_MODE=real) ─────────
def _wait_for_start_signal_sync() -> None:
    """แทนสัญญาณ OBJECT_READY/ปุ่ม START จาก Micro ที่ยังไม่ได้ต่อจริง —
    ให้ operator กดพิมพ์ Enter ที่ terminal ของ Agent เองก่อนแต่ละชิ้น
    (ตามที่ Ball ระบุไว้: `start = input("พิมพ์เริ่ม: ")`)
    เป็น blocking call จึงต้องเรียกผ่าน run_in_executor เท่านั้น — ห้ามเรียก
    ตรงๆ ใน coroutine เพราะจะบล็อก event loop ทั้งตัว (heartbeat/FastAPI ค้าง)
    """
    input("พิมพ์เริ่ม: ")


async def wait_for_start_signal() -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _wait_for_start_signal_sync)


def _extract_template_number(template_name: Optional[str]) -> str:
    """แปลง template_name ที่ backend ส่งมาให้เป็นเลขโปรแกรม 3 หลักสำหรับคำสั่ง
    PW,1,<nnn> — ตอนนี้ยังไม่รู้ format จริงของ template_name ที่ backend ส่งมา
    (เป็นเลขล้วนอย่าง "21"/"021" หรือมีคำนำหน้าอย่าง "IPM_021" ก็เป็นได้) เลย
    เดา 2 เคสไว้ก่อน:
      1) เป็นตัวเลขล้วน → zero-pad เป็น 3 หลักตรงๆ
      2) มีตัวอักษรปน → ดึงกลุ่มตัวเลขท้ายสุดออกมา zero-pad เป็น 3 หลัก
    ถ้า Ball ทดสอบแล้วพบว่า mapping จริงไม่ตรงกับนี้ ให้แก้ฟังก์ชันนี้จุดเดียว
    """
    if not template_name:
        raise ValueError("template_name ว่างเปล่า — ไม่รู้จะโหลดโปรแกรมไหน")
    if template_name.isdigit():
        return template_name.zfill(3)
    match = re.search(r"(\d+)\s*$", template_name)
    if match:
        return match.group(1).zfill(3)
    raise ValueError(f"ดึงเลข template จาก {template_name!r} ไม่ได้ — ตรวจสอบ format กับ backend")


def _pick_mode(str_values: list) -> float:
    """หาฐานนิยม (mode) จากค่าที่วัดได้หลายรอบ (string ตรงจาก TM-X เช่น '12.3450')
    ใช้ Counter แทน statistics.mode() เพราะ statistics.mode() จะ raise
    StatisticsError ถ้าไม่มีฐานนิยมเดียวชัดเจน (ทุกค่าไม่ซ้ำกันเลย) — Counter
    ทนกว่า ถ้าไม่มีค่าซ้ำกันเลยจะได้ค่าแรกที่เจอมาแทน (better than crashing)
    """
    if not str_values:
        raise ValueError("ไม่มีค่าที่วัดได้เลยในรอบนี้ (ทุกรอบเป็น 9999.999/error)")
    counts = Counter(str_values)
    most_common_value, _ = counts.most_common(1)[0]
    return float(most_common_value)


def _read_one_round_xy_sync():
    """1 รอบ: ส่ง T1 (trigger ให้วัดใหม่) แล้วดึงค่าด้วย GM,0,0 — คืนค่า
    (value_x_str, value_y_str) หรือ None ถ้ารอบนี้ไม่มีค่าที่ใช้ได้เลย
    (ล้อกับ logic เดิมใน tcp.py: ตัด +/- ทิ้ง, ข้าม placeholder 9999.999,
    เอา 2 ค่าสุดท้ายของรอบนี้เป็น value_x, value_y ตามลำดับที่ตั้ง tool ไว้)

    เช็ค error response ("ER,...") จากทั้ง T1 และ GM ก่อนเสมอ — เดิมไม่เช็ค
    เลยสักคำสั่ง ถ้า T1 error (เช่นเครื่องยังไม่ READY) แล้วไปเรียก GM ต่อทันที
    อาจได้ค่าเก่าจากรอบก่อนกลับมาแทนแบบเงียบๆ หรือถ้า GM เอง error กลับมา
    ("ER,GM,03") ตัวเลขในนั้นก็จะถูกเข้าใจผิดว่าเป็นค่าที่วัดได้จริง
    """
    t1_resp = _tmx_send_command_with_reconnect("T1")
    if _is_error_response(t1_resp):
        log.warning("TM-X (real): T1 (trigger) ล้มเหลว/error (%r) — ข้ามรอบนี้ ไม่เรียก GM ต่อ", t1_resp)
        return None

    raw = _tmx_send_command_with_reconnect("GM,0,0")
    if _is_error_response(raw):
        log.warning("TM-X (real): GM,0,0 ล้มเหลว/error (%r) — ข้ามรอบนี้", raw)
        return None

    cleaned = []
    for token in raw.split(","):
        token = token.strip().strip("+").strip("-")
        if token == "" or token == "9999.999":
            continue
        cleaned.append(token)

    if len(cleaned) < 2:
        log.warning("TM-X (real): รอบนี้ได้ค่าไม่ครบ (raw=%r, cleaned=%r)", raw, cleaned)
        return None

    value_x_str, value_y_str = cleaned[-2], cleaned[-1]
    return value_x_str, value_y_str


def _get_real_measurement_sync(rounds: int = TMX_ROUNDS) -> "MeasurementData":
    """วนอ่านค่า X/Y จาก TM-X จริง `rounds` รอบ แล้วหาฐานนิยมของแต่ละแกน
    เพื่อกันค่ากระเพื่อม/ค่าหลุดเป็นครั้งคราวจากรอบเดียว (ตามที่ Ball ระบุไว้)

    เช็ค is_running ก่อนเริ่มแต่ละรอบด้วย — เดิมวนครบ 5 รอบเสมอไม่สนใจว่ากด
    Stop ไปแล้วหรือยัง กด Stop กลางรอบที่ 2/5 จะยังวัดจนครบ 5 รอบก่อน (ยังมี
    ข้อจำกัดเดิมอยู่ตรงที่หยุดกลาง 1 รอบ T1+GM คู่เดียวไม่ได้ เพราะเป็น
    blocking I/O ธรรมดา แต่อย่างน้อยจะไม่ลากยาวไปอีก 4 รอบที่เหลือ)
    """
    xs, ys = [], []
    for j in range(1, rounds + 1):
        if not is_running:
            log.warning("TM-X (real): ถูกสั่ง stop ระหว่างรอบที่ %d/%d — หยุดอ่านค่าเพิ่ม", j, rounds)
            break
        result = _read_one_round_xy_sync()
        if result is None:
            log.warning("TM-X (real): ข้ามรอบที่ %d/%d (อ่านค่าไม่ได้)", j, rounds)
            continue
        x_str, y_str = result
        xs.append(x_str)
        ys.append(y_str)
        log.info("TM-X (real): รอบที่ %d/%d | x=%s y=%s", j, rounds, x_str, y_str)

    value_x = _pick_mode(xs)
    value_y = _pick_mode(ys)
    log.info("TM-X (real): ฐานนิยมจาก %d/%d รอบ → x=%.4f y=%.4f", len(xs), rounds, value_x, value_y)
    return MeasurementData(value_x=value_x, value_y=value_y)


async def get_real_measurement() -> "MeasurementData":
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_real_measurement_sync)



def _generate_mock_measurement() -> "MeasurementData":
    """สุ่มค่า value_x/value_y แบบมั่วๆ ไว้ใช้ทดสอบ flow ทั้งระบบโดยไม่ต้องมี
    TM-X/MCU จริงต่ออยู่ — สุ่มในช่วงที่กว้างพอจะได้เห็นทั้งผล OK และ NG
    คละกันบ้าง (ไม่ได้ดูค่า tolerance จริงของ part เลย เพราะ Agent ไม่รู้
    และไม่จำเป็นต้องรู้ — backend เป็นคนตัดสิน OK/NG เองอยู่แล้ว)
    """
    return MeasurementData(value_x=round(random.uniform(1.0, 20.0), 3),
                            value_y=round(random.uniform(1.0, 20.0), 3))


FAILED_MEASUREMENTS_LOG = os.path.join(_root, "failed_measurements.jsonl")


def _persist_failed_measurement(session_id, number_alpl, value_x, value_y, reason: str) -> None:
    """บันทึกชิ้นที่ POST เข้า backend ไม่สำเร็จ (หลัง retry ครบ 3 ครั้งแล้ว หรือ
    ถูกปฏิเสธถาวร) ลงไฟล์ local (JSON Lines) เป็น safety net — เดิมถ้า POST
    ล้มเหลว ค่าที่วัดได้จริงจากเครื่อง (โดยเฉพาะ real mode ที่กว่าจะได้มาต้อง
    ยิง T1+GM ครบ 5 รอบ ไม่ใช่แค่ mock สุ่มใหม่ได้ฟรีๆ) จะหายไปเฉยๆ ไม่มีทาง
    กู้คืนนอกจากไปไล่จด log บน terminal เอง

    กู้คืนทีหลังได้โดยเปิดไฟล์นี้แล้วกรอกกลับเข้า Database Editor (Add
    Measurement) ด้วยมือ — ไม่ได้ auto-retry เข้า backend ให้ เพราะถ้า backend
    ปฏิเสธไปแล้ว (เช่น session ไม่ running) auto-retry ก็จะพังซ้ำอยู่ดี
    """
    try:
        with open(FAILED_MEASUREMENTS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "timestamp":   time.strftime("%Y-%m-%d %H:%M:%S"),
                "session_id":  session_id,
                "number_alpl": number_alpl,
                "value_x":     value_x,
                "value_y":     value_y,
                "reason":      reason,
            }, ensure_ascii=False) + "\n")
        log.info("บันทึกชิ้นที่ส่ง backend ไม่สำเร็จลงไฟล์ %s แล้ว (กู้คืนทีหลังได้ด้วยมือ)", FAILED_MEASUREMENTS_LOG)
    except Exception as exc:
        log.error(
            "บันทึกไฟล์ %s ก็ไม่สำเร็จอีก — ค่านี้หายจริง (x=%.3f, y=%.3f): %s",
            FAILED_MEASUREMENTS_LOG, value_x, value_y, exc,
        )


async def post_measurement(index: int, value_x: float, value_y: float, source_label: str = "Measurement"):
    """POST ค่า value_x/value_y 1 ชิ้นไป backend (retry ได้ถึง 3 ครั้ง) + หา/
    อัปโหลดรูปคู่กัน — logic เดิมที่เคยอยู่ใน mock_single_measurement แยกออกมา
    เป็นฟังก์ชันกลาง เพื่อให้ real_single_measurement เรียกใช้ร่วมกันได้โดยไม่
    ต้อง copy โค้ด retry/error-handling ซ้ำอีกชุด (mock กับ real ต่างกันแค่
    "ค่ามาจากไหน" ส่วนที่เหลือหลังจากได้ value_x/value_y แล้วเหมือนกันทุกอย่าง)

    คืนค่า (ok, status, session_active) เป็น tuple:
      - ok: True ถ้าบันทึกลง backend สำเร็จ, False ถ้าล้มเหลวจนต้องข้ามชิ้นนี้ไป
      - status: ค่า "status" ที่ backend ตอบกลับมา ("continue"/"complete") หรือ
        None ถ้า POST ไม่สำเร็จ — คือ "สัญญาณว่าวัดเสร็จแล้วจาก Backend" เอาไว้
        ให้ flow เช็คแล้วหยุด loop ทันทีได้โดยไม่ต้องรอ target ที่นับเองครบ
      - session_active: False เฉพาะกรณี backend ตอบ HTTP 400 "Session is not
        running" กลับมา (session ถูกจบไปแล้วจากทางอื่น เช่นกด Stop จากเว็บ
        พร้อมกัน หรือ auto-complete ไปแล้วแต่ Agent ไม่ทันรู้) — ความหมายคือ
        "อย่าพยายามวัดชิ้นต่อไปอีกเลย เพราะจะโดนปฏิเสธเหมือนกันหมดทุกชิ้น"
        ปกติเป็น True เสมอ (คือ POST ล้มเหลวด้วยเหตุผลอื่นที่ไม่เกี่ยวกับ
        session state เช่น เน็ตสะดุด ยังวัดชิ้นถัดไปต่อได้ตามปกติ)
    """
    global current_session_id, current_number_alpl

    log.info("%s #%d: x=%.3f, y=%.3f", source_label, index, value_x, value_y)

    image_path = await get_new_image()

    client_uuid = str(uuid.uuid4())
    data = None
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{BACKEND_URL}/api/measurements",
                    json={
                        "session_id":  current_session_id,
                        "number_alpl": current_number_alpl,
                        "value_x":     value_x,
                        "value_y":     value_y,
                        "client_uuid": client_uuid,
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
            break
        except httpx.HTTPStatusError as exc:
            http_status = exc.response.status_code
            if http_status == 400:
                # เช็คว่าเป็นกรณี "Session is not running" หรือเปล่า (ดู
                # main.py create_measurement) — ถ้าใช่ ไม่มีประโยชน์จะ retry
                # หรือไปวัดชิ้นถัดไปเลย เพราะทุกชิ้นจะโดนปฏิเสธเหมือนกันหมด
                # (session จบไปแล้วจากทางอื่น เช่นกด Stop จากเว็บพร้อมกัน)
                try:
                    detail = str(exc.response.json().get("detail", "")).lower()
                except Exception:
                    detail = ""
                if "not running" in detail:
                    log.error("%s #%d: backend บอกว่า session ไม่ running แล้ว (%s) "
                              "— หยุดทั้ง session ทันที ไม่วัดชิ้นถัดไป",
                              source_label, index, detail)
                    _persist_failed_measurement(current_session_id, current_number_alpl,
                                                 value_x, value_y, reason="session_not_running")
                    return False, None, False
            if http_status < 500:
                log.error("%s #%d: backend ปฏิเสธ (HTTP %d): %s", source_label, index, http_status, exc)
                break
            log.warning("%s #%d: POST attempt %d/3 failed (HTTP %d): %s",
                        source_label, index, attempt, http_status, exc)
        except Exception as exc:
            log.warning("%s #%d: POST attempt %d/3 failed: %s", source_label, index, attempt, exc)
        if attempt < 3:
            await asyncio.sleep(2)

    if data is None:
        print(f"❌ ชิ้นที่ {index}/{current_target_count}: บันทึกไม่สำเร็จ "
              f"(x={value_x:.3f}, y={value_y:.3f}) — บันทึก backup ไว้แล้ว, ข้ามไปชิ้นถัดไป")
        log.error(
            "%s #%d: POST /api/measurements ล้มเหลวหลังลองครบ 3 ครั้ง "
            "(หรือถูกปฏิเสธ) — ค่า x=%.3f, y=%.3f ของชิ้นนี้ไม่ถูกบันทึกลง backend",
            source_label, index, value_x, value_y,
        )
        _persist_failed_measurement(current_session_id, current_number_alpl,
                                     value_x, value_y, reason="post_failed_after_retries")
        return False, None, True

    measurement_id = data["measurement_id"]
    backend_status = data["status"]  # "continue" หรือ "complete" (ดู docstring)
    log.info(
        "%s #%d: measurement_id=%d ALPL=%s result=%s measured=%d/%d status=%s",
        source_label, index, measurement_id, data.get("number_alpl"), data["result"],
        data["measured"], data["target"], backend_status,
    )

    if image_path:
        task = asyncio.create_task(upload_image(image_path, measurement_id))
        _pending_uploads.append(task)
        task.add_done_callback(_pending_uploads.remove)

    return True, backend_status, True


async def mock_single_measurement(index: int):
    """ทำ 1 รอบของการ "วัด" แบบ mock: gen ค่า → post_measurement() เหมือนเดิม
    ทุกอย่าง (ไม่แตะ TCP/Serial เลย) — logic POST/retry/รูป ย้ายไปรวมอยู่ที่
    post_measurement() แล้ว ฟังก์ชันนี้เหลือแค่ "หาค่ามาจากไหน" เท่านั้น
    คืนค่า (ok, status) เหมือน post_measurement — ดูรายละเอียดที่นั่น
    """
    m = _generate_mock_measurement()
    return await post_measurement(index, m.value_x, m.value_y, source_label="Mock measurement")


async def real_single_measurement(index: int):
    """ทำ 1 รอบของการวัดจริง: รอสัญญาณเริ่ม (แทน trigger จาก Micro ด้วย input()
    ไปก่อน) → อ่านค่า X/Y จริงจาก TM-X ครบ 5 รอบแล้วหาฐานนิยม → post_measurement()
    เหมือน mock ทุกอย่างหลังจากได้ค่าแล้ว คืนค่า (ok, status, session_active) เหมือน post_measurement
    """
    print(f"\n▶ พร้อมวัดชิ้นที่ {index}/{current_target_count} — วางชิ้นงานแล้วกด Enter เพื่อเริ่ม")
    await wait_for_start_signal()

    try:
        m = await get_real_measurement()
    except Exception as exc:
        log.error("Real measurement #%d: อ่านค่าจาก TM-X ล้มเหลว: %s", index, exc)
        print(f"❌ ชิ้นที่ {index}/{current_target_count}: อ่านค่าจาก TM-X ไม่สำเร็จ ({exc}) — ข้ามไปชิ้นถัดไป")
        return False, None, True  # session_active=True — ปัญหาคือ TM-X ไม่ใช่ backend session

    return await post_measurement(index, m.value_x, m.value_y, source_label="Real measurement")


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
        ok, backend_status, session_active = await mock_single_measurement(i)
        if ok:
            succeeded += 1
        # backend บอกว่า session ไม่ running แล้ว (เช่นถูก Stop จากที่อื่น
        # พร้อมกัน) — วัดชิ้นถัดไปก็จะโดนปฏิเสธเหมือนกันหมด หยุดทันทีดีกว่า
        if not session_active:
            log.error("Mock flow: backend บอกว่า session ไม่ running แล้ว — หยุดทันที (ชิ้นที่ %d/%d)", i, target)
            print(f"❌ Backend บอกว่า session นี้ไม่ได้ running แล้ว — หยุดวัดทันที")
            break
        # backend เป็นคนตัดสิน "วัดครบ session แล้ว" จริงๆ (เช็ค measured >=
        # target ใน DB เอง) ไม่ใช่ตัวเลข target ที่ Agent จำไว้ตอน start — ถ้า
        # backend บอกมาว่า complete แล้ว หยุด loop ทันที ไม่ต้องรอนับของตัวเอง
        # (กันเคสตัวเลขสองฝั่งไม่ตรงกัน เช่น Agent เคย restart กลาง session)
        if backend_status == "complete":
            log.info("Mock flow: backend แจ้งว่า session complete แล้ว (ชิ้นที่ %d/%d) — หยุด loop ทันที", i, target)
            break

    async with _state_lock:
        is_running = False

    # รอ upload รูปที่ยังค้างอยู่ให้เสร็จก่อน cleanup (เหมือน flow จริงเดิม)
    if _pending_uploads:
        log.info("Waiting for %d upload(s) to finish before cleanup…", len(_pending_uploads))
        await asyncio.gather(*_pending_uploads, return_exceptions=True)
    _cleanup_temp_images()

    if succeeded == target:
        print(f"✅ Done — วัดครบ {target} ชิ้นแล้ว (session_id={current_session_id})")
    else:
        print(f"⚠️ Done — วัดสำเร็จ {succeeded}/{target} ชิ้น "
              f"({target - succeeded} ชิ้นบันทึกไม่สำเร็จ ดู log ด้านบน) "
              f"(session_id={current_session_id})")
    log.info("Mock flow done: session=%s สำเร็จ %d/%d", current_session_id, succeeded, target)



async def real_measurement_flow() -> None:
    """Flow จริง: ต่อ TM-X → R0 (เข้าโหมดดำเนินงาน) → PW,1,<เลข template>
    (โหลดโปรแกรมตาม template ที่ backend สั่งมา) → วน real_single_measurement
    ทีละชิ้นตาม target_count (แต่ละชิ้นรอ input() แทน trigger จาก Micro ก่อน
    แล้วค่อยยิง T1+GM 5 รอบหาฐานนิยม) → S0 (กลับโหมดตั้งค่า) → ปิด connection

    หมายเหตุ: ถ้า connect หรือ PW ล้มเหลว (เช่น error 22 พารามิเตอร์ผิด, หรือ
    เครื่องต่อไม่ติด) จะ log + print แล้ว "หยุดทั้ง session ทันที" ไม่ไล่วัดต่อ
    เพราะไม่มีทางรู้ว่าโปรแกรมที่โหลดถูกชิ้นงานจริงหรือเปล่า
    """
    global is_running

    print(f"✅ ได้รับคำสั่ง Start (REAL) — session_id={current_session_id}, template_name={current_template_name!r}")
    log.info("Real start: session=%s template=%r target_count=%s",
              current_session_id, current_template_name, current_target_count)

    try:
        program_no = _extract_template_number(current_template_name)
    except ValueError as exc:
        log.error("Real flow: %s", exc)
        print(f"❌ ยกเลิก session — {exc}")
        async with _state_lock:
            is_running = False
        return

    try:
        await tmx_connect()
        r0_resp = await tmx_send_command("R0")
        log.info("Real flow: R0 -> %r", r0_resp)
        if _is_error_response(r0_resp):
            log.warning("Real flow: R0 ตอบ error/ไม่มีการตอบกลับ (%r) — ไปต่อ PW ทันที "
                        "(R0 error บางเคสไม่ fatal เช่นเข้าโหมด operation อยู่แล้ว)", r0_resp)

        pw_resp = await tmx_send_command(f"PW,1,{program_no}")
        log.info("Real flow: PW,1,%s -> %r", program_no, pw_resp)
        if _is_error_response(pw_resp):
            raise RuntimeError(f"โหลดโปรแกรม {program_no} ไม่สำเร็จ: {pw_resp}")
        await asyncio.sleep(1.0)  # รอโปรแกรม load เสร็จ (เท่ากับ tcp.py)
    except Exception as exc:
        log.error("Real flow: เชื่อมต่อ/โหลดโปรแกรมล้มเหลว: %s", exc)
        print(f"❌ ยกเลิก session — เชื่อมต่อ/โหลดโปรแกรม TM-X ไม่สำเร็จ: {exc}")
        await tmx_disconnect()
        async with _state_lock:
            is_running = False
        return

    succeeded = 0
    target = current_target_count or 0
    for i in range(1, target + 1):
        if not is_running:
            log.warning("Real flow: ถูกสั่ง stop กลางทาง (รอบที่ %d/%d) — หยุดเลย", i, target)
            break
        ok, backend_status, session_active = await real_single_measurement(i)
        if ok:
            succeeded += 1
        if not session_active:
            log.error("Real flow: backend บอกว่า session ไม่ running แล้ว — หยุดทันที (ชิ้นที่ %d/%d)", i, target)
            print(f"❌ Backend บอกว่า session นี้ไม่ได้ running แล้ว — หยุดวัดทันที (S0 กำลังส่งไป TM-X)")
            break
        # backend เป็นคนตัดสิน "วัดครบ session แล้ว" จริงๆ (เช็ค measured >=
        # target ใน DB เอง) — นี่คือ "สัญญาณว่าวัดเสร็จแล้วจาก Backend" ตามที่
        # Ball ระบุไว้ (ข้อ 4) ถ้า backend บอกมาว่า complete แล้ว หยุด loop
        # ทันที ไม่ต้องรอ target ที่ Agent นับเองครบ (กันเคสตัวเลขสองฝั่งไม่
        # ตรงกัน) แล้วโค้ดหลัง loop ด้านล่างจะส่ง S0 ให้เองอยู่แล้ว
        if backend_status == "complete":
            log.info("Real flow: backend แจ้งว่า session complete แล้ว (ชิ้นที่ %d/%d) — หยุด loop ทันที ไม่รอ target ของตัวเอง", i, target)
            break

    async with _state_lock:
        is_running = False

    try:
        await tmx_send_command("S0")  # กลับเข้าโหมดตั้งค่า เหมือน tcp.py ตอนจบ
    finally:
        await tmx_disconnect()

    if _pending_uploads:
        log.info("Waiting for %d upload(s) to finish before cleanup…", len(_pending_uploads))
        await asyncio.gather(*_pending_uploads, return_exceptions=True)
    _cleanup_temp_images()

    if succeeded == target:
        print(f"✅ Done — วัดครบ {target} ชิ้นแล้ว (session_id={current_session_id})")
    else:
        print(f"⚠️ Done — วัดสำเร็จ {succeeded}/{target} ชิ้น "
              f"({target - succeeded} ชิ้นบันทึกไม่สำเร็จ ดู log ด้านบน) "
              f"(session_id={current_session_id})")
    log.info("Real flow done: session=%s สำเร็จ %d/%d", current_session_id, succeeded, target)


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
    """เลือก flow ตาม AGENT_MODE (.env):
      - AGENT_MODE=mock (ค่าเริ่มต้น) → mock_measurement_flow() เหมือนเดิมทุก
        อย่าง ไม่กระทบของเดิมเลยถ้าไม่ตั้งค่าอะไรเพิ่ม
      - AGENT_MODE=real → real_measurement_flow() ต่อ TM-X จริงผ่าน TCP ตาม
        tcp.py (PW โหลด template ที่ backend สั่ง, T1+GM 5 รอบหาฐานนิยม)
    ฟังก์ชัน tcp_write/tcp_readline (async stream แบบเก่า) และ send_serial
    ด้านบนยังเก็บไว้เผื่ออนาคตเหมือนเดิม ไม่ได้ใช้ในทั้งสอง flow นี้
    """
    if AGENT_MODE == "real":
        await real_measurement_flow()
    else:
        await mock_measurement_flow()


async def stop_flow() -> None:
    """Stop จาก Backend (กดปุ่ม Stop บนเว็บ → POST /command action="stop")

    1. ตั้ง is_running=False — loop ใน mock_measurement_flow/real_measurement_flow
       จะเห็นตอนขึ้นรอบถัดไปแล้วหยุดเอง (ถ้า loop กำลังรอ input()/T1/GM อยู่
       พอดี จะหยุดไม่ทันที ต้องรอรอบปัจจุบันจบก่อน — ข้อจำกัดของการ interrupt
       blocking I/O กลางคันด้วย asyncio ธรรมดา)
    2. ถ้าอยู่ใน AGENT_MODE=real และ TM-X ต่ออยู่จริง ให้ยิง "S0" ไปทันทีเลย
       ตามที่ Ball ระบุไว้ (ข้อ 2) — ยิงซ้ำกับที่ real_measurement_flow จะส่ง
       S0 อีกทีตอน loop จบตามปกติได้ไม่เป็นไร (TM-X รับ S0 ซ้ำได้ ไม่พัง) แต่
       ทำให้ "หยุดจริง" เร็วขึ้นแทนที่จะรอ loop จบเองอย่างเดียว
    """
    global is_running
    log.info("Stop flow: หยุด measurement flow (mode=%s)", AGENT_MODE)
    async with _state_lock:
        is_running = False

    if AGENT_MODE == "real" and _tmx_sock is not None:
        try:
            resp = await tmx_send_command("S0")
            log.info("Stop flow: ส่ง S0 ทันที -> %r", resp)
        except Exception as exc:
            log.warning("Stop flow: ส่ง S0 ไม่สำเร็จ (จะลองอีกทีตอน loop จบ): %s", exc)


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
http_app = FastAPI(title="TM-X Agent")


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
    _cleanup_temp_images()
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
