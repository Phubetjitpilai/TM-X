# Backend-pc_station/agent.py
# How to run:
#   cd Backend-pc_station
#   pip install -r requirements.txt
#   python agent.py

import asyncio
import json
import logging
import os
import socket
import threading
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
# TMX_HOST/TMX_PORT ต้องตั้งใน .env ให้ตรงกับ IP/Port จริงของเครื่อง TM-X
# (เช่น 192.168.10.11 / 8600 ตามที่ทดสอบเชื่อมต่อสำเร็จ) ค่า default ด้านล่าง
# เป็นแค่ fallback เฉยๆ
TMX_HOST       = os.getenv("TMX_HOST",      "127.0.0.1")
TMX_PORT       = int(os.getenv("TMX_PORT",      5000))
TMX_BUFFER_SIZE = 1024
TEMP_IMAGE_DIR = os.getenv("TEMP_IMAGE_DIR", "./Store_image_temporary")

# FTP server สำหรับรับรูปจากกล้อง Keyence — ตัวแปรชุดเดียวกับที่เคยอยู่ใน
# run_ftp_server.py (ย้ายมารวมในไฟล์นี้แล้ว ไม่ต้องรันแยกสคริปต์อีกไฟล์)
FTP_HOST     = os.getenv("FTP_HOST", "0.0.0.0")
FTP_PORT     = int(os.getenv("FTP_PORT", 21))
FTP_USER     = os.getenv("FTP_USER", "INTERN_USER")
FTP_PASSWORD = os.getenv("FTP_PASSWORD", "123456")

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

# ── TCP connection to TM-X ────────────────────────────────────────────────────
# หมายเหตุ: ทดสอบกับเครื่องจริงแล้วว่า TM-X คุยด้วย TCP socket แบบ blocking
# ธรรมดา (connect → sendall → recv) ไม่ใช่ asyncio stream — และตัวคั่นคำสั่ง
# (delimiter) คือ CR ("\r") ไม่ใช่ "\n" (ดู test script ต้นฉบับที่ยืนยันไว้แล้ว
# ว่าใช้ได้จริงกับกล้อง/เครื่องวัด) จึงใช้ socket แบบ sync ตรงๆ แล้วสั่งให้รันใน
# executor (loop.run_in_executor) เพื่อไม่ให้ไป block event loop ของ FastAPI
_tmx_socket: Optional[socket.socket] = None
_tmx_lock = asyncio.Lock()


def _tmx_connect_sync() -> None:
    global _tmx_socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5.0)
    s.connect((TMX_HOST, TMX_PORT))
    _tmx_socket = s
    log.info("TM-X: Connected to %s:%d", TMX_HOST, TMX_PORT)


def _tmx_send_command_sync(command: str) -> Optional[str]:
    """ส่งคำสั่งไปยัง TM-X ผ่าน TCP แล้วรอรับผลลัพธ์ตอบกลับ

    ต่อท้ายด้วย CR ("\\r") เสมอ ตามโปรโตคอลที่ทดสอบสำเร็จแล้ว ลอง reconnect
    ใหม่ 1 ครั้งถ้า socket หลุด/ส่งไม่สำเร็จ ก่อนจะยอมแพ้แล้ว log error ออกไป
    """
    global _tmx_socket
    cmd_to_send = command + "\r"
    for attempt in (1, 2):
        try:
            if _tmx_socket is None:
                _tmx_connect_sync()
            _tmx_socket.sendall(cmd_to_send.encode("ascii"))
            time.sleep(0.1)  # หน่วงเวลาให้เครื่องประมวลผลเล็กน้อย เหมือน script ทดสอบเดิม
            response = _tmx_socket.recv(TMX_BUFFER_SIZE).decode("ascii").strip()
            log.info("TM-X >>> %r | <<< %r", command, response)
            return response
        except Exception as exc:
            log.warning("TM-X: ส่งคำสั่ง %r ไม่สำเร็จ (ครั้งที่ %d): %s", command, attempt, exc)
            try:
                if _tmx_socket:
                    _tmx_socket.close()
            except Exception:
                pass
            _tmx_socket = None

    log.error("TM-X: คำสั่ง %r ล้มเหลวหลังลองใหม่แล้ว", command)
    return None


async def tmx_send_command(command: str) -> Optional[str]:
    async with _tmx_lock:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _tmx_send_command_sync, command)


def _parse_gm_response(response: str) -> list:
    """แกะ response ของคำสั่ง "GM,0,0" ออกมาเป็น list ของค่าที่ใช้งานได้จริง

    ตามที่ยืนยันจาก script ทดสอบกับเครื่องจริง: TM-X ตอบกลับเป็นค่าคั่นด้วย
    comma หลายค่า บางแกนที่ยังไม่มีชิ้นงาน/ยังไม่ settle จะเป็น "9999.999"
    (placeholder) — ต้องกรองทิ้ง ส่วนเครื่องหมาย +/- ที่นำ/ต่อท้ายตัวเลขก็ต้อง
    strip ออกก่อน (พฤติกรรมเดิมของ script ทดสอบ คือเก็บแค่ magnitude ไม่สนใจ
    เครื่องหมาย) ค่า 2 ตัวสุดท้ายที่เหลือหลังกรองคือค่าที่ใช้เป็น value_x/value_y
    """
    values = []
    for token in response.split(","):
        token = token.strip().strip("-").strip("+")
        if token == "9999.999" or not token:
            continue
        values.append(token)
    return values


# ── FTP server (รับรูปจากกล้อง Keyence เข้ามาที่ TEMP_IMAGE_DIR) ─────────────
# หมายเหตุ: ย้ายมาจาก run_ftp_server.py เดิม — รวมเข้าไฟล์เดียวเพื่อไม่ต้องรัน
# 2 โปรเซสแยกกัน (agent.py ตัวเดียวก็รับรูป + คุย TM-X + คุย backend ครบ)
#
# pyftpdlib ใช้ asyncore loop ของตัวเอง (server.serve_forever() เป็น blocking
# call) เอามาผสมกับ asyncio event loop ของ FastAPI ตรงๆ ไม่ได้ จึงต้องรันใน
# thread แยกต่างหาก (ไม่ใช้ thread pool เดียวกับ run_in_executor ของ TM-X/รูป
# กันแย่ง worker thread กัน) เป็น daemon thread เพื่อให้ปิดพร้อมโปรเซสหลักเอง
def _start_ftp_server_sync() -> None:
    from pyftpdlib.authorizers import DummyAuthorizer
    from pyftpdlib.handlers import FTPHandler
    from pyftpdlib.servers import FTPServer

    # pyftpdlib ต้องการให้โฟลเดอร์ที่ระบุมีอยู่จริงก่อน ถึงจะ add_user ได้
    os.makedirs(TEMP_IMAGE_DIR, exist_ok=True)

    authorizer = DummyAuthorizer()
    authorizer.add_user(FTP_USER, FTP_PASSWORD, TEMP_IMAGE_DIR, perm="elradfmw")

    handler = FTPHandler
    handler.authorizer = authorizer

    server = FTPServer((FTP_HOST, FTP_PORT), handler)
    log.info("FTP: server started on %s:%d — saving images to %s", FTP_HOST, FTP_PORT, TEMP_IMAGE_DIR)
    print(f"FTP Server is running on {FTP_HOST}:{FTP_PORT} — saving images to {TEMP_IMAGE_DIR}")
    server.serve_forever()


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


# ── Real measurement flow (คุยกับ TM-X จริงผ่าน TCP) ─────────────────────────
async def real_single_measurement(index: int) -> None:
    """ทำ 1 รอบของการวัดจริง:
      1. รอ operator กด Enter ที่ terminal ของ Agent เพื่อบอกว่าวางชิ้นงานพร้อมแล้ว
      2. ส่ง "GM,0,0" ขอค่าผลวัด แล้วแกะเอา 2 ค่าสุดท้ายที่ไม่ใช่ 9999.999 มาเป็น
         value_x/value_y — ลองใหม่ได้สูงสุด 5 ครั้งถ้ายังไม่ settle
      3. POST เข้า backend + หา/อัปโหลดรูป เหมือน flow เดิมทุกอย่าง
    """
    global current_session_id, current_number_alpl

    print(f"\n>>> พร้อมวัดชิ้นที่ {index}/{current_target_count} — วางชิ้นงานแล้วกด Enter:")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, input)

    value_x = value_y = None
    for attempt in range(1, 6):
        response = await tmx_send_command("GM,0,0")
        if response:
            parsed = _parse_gm_response(response)
            if len(parsed) >= 2:
                value_x, value_y = parsed[-2], parsed[-1]
                break
        log.info("Measurement #%d: ยังไม่ได้ค่าที่ใช้ได้ (ลองครั้งที่ %d/5)", index, attempt)
        await asyncio.sleep(0.3)

    if value_x is None or value_y is None:
        log.error("Measurement #%d: ไม่ได้ค่าจาก TM-X (ยังเป็น 9999.999 ทั้งหมดหลังลอง 5 ครั้ง) — ข้ามรอบนี้", index)
        return

    try:
        m = MeasurementData(value_x=float(value_x), value_y=float(value_y))
    except Exception as exc:
        log.error("Measurement #%d: แปลงค่า (%s, %s) เป็นตัวเลขไม่ได้: %s", index, value_x, value_y, exc)
        return

    log.info("Measurement #%d: TM-X ส่งค่า x=%.3f, y=%.3f", index, m.value_x, m.value_y)

    # หารูปคู่กับการวัดนี้ (เหมือนเดิมทุกอย่าง — ไม่แก้ส่วนนี้)
    image_path = await get_new_image()

    # client_uuid: สร้างครั้งเดียวต่อการวัดนี้ แล้วใช้ตัวเดิมซ้ำทุกครั้งที่ retry
    # ด้านล่าง — เป็น idempotency key ให้ backend เช็คได้ว่าเป็น request เดิม
    # ที่ retry มา ไม่ใช่การวัดครั้งใหม่ (กัน insert ซ้ำถ้ารอบก่อน backend
    # บันทึกสำเร็จไปแล้วจริงๆ แต่ response หลุดหายระหว่างทางกลับมา)
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
        except Exception as exc:
            log.warning("Measurement #%d: POST /api/measurements ล้มเหลว (ครั้งที่ %d/3): %s", index, attempt, exc)
            if attempt < 3:
                await asyncio.sleep(2)

    if data is None:
        log.error(
            "Measurement #%d: POST /api/measurements ล้มเหลวหลังลอง 3 ครั้ง — "
            "ค่า x=%.3f, y=%.3f ของชิ้นนี้ไม่ถูกบันทึก ต้องวัดชิ้นนี้ซ้ำเอง",
            index, m.value_x, m.value_y,
        )
        return

    measurement_id = data["measurement_id"]
    log.info(
        "Measurement #%d: measurement_id=%d ALPL=%s result=%s measured=%d/%d status=%s",
        index, measurement_id, data.get("number_alpl"), data["result"],
        data["measured"], data["target"], data["status"],
    )

    if image_path:
        task = asyncio.create_task(upload_image(image_path, measurement_id))
        _pending_uploads.append(task)
        task.add_done_callback(_pending_uploads.remove)


async def real_measurement_flow() -> None:
    """แทนที่ mock flow เดิมด้วยการคุยกับ TM-X จริงผ่าน TCP ตามโปรโตคอลที่ทดสอบ
    สำเร็จแล้ว: R0 (reset) → PW,<template args> (โหลด template) → วนขอ GM,0,0
    ทีละชิ้นจนครบ target_count → S0 (stop) ตอนจบ (ทั้งจบปกติหรือโดนสั่ง stop
    กลางทางก็ตาม ใช้ try/finally คุมให้ส่ง S0 เสมอ)

    หมายเหตุสำคัญ: current_template_name ที่ backend ส่งมา "คือ" ค่าที่ต่อท้าย
    "PW," ตรงๆ อยู่แล้ว (เช่น "3,021") ตามที่ตกลงกันไว้ — ไม่ต้องแปลง/แม็พอะไร
    เพิ่ม ถ้า package_size.template_name ใน DB เก็บไม่ตรง format นี้ ต้องแก้ที่
    DB ไม่ใช่ที่นี่
    """
    global is_running

    print(f"✅ ได้รับคำสั่ง Start — session_id={current_session_id}, template_name={current_template_name!r}")
    log.info("Real start: session=%s template=%r target_count=%s",
              current_session_id, current_template_name, current_target_count)

    if not current_template_name:
        log.error("Real flow: ไม่มี template_name ส่งมา — ยกเลิกการวัด")
        async with _state_lock:
            is_running = False
        return

    try:
        await tmx_send_command("R0")
        await asyncio.sleep(0.5)

        await tmx_send_command(f"PW,{current_template_name}")
        log.info("Waiting for program to load…")
        await asyncio.sleep(1.0)

        for i in range(1, (current_target_count or 0) + 1):
            if not is_running:
                log.warning("Real flow: ถูกสั่ง stop กลางทาง (รอบที่ %d/%d) — หยุดเลย", i, current_target_count)
                break
            await real_single_measurement(i)
    finally:
        await tmx_send_command("S0")

    async with _state_lock:
        is_running = False

    # รอ upload รูปที่ยังค้างอยู่ให้เสร็จก่อน cleanup
    if _pending_uploads:
        log.info("Waiting for %d upload(s) to finish before cleanup…", len(_pending_uploads))
        await asyncio.gather(*_pending_uploads, return_exceptions=True)
    _cleanup_temp_images()

    print(f"✅ Done — วัดครบ {current_target_count} ชิ้นแล้ว (session_id={current_session_id})")
    log.info("Real flow done: session=%s", current_session_id)


# ── Start / Stop flows ────────────────────────────────────────────────────────
async def start_flow() -> None:
    await real_measurement_flow()


async def stop_flow() -> None:
    global is_running
    log.info("Stop flow: หยุดการวัด (real flow จะเช็ค is_running แล้วหยุดเองที่รอบถัดไป)")
    async with _state_lock:
        is_running = False


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


# ── Main: run everything concurrently ─────────────────────────────────────────
async def main() -> None:
    _cleanup_temp_images()

    threading.Thread(target=_start_ftp_server_sync, daemon=True, name="ftp-server").start()

    server = uvicorn.Server(
        uvicorn.Config(http_app, host="0.0.0.0", port=AGENT_PORT, log_level="info")
    )

    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())