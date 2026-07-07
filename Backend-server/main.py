# Backend-server/main.py
# How to run:
#   cd Backend-server
#   pip install -r requirements.txt
#   uvicorn main:app --reload --host 0.0.0.0 --port 8000

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from io import StringIO
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import pymysql
import pymysql.cursors
import urllib3
from pymysql.constants import CLIENT
from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from minio import Minio
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Config ──────────────────────────────────────────────────────────────────
DB_CONFIG = dict(
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("DB_NAME", "tmx_db"),
    port=int(os.getenv("DB_PORT", 3307)),
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
    # CLIENT.FOUND_ROWS: ค่า default ของ MySQL/pymysql คือ cur.rowcount หลัง UPDATE
    # จะนับเฉพาะ "แถวที่ค่าจริงเปลี่ยน" ไม่ใช่ "แถวที่ WHERE เจอ" — ทำให้กด Save โดย
    # ไม่แก้อะไรเลย (ส่ง payload ค่าเดิมกลับมา) แล้ว rowcount == 0 ทั้งที่แถวมีอยู่จริง
    # โค้ดที่เช็ค `if cur.rowcount == 0: raise 404 not found` (update_part,
    # update_measurement) เลยฟ้อง "not found" หลอกๆ ตั้ง flag นี้เพื่อให้ rowcount
    # นับจากแถวที่ WHERE จับคู่เจอแทน ทำให้เช็ค 404 เดิมถูกต้องอีกครั้ง
    client_flag=CLIENT.FOUND_ROWS,
)

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY")
if not MINIO_SECRET:
    raise ValueError("MINIO_SECRET_KEY environment variable is required")
MINIO_BUCKET    = "tmx-images"

AGENT_PORT      = int(os.getenv("AGENT_PORT", 9998))
AGENT_BASE_URL  = f"http://localhost:{AGENT_PORT}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [Server] %(message)s")
log = logging.getLogger(__name__)

# ── SSE broadcast queue ──────────────────────────────────────────────────────
subscribers: List[asyncio.Queue] = []

# ── In-memory queue state สำหรับ session แบบ IPM/New ─────────────────────────
# เก็บ "คิว" ALPL + ตำแหน่งปัจจุบันของ session ที่เริ่มจาก Part Entry card
# (โหมด IPM/New) — เป็นตัวแปร memory ธรรมดา ไม่ใช่ column ใน DB เลย เพราะ
# schema ของ `sessions` ไม่มีที่เก็บลำดับ ALPL ทั้งคิว มีแค่ number_alpl ตัวเดียว
# (ที่เราใส่เป็น ALPL ตัวแรกในคิวไปแทน) — ถ้า server restart กลางที่ session
# กำลัง running อยู่ คิวนี้จะหาย (ยอมรับความเสี่ยงนี้ได้ตามที่คุยกันไว้)
#
# โครงสร้าง: { session_id: {"entry_mode": "IPM"|"New", "queue": [1011, 1002, ...],
#                            "position": 0} }
session_queues: Dict[int, Dict[str, Any]] = {}


async def push_event(event_type: str, data: dict) -> None:
    """กระจาย (broadcast) เหตุการณ์ SSE หนึ่งรายการไปให้ทุก client ที่เปิดหน้า dashboard อยู่

    ทำไมต้องทำแบบนี้: backend คือ Single Source of Truth ของสถานะ session/measurement
    ดังนั้นเมื่อสถานะมีการเปลี่ยนแปลง (เริ่ม session, มี measurement ใหม่, timeout ฯลฯ)
    ทุกแท็บ dashboard ที่เปิดอยู่ต้องรู้ทันที แต่ละ subscriber มี asyncio.Queue ของตัวเอง
    (ดู sse_stream ด้านล่าง) เราแค่ส่ง payload เดียวกันลงไปในทุกคิว SSE ไหลทางเดียวจาก
    server ไป client เท่านั้น (ไม่ใช่ request/response แบบ 2 ทาง)
    """
    payload = json.dumps(data, default=str)
    for q in subscribers:
        await q.put({"event": event_type, "data": payload})
    log.info("SSE ▶ %s: %s", event_type, payload)


# ── DB / MinIO helpers ───────────────────────────────────────────────────────
def get_db():
    """เปิด MySQL connection ใหม่สำหรับ 1 request

    ทำไมต้องเปิดใหม่ทุกครั้งแทนใช้ connection pool: นี่คือระบบที่ deploy บน PC เดียว
    มี concurrency ต่ำ ความเรียบง่ายของ "connect → ใช้งาน → close" จึงคุ้มกว่าความซับซ้อน
    ของการทำ pool ทุก endpoint ด้านล่างจะเปิด connection นี้ใน try/finally แล้วปิดเมื่อใช้เสร็จ
    """
    return pymysql.connect(**DB_CONFIG)


def get_minio() -> Minio:
    """สร้าง MinIO client สำหรับเก็บ/ดึงรูปภาพการตรวจสอบ (inspection images)

    ทำไมต้องตั้ง connect timeout สั้น: ถ้า MinIO ล่มหรือเข้าไม่ถึง อยากให้ request fail
    เร็ว (ภายในไม่กี่วินาที) แทนที่จะค้างเป็นนาทีจนไป block event loop ของแอป
    """
    http_client = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=3, read=10),
        retries=urllib3.Retry(total=1, backoff_factor=0),
    )
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,
        http_client=http_client,
    )


def ensure_bucket() -> None:
    """สร้าง MinIO bucket สำหรับเก็บรูป measurement ถ้ายังไม่มี

    ทำไม: เหตุการณ์นี้ต้องเกิดแค่ครั้งเดียว (เช่นรันครั้งแรก หรือ MinIO instance ใหม่)
    จึงเช็คก่อนว่ามี bucket อยู่แล้วหรือยัง ก่อนจะสร้าง ไม่ใช่สร้างทับซ้ำทุกครั้ง
    """
    mc = get_minio()
    if not mc.bucket_exists(MINIO_BUCKET):
        mc.make_bucket(MINIO_BUCKET)
        log.info("Created MinIO bucket: %s", MINIO_BUCKET)


# ── Lifespan ─────────────────────────────────────────────────────────────────
async def _init_bucket_bg() -> None:
    """พยายามสร้าง MinIO bucket แบบ background — ไม่ block การ startup ของแอป

    ทำไม: ถ้า MinIO เริ่มช้าหรือเข้าไม่ถึงชั่วคราว เราไม่อยากให้ทั้งแอป FastAPI
    boot ไม่ขึ้นเพราะเรื่องนี้ จึงรันเป็น fire-and-forget task ที่มี timeout สั้นๆ
    ของตัวเอง ถ้า MinIO มีปัญหาก็แค่ log warning ไม่ทำให้ server crash
    """
    try:
        loop = asyncio.get_event_loop()
        await asyncio.wait_for(loop.run_in_executor(None, ensure_bucket), timeout=6)
        log.info("MinIO bucket ready")
    except Exception as exc:
        log.warning("MinIO init failed (not fatal): %s", exc)


async def _reload_session_queues() -> None:
    """โหลด session_queues กลับเข้า memory จากคอลัมน์ sessions.queue_state

    ทำไมต้องมี: session_queues เดิมอยู่ใน memory ของ backend ล้วนๆ ถ้า backend
    ถูก restart (reload ตอน dev, crash แล้ว auto-restart, deploy ใหม่) ระหว่างที่
    มี session แบบ IPM/New กำลัง running อยู่ คิวจะหายไปจาก memory ทันที —
    create_measurement หลังจากนั้นจะ fallback ไปใช้ req.number_alpl ที่ Agent
    ส่งมา ซึ่งเป็น ALPL ตัวแรกในคิวเสมอ (Agent ไม่เคยอัปเดตค่านี้เอง) ทำให้ทุก
    measurement ที่เหลือถูกบันทึกผิด ALPL ไปเรื่อยๆ แบบไม่มี error เตือนเลย

    จึงต้องรันตรงนี้ (ก่อน yield ให้แอปเริ่มรับ request) — ต้อง await ตรงๆ ไม่ใช่
    fire-and-forget แบบ _init_bucket_bg เพราะต้องมั่นใจว่า session_queues ถูก
    เติมกลับให้ครบก่อนที่ request แรกจาก Agent จะเข้ามาได้
    """
    try:
        db = get_db()
        try:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT session_id, queue_state FROM sessions "
                    "WHERE state = 'running' AND queue_state IS NOT NULL"
                )
                rows = cur.fetchall()
        finally:
            db.close()

        for row in rows:
            try:
                session_queues[row["session_id"]] = json.loads(row["queue_state"])
                log.info("Restored queue_state for session %s from DB", row["session_id"])
            except Exception as exc:
                log.warning("Failed to parse queue_state for session %s: %s", row["session_id"], exc)
    except Exception as exc:
        # DB อาจยังไม่พร้อมตอน boot — อย่าทำให้แอปบูตไม่ขึ้นเพราะเรื่องนี้ แค่
        # log ไว้ (session ที่ running อยู่ตอน restart แบบนี้จะพลาดการกู้คืนคิว
        # แต่ยังใช้งานต่อได้ปกติถ้าไม่ใช่ queue-based หรือกด Stop แล้วเริ่มใหม่)
        log.warning("Reload session_queues failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Hook ตอน FastAPI เริ่มทำงาน (startup) และตอนปิด (shutdown)

    ทำไม: ตรงนี้คือจุดที่ background task (สร้าง MinIO bucket) ถูกสั่งให้เริ่ม
    ทำงานตอนแอป boot แทนที่จะไปสั่งเริ่มภายใน request handler ส่วน
    _reload_session_queues() ต้อง await ให้เสร็จก่อน yield เพราะต้องกู้คืนคิว
    ให้ครบก่อนรับ request

    หมายเหตุ: เดิมมี heartbeat_checker() คู่กันด้วย (ตรวจ Agent เงียบหายกลาง
    session แล้ว mark เป็น 'timeout') — เอาออกแล้วเพราะ Agent/Backend/Web รันอยู่
    บน Pi เครื่องเดียวกันหมด (localhost) ค่าที่มันเคยป้องกัน (network/สถานีหลุด
    ระหว่างเครื่อง) ไม่ค่อยมีความหมายแล้ว ยอมรับความเสี่ยงที่เหลืออยู่ (ถ้า Agent
    process ตาย/แฮงค์กลาง session ตัว session จะค้างเป็น 'running' ตลอดไปจนกว่า
    จะกด Stop เองจาก dashboard) ตามที่ตกลงกันไว้
    """
    asyncio.create_task(_init_bucket_bg())       # fire-and-forget, never blocks
    await _reload_session_queues()               # ต้องเสร็จก่อนรับ request
    yield


app = FastAPI(title="TM-X Backend Server", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════════════
# SSE Stream
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/stream")
async def sse_stream(request: Request):
    """Endpoint SSE ที่ dashboard เชื่อมต่อเข้ามาเพื่อรับข้อมูล real-time

    ทำไม: แทนที่ frontend จะ poll backend ทุกวินาที มันเปิด connection ค้างไว้ทีเดียว
    ที่นี่ แล้วเรา push event ไปให้ตอนมันเกิดขึ้นจริง (ดู push_event) แต่ละ client
    จะมี queue ของตัวเองที่ลงทะเบียนใน `subscribers` เราจะ yield "ping" ทุก 25 วินาที
    ตอนไม่มีอะไรใหม่ แค่เพื่อ keep connection ไว้ไม่ให้ proxy/browser ตัดการเชื่อมต่อ
    ที่ idle อยู่ SSE เป็นทางเดียว (server → client เท่านั้น) — ฝั่ง frontend ยังใช้
    POST request ปกติในการส่งคำสั่งไปที่ backend
    """
    async def generator():
        queue: asyncio.Queue = asyncio.Queue()
        subscribers.append(queue)
        log.info("SSE client connected  (total=%d)", len(subscribers))
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=25)
                    yield event
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": ""}
        finally:
            if queue in subscribers:
                subscribers.remove(queue)
            log.info("SSE client disconnected (total=%d)", len(subscribers))

    return EventSourceResponse(generator())


# ══════════════════════════════════════════════════════════════════════════════
# Session endpoints
# ══════════════════════════════════════════════════════════════════════════════
class StopSessionRequest(BaseModel):
    session_id: int


@app.get("/api/session/state")
async def get_session_state():
    """คืนสถานะปัจจุบันของ session ล่าสุด

    ทำไม: ตอน dashboard โหลดครั้งแรก (หรือ refresh) มันต้องรู้ว่า "มี run การวัด
    กำลังทำงานอยู่ไหม" ก่อนที่ SSE connection จะเปิดเสียอีก นี่คือ snapshot
    แบบครั้งเดียวที่ใช้ sync ตอนเริ่ม หลังจากนั้น SSE event จะคอยอัปเดตให้ real-time
    """
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT session_id, number_alpl, state, target_count, measured_count, "
                "last_seen, started_at, ended_at "
                "FROM sessions ORDER BY session_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        return row or {"state": "idle"}
    finally:
        db.close()


# หมายเหตุ: เดิมมีฟังก์ชัน _insert_new_parts_from_payload() ที่ insert Part
# "ทุกตัวในคิว" ทีเดียวตอน start_session — เปลี่ยนพฤติกรรมแล้ว (ดู start_session
# และ create_measurement) เพราะถ้า user กด Stop กลางคัน ALPL ที่ยังไม่ทันวัดจะ
# ค้างเป็น Part "ผี" อยู่ใน DB ทั้งที่ไม่เคยมีการวัดจริงเกิดขึ้นเลย ตอนนี้จึง
# insert Part แค่ตัวแรกตอน start_session (จำเป็นเพราะ sessions.number_alpl มี
# FK ไป parts ต้องมี row อยู่ก่อนถึงจะ insert sessions ได้) ส่วนตัวที่เหลือใน
# คิวจะถูก insert ทีละตัว "ตอนได้ผลวัดจริงจาก Agent" ใน create_measurement เท่านั้น


def _get_template_name_for_ipm(cur, first_alpl: int) -> str:
    """Query หา template_name — ใช้เฉพาะกรณี IPM เพราะ JSON ของ IPM ไม่มี
    template_name ส่งมาด้วย ต่างจาก New ที่ frontend ส่ง template_name มาใน
    ก้อน JSON เลย ไม่ต้อง query

    schema ใหม่: template_name ไม่ได้อยู่ใน `parts` ตรงๆ อีกต่อไป — ย้ายไปอยู่
    ใน `package_size` (1 package_size ผูกกับ 1 template_name) จึงต้อง join
    ผ่าน package_size_id ของ part นั้น

    ใช้ ALPL ตัวแรกในคิวเป็นตัวหา เพราะ IPM ต้องมี parts row อยู่แล้วทุกตัว
    (ลงทะเบียนไว้ก่อนหน้านี้) — ถ้าหาไม่เจอ แปลว่า frontend เช็คตกหรือมีคน
    ลบ part ออกไปหลัง frontend เช็คผ่าน ให้ raise error ชัดๆ ไปเลย ไม่เดา
    """
    cur.execute(
        "SELECT ps.template_name FROM parts p "
        "JOIN package_size ps ON p.package_size_id = ps.package_size_id "
        "WHERE p.number_alpl = %s",
        (first_alpl,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(
            404,
            f"ALPL {first_alpl} ยังไม่ได้ลงทะเบียนชิ้นงานนี้ หรือยังไม่ได้ตั้ง package_size "
            f"ให้ part นี้ (หา template_name ไม่เจอ)",
        )
    return row["template_name"]


async def _notify_agent_start(
    session_id: int,
    template_name: str,
    target_count: int,
    number_alpl: int,
) -> None:
    """ยิง POST ไปที่ Agent (`agent.py`) เพื่อบอกให้เริ่มวัด

    ใช้ร่วมกันทั้ง New และ IPM — สิ่งที่ Agent ต้องรู้เหมือนกันทุกกรณีคือ
    session_id, template_name (ให้โหลดเข้า TM-X), target_count, และ
    number_alpl ตัวแรกที่จะวัด (ตัวต่อไปในคิว Agent ไม่จำเป็นต้องรู้ เพราะ
    มันแค่ส่ง value_x/value_y มาเรื่อยๆ โดยไม่ต้องสนใจว่าเป็น ALPL ไหน —
    backend เป็นคนจับคู่กับ ALPL เองจากตำแหน่งในคิว)

    ครอบด้วย try/except เพราะถ้า Agent ไม่ได้รันอยู่ (หรือตอบช้าเกิน timeout)
    httpx จะ raise exception ที่ FastAPI ไม่รู้จัก (ConnectError/TimeoutException)
    — ถ้าปล่อยให้ exception นี้หลุดออกไปจากเอนด์พอยต์โดยไม่ catch จะกลายเป็น
    unhandled exception (500) ที่บางครั้งไม่มี CORS header แนบมาด้วย ทำให้
    browser เข้าใจผิดว่าเป็น CORS error ทั้งที่จริงๆคือ Agent ไม่ตอบ — log
    warning ไว้เฉยๆ แล้วให้ session ใน DB ยัง 'running' ต่อไปได้ (เหมือนที่
    stop_session ทำไว้อยู่แล้วตอน notify agent ตอน stop)
    """
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{AGENT_BASE_URL}/command",
                json={
                    "action": "start",
                    "session_id": session_id,
                    "template_name": template_name,
                    "target_count": target_count,
                    "number_alpl": number_alpl,
                },
                timeout=10,
            )
    except Exception as exc:
        log.warning("Agent start notify failed: %s", exc)


@app.post("/api/session/start")
async def start_session(request: Request):
    """เริ่ม session การวัดใหม่ จาก Part Entry card (โหมด IPM หรือ New)

    **กรณี Measure_Type == "New"** (ลงทะเบียน Part ใหม่ + วัดในรอบเดียว):
      1. Insert Part เฉพาะ "ตัวแรก" ในคิวก่อน (ต้องทำก่อน insert session เพราะ
         sessions.number_alpl มี FOREIGN KEY ไป parts — ถ้า insert session
         ก่อนโดย ALPL ยังไม่มีอยู่จริง MySQL จะปฏิเสธทันที) — ALPL ตัวที่เหลือใน
         คิวจะ insert ทีละตัว "ตอนได้ผลวัดจริงจาก Agent" ใน create_measurement
         แทน ไม่ insert รวดเดียวทั้งคิว กันกรณีกด Stop กลางคันแล้วมี Part ที่
         ไม่เคยวัดจริงค้างอยู่ใน DB
      2. Insert sessions row (number_alpl = ALPL ตัวแรกในคิว) → ได้ session_id
      3. เก็บคิว ALPL ทั้งหมด + ตำแหน่งเริ่มต้น (0) + config ของ Part (ไว้ insert
         ตัวถัดๆไปแบบ lazy) ไว้ใน session_queues (memory)
      4. Notify Agent ให้เริ่มวัด (template_name มาจาก JSON ตรงๆ ไม่ query DB)

    **กรณี Measure_Type == "IPM"** (ALPL ลงทะเบียนไว้แล้ว):
      1. Query DB หา template_name จาก ALPL ตัวแรกในคิว (ไม่ insert parts เลย)
      2. Insert sessions row → ได้ session_id
      3. เก็บคิวไว้ใน session_queues เหมือนกัน
      4. Notify Agent

    ทั้ง 2 กรณี — Agent ไม่ต้องรู้ความต่างเลย ได้รับ payload หน้าตาเดียวกัน
    (action/session_id/template_name/target_count/number_alpl ตัวแรก) ส่วน
    การ map ALPL ตัวต่อๆไปในคิวเข้ากับ measurement ที่จะตามมา เป็นเรื่องที่
    backend จัดการเองทั้งหมดผ่าน session_queues (ดู create_measurement)

    หมายเหตุ (เพิ่มเข้ามาทีหลัง): Race condition ตอนกด Start ซ้ำเร็วๆ — ครอบ
    check+insert ด้วย MySQL GET_LOCK/RELEASE_LOCK กันสอง request แข่งกันผ่าน
    Button Guard พร้อมกันได้ (เดิมเช็คแล้ว insert คนละคำสั่ง ไม่มีอะไรล็อก
    ระหว่างนั้นเลย)
    """
    data = await request.json()
    log.info("📥 ได้รับ payload จาก /api/session/start:\n%s", json.dumps(data, ensure_ascii=False, indent=2))

    measure_type = data.get("Measure_Type")
    if measure_type not in ("New", "IPM"):
        raise HTTPException(400, "Measure_Type ต้องเป็น 'New' หรือ 'IPM'")

    # parse number_alpl อย่างระมัดระวัง — ถ้า key หายไปหรือมีค่าที่แปลงเป็น
    # int ไม่ได้ จะได้ KeyError/ValueError ซึ่งเป็น raw exception (ไม่ใช่
    # HTTPException) ที่ FastAPI ไม่รู้จัก ทำให้ response กลายเป็น 500 แบบ
    # ไม่มี CORS header แนบไปด้วย (เกิดปัญหาเดียวกับที่เจอใน _notify_agent_start
    # ก่อนหน้านี้) จึงต้อง catch แล้ว raise เป็น HTTPException ให้ชัดเจน
    try:
        alpl_queue = [int(x) for x in data["number_alpl"]]
    except KeyError:
        raise HTTPException(400, "ต้องมี field 'number_alpl' ใน payload")
    except (ValueError, TypeError):
        raise HTTPException(400, "number_alpl ต้องเป็น array ของเลขจำนวนเต็มทั้งหมด")

    if not alpl_queue:
        raise HTTPException(400, "number_alpl ต้องมีอย่างน้อย 1 ค่า")
    first_alpl = alpl_queue[0]
    target_count = len(alpl_queue)

    db = get_db()
    try:
        # GET_LOCK ครอบทั้ง Button Guard + insert — ให้ทั้งสองเป็น atomic
        # section เดียวกันจริงๆ ในระดับ DB (ไม่ใช่แค่ระดับ Python) กันสอง
        # request "Start" ที่มาถึงพร้อมกันเป๊ะๆ ผ่าน check ทั้งคู่ก่อนจะมีใคร
        # insert ทัน — timeout 5 วิ พอสำหรับ critical section สั้นๆ นี้
        with db.cursor() as cur:
            cur.execute("SELECT GET_LOCK('tmx_start_session', 5) AS got")
            if not cur.fetchone()["got"]:
                raise HTTPException(503, "ระบบกำลังประมวลผลคำสั่ง Start อื่นอยู่ ลองใหม่อีกครั้ง")

        try:
            with db.cursor() as cur:
                # Button Guard — กันรัน 2 session ซ้อนกัน (เหมือนของเดิมก่อนหน้านี้)
                cur.execute("SELECT session_id FROM sessions WHERE state = 'running'")
                if cur.fetchone():
                    raise HTTPException(400, "A session is already running")

                if measure_type == "New":
                    # 1) Insert Part เฉพาะ "ตัวแรก" ในคิวก่อน (ต้องมาก่อน insert
                    # session เพราะ FK — sessions.number_alpl ต้องมี Part อยู่จริง
                    # ก่อนถึงจะ insert ได้) ส่วน ALPL ตัวที่เหลือในคิวจะถูก insert
                    # ทีละตัว "ตอนได้ผลวัดจริง" ใน create_measurement แทน ไม่ insert
                    # รวดเดียวทั้งคิวแบบเดิม — กันกรณีกด Stop กลางคันแล้วมี Part ที่
                    # ไม่เคยวัดจริงค้างอยู่ใน DB
                    #
                    # ครอบ try/except เพราะ MySQL อาจ throw error ได้หลายแบบตอน
                    # insert (ALPL ซ้ำ, ข้อมูลผิด type, ค่ายาวเกิน column ฯลฯ) —
                    # จับ pymysql.MySQLError (base class ของ error ทุกชนิดจาก MySQL)
                    # ไม่ใช่แค่ IntegrityError ตัวเดียว เพื่อกัน raw exception หลุด
                    # ออกไปทำให้ response 500 ไม่มี CORS header แนบมา
                    try:
                        _insert_part_row(cur, first_alpl, data)
                    except pymysql.MySQLError as exc:
                        raise HTTPException(409, f"Insert Part แรกในคิวไม่สำเร็จ: {exc}")
                    template_name = data.get("template_name")
                else:
                    # IPM: ไม่ insert parts เลย แค่ query หา template_name
                    template_name = _get_template_name_for_ipm(cur, first_alpl)

                # 2) Insert sessions row (ผ่าน FK ได้แน่นอนแล้ว ไม่ว่าจะ New หรือ IPM)
                cur.execute(
                    "INSERT INTO sessions (number_alpl, state, target_count, measured_count) "
                    "VALUES (%s, 'running', %s, 0)",
                    (first_alpl, target_count),
                )
                session_id = cur.lastrowid
        finally:
            with db.cursor() as cur:
                cur.execute("SELECT RELEASE_LOCK('tmx_start_session')")

        # 3) เก็บคิวไว้ใน memory ผูกกับ session_id นี้ (หลัง insert สำเร็จแล้ว
        # ค่อยผูก กัน insert fail แล้วมี state ค้างอยู่ใน session_queues)
        # operator/note มาจาก field "Operator"/"Note" ใน payload — ใช้ได้ทั้ง
        # IPM และ New (สำหรับ New เป็น note ของ "การวัดรอบนี้" ไม่ใช่ของตัว
        # part เพราะ parts table ไม่มี column note เลย)
        queue_state = {
            "entry_mode": measure_type,
            "queue": alpl_queue,
            "position": 0,
            "operator": data.get("Operator"),
            "note": data.get("Note"),
            # เก็บ config เดิมของ New ไว้ (part_number/handler/vendor/category/
            # package_size/owner ฯลฯ) ให้ create_measurement เอาไป insert Part
            # ตัวถัดๆไปในคิวแบบ lazy ทีละตัวตอนวัดจริง — None ถ้าเป็น IPM (ไม่ต้อง
            # insert Part ใหม่เลย เพราะ IPM ใช้ Part ที่ลงทะเบียนไว้แล้วทั้งหมด)
            "new_part_config": data if measure_type == "New" else None,
        }
        session_queues[session_id] = queue_state

        # เขียนสำเนา queue_state ลง DB ด้วย (คอลัมน์ sessions.queue_state) — ถ้า
        # backend restart กลาง session นี้ จะโหลดกลับเข้า memory ได้ตอน boot
        # แทนที่จะ fallback ไปใช้ ALPL ตัวแรกผิดๆ ตลอดที่เหลือ (ดู create_measurement
        # และ lifespan())
        with db.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET queue_state = %s WHERE session_id = %s",
                (json.dumps(queue_state), session_id),
            )

        # 4) Notify Agent ให้เริ่มวัด
        await _notify_agent_start(session_id, template_name, target_count, first_alpl)

        await push_event(
            "session_started",
            {
                "session_id": session_id,
                "number_alpl": first_alpl,
                "template_name": template_name,
                "target_count": target_count,
            },
        )
        return {"session_id": session_id, "template_name": template_name, "target_count": target_count}
    finally:
        db.close()


@app.post("/api/session/stop")
async def stop_session(req: StopSessionRequest):
    """หยุด session ที่กำลัง running จากปุ่ม Stop บน dashboard

    ทำไมเรื่องนี้สำคัญ: นี่คือ path "web-initiated stop" — มันอัปเดต DB
    (state='stopped', ended_at=NOW()) แล้วบอก Agent ให้หยุด ซึ่งต่างจากปุ่ม
    Stop ทางกายภาพที่ MCU (ในการ implement ปัจจุบันของ Agent) ที่แค่ flip
    flag ใน memory ฝั่ง Agent โดยไม่แตะ DB เลย — เป็นความไม่สมดุล (asymmetry)
    ที่รู้กันอยู่ระหว่าง stop ทั้ง 2 path นี้
    """
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT state FROM sessions WHERE session_id = %s", (req.session_id,))
            if not cur.fetchone():
                raise HTTPException(404, "Session not found")
            cur.execute(
                "UPDATE sessions SET state = 'stopped', ended_at = NOW() "
                "WHERE session_id = %s",
                (req.session_id,),
            )

        async with httpx.AsyncClient() as client:
            try:
                await client.post(
                    f"{AGENT_BASE_URL}/command", json={"action": "stop"}, timeout=10
                )
            except Exception as exc:
                log.warning("Agent stop notify failed: %s", exc)

        session_queues.pop(req.session_id, None)  # กดหยุดเองก่อนคิวหมด ก็เคลียร์ memory ทิ้งด้วย
        await push_event("session_stopped", {"session_id": req.session_id})
        return {"ok": True}
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Lookup endpoints (dropdown data สำหรับ index.html / edit.html)
# ══════════════════════════════════════════════════════════════════════════════
# Dropdown ทุกตัวนี้เป็นแบบ "ปิด" (closed) — frontend เลือกได้เฉพาะค่าที่มีอยู่
# จริงใน DB เท่านั้น ไม่มีช่องพิมพ์เพิ่มค่าใหม่ในฟอร์ม ถ้าต้องเพิ่ม
# owner/vendor/handler/category/operator ใหม่ ต้อง insert ตรงเข้า DB เอง
# (ตามที่คุยกันไว้ — ไม่ทำ "add new" inline ในฟอร์ม)
@app.get("/api/operators")
async def list_operators():
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT operator_id, operator_name FROM operator ORDER BY operator_name")
            return cur.fetchall()
    finally:
        db.close()


@app.get("/api/owners")
async def list_owners():
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT owner_id, owner_name FROM owner ORDER BY owner_name")
            return cur.fetchall()
    finally:
        db.close()


@app.get("/api/vendors")
async def list_vendors():
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT vendor_id, vendor_name FROM vendor ORDER BY vendor_name")
            return cur.fetchall()
    finally:
        db.close()


@app.get("/api/handlers")
async def list_handlers():
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT handler_id, handler_name FROM handler ORDER BY handler_name")
            return cur.fetchall()
    finally:
        db.close()


@app.get("/api/categories")
async def list_categories():
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT category_id, category_name FROM category ORDER BY category_name")
            return cur.fetchall()
    finally:
        db.close()


@app.get("/api/package-sizes")
async def list_package_sizes():
    """คืนรายการ package_size ทั้งหมด พร้อม nominal/tolerance/template_name —
    ใช้เติม datalist ของช่อง Package Size ใน index.html/edit.html (nominal/tol/
    template_name แนบมาด้วยเผื่อ frontend อยากพรีวิวค่าที่จะถูก map ให้อัตโนมัติ)
    """
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT package_size_id, package_size, nominal_x, nominal_y, "
                "upper_tol, lower_tol, template_name "
                "FROM package_size ORDER BY package_size"
            )
            return cur.fetchall()
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Parts endpoints
# ══════════════════════════════════════════════════════════════════════════════
# SELECT ที่ join parts กับทุกตาราง lookup ไว้ในที่เดียว — ใช้ร่วมกันทั้ง
# list_parts และ get_part เพื่อให้ response มีทั้งชื่อ (handler/vendor/category/
# owner/package_size) และรายละเอียดของ package_size (nominal/tolerance/
# template_name) ไม่ใช่แค่ id เปล่าๆ ที่ frontend เอาไปแสดงตรงๆ ไม่ได้
PARTS_SELECT = """
    SELECT p.part_id, p.number_alpl, p.part_number, p.description, p.po_number,
           h.handler_name   AS handler,
           v.vendor_name    AS vendor,
           c.category_name  AS category,
           o.owner_name     AS owner,
           ps.package_size  AS package_size,
           ps.nominal_x     AS nominal_x,
           ps.nominal_y     AS nominal_y,
           ps.upper_tol     AS upper_tol,
           ps.lower_tol     AS lower_tol,
           ps.template_name AS template_name
    FROM parts p
    LEFT JOIN handler h       ON p.handler_id = h.handler_id
    LEFT JOIN vendor v        ON p.vendor_id = v.vendor_id
    LEFT JOIN category c      ON p.category_id = c.category_id
    LEFT JOIN owner o         ON p.owner_id = o.owner_id
    LEFT JOIN package_size ps ON p.package_size_id = ps.package_size_id
"""


def _lookup_id(cur, table: str, id_col: str, name_col: str, value: Optional[str]) -> Optional[int]:
    """แปลงชื่อ (เช่น vendor_name ที่ frontend ส่งมาจาก dropdown) เป็น id
    (เช่น vendor_id) จากตาราง lookup ที่เกี่ยวข้อง

    ทำไม: dropdown ฝั่ง frontend (Operator/Owner/Vendor/Handler/Category/
    Package Size) เป็น dropdown "ปิด" — เลือกได้เฉพาะค่าที่มีอยู่แล้วใน DB
    เท่านั้น ไม่มีช่องพิมพ์ค่าใหม่ ดังนั้นค่าที่ส่งเข้ามาควรมีอยู่จริงเสมอ แต่ยัง
    defensive เช็คไว้กันกรณี frontend ค้างข้อมูลเก่า/ผิดพลาด — ถ้าหาไม่เจอ
    ให้ 400 ชัดเจนแทนที่จะปล่อยให้ FK constraint error กลายเป็น 500 ตอน insert
    """
    if value in (None, ""):
        return None
    cur.execute(f"SELECT {id_col} FROM {table} WHERE {name_col} = %s", (value,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(400, f"ไม่พบค่า '{value}' ใน {table} (เลือกจาก dropdown เท่านั้น)")
    return row[id_col]


def _block_if_session_running(cur, action: str) -> None:
    """เช็คว่ามี session ไหนกำลัง running อยู่ไหม — ถ้ามี ปฏิเสธการแก้ไข/ลบ Part
    หรือ Measurement ทันที (ทั้งคู่ ไม่ว่า ALPL ไหน) เพราะข้อมูลกำลังถูกวัดอยู่จริง

    ทำไมต้อง block กว้างขนาดนี้ (ไม่ใช่แค่ ALPL ที่กำลังวัดอยู่): ผู้ใช้เลือกไว้
    ชัดเจนว่าอยากให้ Edit/Delete กดไม่ได้เลยทั้ง Part และ Measurement ตราบใดที่
    ยังมี session running อยู่ — เพื่อความคาดเดาได้ง่าย ไม่ต้องตามว่า ALPL ไหน
    "ปลอดภัย" ไหม (ระบบรัน session พร้อมกันได้แค่ 1 อันเสมออยู่แล้ว — ดู
    Button Guard ใน start_session — เช็คแค่ "มี session running อยู่ไหม" จึง
    เทียบเท่ากับ "session ปัจจุบันกำลังวัดอยู่ไหม")
    """
    cur.execute("SELECT 1 FROM sessions WHERE state = 'running' LIMIT 1")
    if cur.fetchone():
        raise HTTPException(
            409,
            f"ไม่สามารถ{action}ข้อมูลได้ขณะนี้ — กำลังมีการวัดอยู่ (session running) "
            f"กรุณากด Stop ก่อนแล้วค่อยแก้ไข",
        )


class PartCreate(BaseModel):
    # schema ใหม่: parts ไม่เก็บ nominal/tolerance/template_name ตรงๆ อีกต่อไป —
    # ย้ายไปอยู่ใน package_size ทั้งหมด (ดู init.sql) เลือก package_size เดียว
    # ก็ map ค่าพวกนี้ให้อัตโนมัติ ส่วน handler/vendor/category/owner ตอนนี้เป็น
    # FK ไป lookup table — frontend ส่งมาเป็น "ชื่อ" (string จาก dropdown) แล้ว
    # backend resolve เป็น id เอง (ดู _lookup_id)
    #
    # part_number เป็น Optional เพราะตอน IPM เจอ ALPL ที่ยังไม่เคยลงทะเบียน
    # (ดู POST /api/session/start) จะลงทะเบียน part ใหม่แบบขั้นต่ำผ่าน endpoint
    # นี้ — มีแค่ number_alpl + package_size เท่านั้น ยังไม่รู้ part_number จริง
    number_alpl:   int
    part_number:   Optional[str] = None
    handler:       Optional[str] = None
    description:   Optional[str] = None
    vendor:        Optional[str] = None
    po_number:     Optional[int] = None
    category:      Optional[str] = None
    package_size:  Optional[str] = None
    owner:         Optional[str] = None


@app.get("/api/parts")
async def list_parts(
    limit:  int = Query(10, ge=1),
    offset: int = Query(0, ge=0),
    search: Optional[str] = None,
):
    """คืน config ของ parts แบบ "แบ่งหน้า" (server-side pagination)

    ทำไมเปลี่ยนจากเดิมที่คืน array ของ parts ทั้งหมดทีเดียว มาเป็น object
    {items, total}: ตาราง parts มีแนวโน้มโตขึ้นเรื่อยๆ ตามการใช้งานจริง การ
    ดึงมาทั้งหมดทุกครั้งจะกินแบนด์วิดท์และหน่วยความจำ frontend โดยเปล่าประโยชน์
    จึงให้ดึงมาทีละหน้า (limit/offset) แทน — frontend ของ edit.html แสดงทีละ 10 แถว

    `total` มาจาก COUNT(*) ที่ใช้ WHERE ชุดเดียวกับ query หลัก (ไม่ใช่นับจาก
    items ของหน้าปัจจุบัน) เพื่อให้ frontend คำนวณจำนวนหน้า/ปิดปุ่ม Next ได้ถูก

    `search` (optional) — กรองด้วย number_alpl หรือ part_number แบบ LIKE
    เหตุผลที่ทำ search ฝั่ง server ไม่ใช่ฝั่ง client: เพื่อให้ค้นหาเจอ "ข้ามทุก
    หน้า" ไม่ใช่แค่ 10 แถวของหน้าที่โหลดมาแสดงอยู่ number_alpl เป็น INT จึงต้อง
    CAST เป็น CHAR ก่อนเทียบ LIKE เพื่อให้ค้นหาบางส่วนของตัวเลขได้ (เช่นพิมพ์
    "10" แล้วเจอทั้ง 1011, 1002 ที่ขึ้นต้นด้วย 10)

    หมายเหตุ: เพิ่ม ORDER BY number_alpl เพื่อให้ลำดับของหน้าคงที่ (stable) —
    ถ้าไม่กำหนด ORDER BY การไล่ LIMIT/OFFSET อาจได้ลำดับไม่แน่นอนข้ามหน้า
    """
    conditions, params = [], []
    if search:
        conditions.append("(CAST(p.number_alpl AS CHAR) LIKE %s OR p.part_number LIKE %s)")
        like = f"%{search}%"
        params.extend([like, like])
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM parts p {where}", params)
            total = cur.fetchone()["total"]
            cur.execute(
                f"{PARTS_SELECT} {where} ORDER BY p.number_alpl LIMIT %s OFFSET %s",
                (*params, limit, offset),
            )
            items = cur.fetchall()
        return {"items": items, "total": total}
    finally:
        db.close()


@app.get("/api/parts/{part_id}")
async def get_part(part_id: int):
    """คืน config ของ part 1 ตัวตาม ALPL number (รวมชื่อ handler/vendor/category/
    owner/package_size ที่ join มาจาก lookup table แล้ว ไม่ใช่แค่ id)"""
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(f"{PARTS_SELECT} WHERE p.number_alpl = %s", (part_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Part not found")
        return row
    finally:
        db.close()


def _insert_part_row(cur, number_alpl: int, config: Dict[str, Any]) -> None:
    """Insert 1 row ลง table `parts` โดยใช้ number_alpl ที่ระบุ + field อื่นจาก
    `config` (dict ของ field part_number/handler/vendor/category/package_size/
    owner ฯลฯ) — handler/vendor/category/package_size/owner รับมาเป็น "ชื่อ"
    (ตรงกับค่าที่เลือกจาก dropdown ฝั่ง frontend) แล้ว resolve เป็น id ก่อน insert

    ใช้ร่วมกันทั้งจาก endpoint POST /api/parts ปกติ, จาก flow ของ New queue ที่
    ALPL หลายตัวใช้ config เดียวกันซ้ำ, และจากการลงทะเบียน part แบบขั้นต่ำตอน
    IPM เจอ ALPL ที่ยังไม่เคยลงทะเบียน (ดู start_session / create_measurement)
    """
    handler_id      = _lookup_id(cur, "handler",      "handler_id",      "handler_name",  config.get("handler"))
    vendor_id       = _lookup_id(cur, "vendor",        "vendor_id",       "vendor_name",   config.get("vendor"))
    category_id     = _lookup_id(cur, "category",      "category_id",     "category_name", config.get("category"))
    owner_id        = _lookup_id(cur, "owner",         "owner_id",        "owner_name",    config.get("owner"))
    package_size_id = _lookup_id(cur, "package_size",  "package_size_id", "package_size",  config.get("package_size"))
    cur.execute(
        "INSERT INTO parts "
        "(number_alpl, part_number, handler_id, description, vendor_id, "
        " po_number, category_id, package_size_id, owner_id) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            number_alpl,
            config.get("part_number"), handler_id, config.get("description"),
            vendor_id, config.get("po_number"), category_id, package_size_id, owner_id,
        ),
    )


@app.post("/api/parts", status_code=201)
async def create_part(part: PartCreate):
    """ลงทะเบียน ALPL part ใหม่: nominal X/Y, tolerance แยกแกน, และ template ของ TM-X ที่ใช้

    ทำไมต้องแยก endpoint นี้ออกจาก start_session: parts/templates ถูก config
    ไว้ล่วงหน้า (เป็นขั้นตอน setup) เพื่อให้ start_session แค่ lookup template
    จาก number_alpl ได้เลย ไม่ต้องให้ผู้ปฏิบัติงานพิมพ์เองทุกครั้งที่รัน
    """
    db = get_db()
    try:
        with db.cursor() as cur:
            _insert_part_row(cur, part.number_alpl, part.dict())
        return {"number_alpl": part.number_alpl}
    finally:
        db.close()


@app.patch("/api/parts/{part_id}")
async def update_part(part_id: int, data: Dict[str, Any] = Body(...)):
    """อัปเดต config ของ part แบบ partial (เฉพาะ field ที่ส่งมาใน body)

    ทำไมต้องมี whitelist (`allowed`): เพื่อไม่ให้ request body ไปเขียนทับ column
    ที่ไม่ควรแก้ผ่าน endpoint นี้ได้โดยไม่ตั้งใจ (หรือถูกใช้ในทางที่ไม่ดี)

    `number_alpl` แก้ไขได้ (จาก edit.html — Edit Part) แม้จะเป็น "business key"
    หลักที่ sessions/measurements อ้างอิงถึงก็ตาม — ถ้า ALPL ตัวนี้มีประวัติ
    session/measurement ผูกอยู่แล้ว MySQL จะปฏิเสธด้วย FK constraint error
    (เพราะ FOREIGN KEY ไม่มี ON UPDATE CASCADE) เราจับ error นั้นแล้วแปลงเป็น
    409 ที่อ่านง่ายแทนที่จะปล่อยให้เป็น 500 ดิบๆ
    """
    # field ที่แก้ตรงๆ ได้เลย ไม่ต้อง resolve ผ่าน lookup table
    direct_fields = {"number_alpl", "part_number", "description", "po_number"}
    # field ที่เป็น "ชื่อ" จาก dropdown — ต้อง resolve เป็น id ก่อน (key ที่รับจาก
    # request → (คอลัมน์จริงใน parts, ตาราง lookup, id column, name column))
    lookup_fields = {
        "handler":      ("handler_id",      "handler",      "handler_id",      "handler_name"),
        "vendor":       ("vendor_id",       "vendor",       "vendor_id",       "vendor_name"),
        "category":     ("category_id",     "category",     "category_id",     "category_name"),
        "owner":        ("owner_id",        "owner",        "owner_id",        "owner_name"),
        "package_size": ("package_size_id", "package_size", "package_size_id", "package_size"),
    }
    db = get_db()
    try:
        with db.cursor() as cur:
            _block_if_session_running(cur, "แก้ไข")
            set_parts, values = [], []
            for k, v in data.items():
                if k in direct_fields:
                    set_parts.append(f"{k} = %s")
                    values.append(v)
                elif k in lookup_fields:
                    col, table, id_col, name_col = lookup_fields[k]
                    set_parts.append(f"{col} = %s")
                    values.append(_lookup_id(cur, table, id_col, name_col, v))
            if not set_parts:
                raise HTTPException(400, "No valid fields provided")
            set_clause = ", ".join(set_parts)
            try:
                cur.execute(
                    f"UPDATE parts SET {set_clause} WHERE number_alpl = %s",
                    (*values, part_id),
                )
            except pymysql.MySQLError as exc:
                raise HTTPException(
                    409,
                    f"บันทึกไม่สำเร็จ — ALPL ใหม่อาจซ้ำกับ part อื่น หรือ ALPL เดิมมี "
                    f"session/measurement ผูกอยู่แล้ว (เปลี่ยน ALPL ที่มีประวัติไม่ได้): {exc}",
                )
            if cur.rowcount == 0:
                raise HTTPException(404, "Part not found")
        return {"ok": True}
    finally:
        db.close()


@app.delete("/api/parts/{part_id}")
async def delete_part(part_id: int, cascade: bool = False):
    """ลบ Part 1 row ออกจากตาราง `parts`

    FK ของ sessions.number_alpl/measurements.number_alpl ที่ชี้มาที่
    parts.number_alpl เป็นแบบ RESTRICT (ไม่มี ON DELETE CASCADE ที่ระดับ DB —
    ดู init.sql) ปกติแล้วถ้า ALPL นี้ยังมีประวัติผูกอยู่ MySQL จะปฏิเสธ DELETE เอง

    cascade=False (ค่า default): ลบตรงๆ เจอ FK block ก็ปฏิเสธเป็น 409

    cascade=True: ใช้เมื่อผู้ใช้กด "ยืนยัน" ใน popup ของ edit.html ที่เตือนไว้ชัดเจน
    ว่าลบ Part นี้จะลบ Measurement ทั้งหมดของ ALPL นี้ไปด้วย — เป็นการ cascade
    แบบต้องยืนยันจากผู้ใช้ตรงๆ ทุกครั้ง (ไม่ใช่ ON DELETE CASCADE เงียบๆ ที่ระดับ DB)

    จุดที่ต้องระวัง: 1 session (แถวใน `sessions`) แทน "1 รอบการวัด" ซึ่งในโหมด
    New อาจมีคิวหลาย ALPL ได้ แต่คอลัมน์ sessions.number_alpl เก็บได้แค่ ALPL
    เดียว (ตัวแรกที่กรอกตอนเริ่ม) — ถ้า ALPL ที่กำลังจะลบดันเป็น "ตัวแทน" ของ
    session ที่ยังมี measurement ของ ALPL อื่นในคิวเดียวกันเหลืออยู่ การ
    DELETE FROM sessions ตรงๆ จะโดน FK block (measurements.session_id ของ ALPL
    อื่นยังชี้มาที่ session นี้อยู่) เราจึงไม่ลบ session ทิ้งดื้อๆ แต่ตรวจก่อนว่า
    session นั้นยังมี measurement ของ ALPL อื่นเหลืออยู่ไหม ถ้ามี → "ย้าย" session
    ไปอ้างอิง ALPL อื่นที่ยังมีข้อมูลแทน (แค่เอา ALPL นี้ออกจากการเป็นตัวแทน ไม่ได้
    ลบประวัติของ ALPL อื่นทิ้งไปด้วย) ถ้าไม่เหลือแล้วจริงๆ ค่อยลบ session ทิ้ง
    ทำทั้งหมดเป็น transaction เดียว (ปิด autocommit ชั่วคราว) กัน DB ค้างอยู่
    ครึ่งๆ กลางๆ ถ้ามีขั้นไหนพังกลางทาง

    ก่อนจะแตะ DB เลย เช็คก่อนว่า ALPL นี้ "ยังไม่ถึงคิว" ของ session ที่กำลัง
    running อยู่ตอนนี้ไหม (เทียบกับ session_queues ใน memory ไม่ต้อง query DB)
    ถ้าใช่ ปฏิเสธการลบไปเลย เพราะถ้าปล่อยให้ลบไปตอนนี้ พอ Agent วัดมาถึง ALPL
    นี้จริงๆ ในอนาคต จะหา Part ไม่เจอ — โหมด IPM จะพังเป็น 404 ตรงๆ (ไม่มี
    lazy-insert กันไว้เหมือนโหมด New) ส่วนโหมด New แม้จะ self-heal ได้ด้วย
    lazy-insert (ดู create_measurement) แต่ก็ทำให้ผู้ใช้งง เพราะกดลบไปแล้วแต่
    ข้อมูลกลับมาอีกทีแบบไม่มีการแจ้งเตือน จึงบล็อกไว้ทั้ง 2 โหมดให้พฤติกรรม
    คาดเดาได้ง่ายเหมือนกัน — ALPL ที่วัดไปแล้ว (ผ่านคิวไปแล้ว) ไม่โดนบล็อก
    ยังลบได้ตามปกติแม้ session จะยัง running อยู่ก็ตาม (ดู delete_part เดิม
    ที่ทดสอบแล้วว่า retarget session ไปอ้างอิง ALPL อื่นที่วัดแล้วได้อย่างปลอดภัย
    ไม่กระทบ session ที่กำลัง running อยู่)
    """
    db = get_db()
    try:
        db.autocommit(False)
        with db.cursor() as cur:
            _block_if_session_running(cur, "ลบ")
            if cascade:
                cur.execute("DELETE FROM measurements WHERE number_alpl = %s", (part_id,))
                cur.execute("SELECT session_id FROM sessions WHERE number_alpl = %s", (part_id,))
                orphaned_session_ids = [row["session_id"] for row in cur.fetchall()]
                for sid in orphaned_session_ids:
                    cur.execute(
                        "SELECT number_alpl FROM measurements WHERE session_id = %s LIMIT 1",
                        (sid,),
                    )
                    remaining = cur.fetchone()
                    if remaining:
                        # session นี้ยังมี measurement ของ ALPL อื่นเหลืออยู่ — ย้าย
                        # ให้ session อ้างอิง ALPL นั้นแทน แทนที่จะลบ session ทิ้ง
                        cur.execute(
                            "UPDATE sessions SET number_alpl = %s WHERE session_id = %s",
                            (remaining["number_alpl"], sid),
                        )
                    else:
                        # ไม่มี measurement เหลือแล้วจริงๆ (ทั้ง session ไม่เคยมีผลวัด
                        # ของ ALPL ไหนเลย หรือ ALPL ที่กำลังลบเป็นตัวเดียวที่เคยวัด)
                        cur.execute("DELETE FROM sessions WHERE session_id = %s", (sid,))
            try:
                cur.execute("DELETE FROM parts WHERE number_alpl = %s", (part_id,))
            except pymysql.MySQLError as exc:
                db.rollback()
                raise HTTPException(
                    409,
                    f"ลบไม่ได้ — ALPL {part_id} ยังมี Session/Measurement ผูกอยู่ "
                    f"กรุณาลบ Measurement ที่เกี่ยวข้องกับ ALPL นี้ทั้งหมดก่อน แล้วค่อยลบ Part: {exc}",
                )
            if cur.rowcount == 0:
                db.rollback()
                raise HTTPException(404, "Part not found")
        db.commit()
        return {"ok": True}
    finally:
        db.autocommit(True)
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Measurements endpoints
# ══════════════════════════════════════════════════════════════════════════════
# SELECT ที่ join measurements กับ operator ไว้ในที่เดียว — ใช้ร่วมกันทั้ง
# list_measurements และ export_csv เพื่อให้ response/CSV มีชื่อ operator (ไม่ใช่
# แค่ operator_id เปล่าๆ) หลังจากย้าย Operator จากคอลัมน์ VARCHAR ตรงๆ ไปเป็น
# FK ชี้ตาราง operator
MEASUREMENTS_SELECT = """
    SELECT m.*, op.operator_name AS operator_name
    FROM measurements m
    LEFT JOIN operator op ON m.operator_id = op.operator_id
"""


class MeasurementCreate(BaseModel):
    # session_id เป็น Optional แล้ว — None หมายถึง "manual add" จากหน้า
    # Database Editor (edit.html ปุ่ม + Add Measurement) ซึ่งไม่มี session ที่
    # Agent กำลัง running อยู่จริงให้อ้างอิงเลย ต่างจาก flow ปกติที่ Agent ส่ง
    # session_id ที่ได้จากตอนเริ่ม session มาด้วยเสมอ (ดู create_measurement)
    session_id:  Optional[int] = None
    number_alpl: int
    value_x:     float
    value_y:     float
    note:        Optional[str] = None
    # UUID ที่ Agent สร้างขึ้นต่อการวัด 1 ครั้ง (uuid4) — ส่งมาด้วยทุกครั้งที่มา
    # จาก agent.py (ไม่มีถ้าเป็น manual add จาก edit.html) ใช้กัน insert ซ้ำ
    # ตอน Agent retry POST นี้ (ดู create_measurement)
    client_uuid: Optional[str] = None


class ImageUpdate(BaseModel):
    image_path: str


@app.get("/api/measurements")
async def list_measurements(
    number_alpl: Optional[int] = None,
    result:      Optional[str] = None,
    date_from:   Optional[str] = None,
    date_to:     Optional[str] = None,
    session_id:  Optional[int] = None,
    limit:  int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
):
    """Query ประวัติ measurement พร้อม filter ที่เลือกได้ — ใช้ทั้งโดยตาราง dashboard
    (renderTable) และเป็นฐานของ /api/export/csv filter ทุกตัวเป็น optional
    และรวมกันด้วย AND

    เปลี่ยน response shape เป็น object {items, total} เหมือน /api/parts เพื่อรองรับ
    server-side pagination (เพิ่ม offset เข้ามาคู่กับ limit ที่มีอยู่เดิม) — measurements
    โตเร็วกว่า parts มาก จึงไม่ควรโหลดทั้งหมดมา slice ฝั่ง client

    `total` ใช้ COUNT(*) บน WHERE ชุดเดียวกับ query หลัก (filter เดียวกัน) เพื่อให้
    frontend รู้จำนวนทั้งหมดที่ตรงกับ filter ปัจจุบัน ไว้คำนวณหน้า/ปิดปุ่ม Next

    หมายเหตุ: /api/export/csv เป็น endpoint แยกที่ "ไม่" reuse ฟังก์ชันนี้ (มันสร้าง
    WHERE ของตัวเองและคืนไฟล์ CSV ไม่ใช่ JSON) — การเปลี่ยน shape ตรงนี้จึงไม่กระทบ export
    """
    conditions, params = [], []
    if number_alpl is not None:
        conditions.append("m.number_alpl = %s"); params.append(number_alpl)
    if result:
        conditions.append("m.result = %s"); params.append(result)
    if date_from:
        conditions.append("m.timestamp >= %s"); params.append(date_from)
    if date_to:
        conditions.append("m.timestamp <= %s"); params.append(date_to)
    if session_id is not None:
        conditions.append("m.session_id = %s"); params.append(session_id)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM measurements m {where}", params)
            total = cur.fetchone()["total"]
            cur.execute(
                f"{MEASUREMENTS_SELECT} {where} ORDER BY m.timestamp DESC LIMIT %s OFFSET %s",
                (*params, limit, offset),
            )
            items = cur.fetchall()
        return {"items": items, "total": total}
    finally:
        db.close()


@app.post("/api/measurements")
async def create_measurement(req: MeasurementCreate):
    """บันทึก measurement หนึ่งรายการที่ส่งมาจาก Agent และตัดสิน OK/NG

    Endpoint นี้ถูกเรียกครั้งละ 1 ชิ้นงานที่ TM-X วัดได้ ส่งต่อมาโดย Agent
    พร้อม session_id ที่ได้รับตอนเริ่ม session — Agent ส่ง `number_alpl` มาด้วย
    เหมือนเดิมเสมอ (ไม่ต้องแก้ agent.py) แต่ตอนนี้ backend จะตัดสินเองว่าจะใช้
    ALPL ไหนจริงๆ ตามประเภทของ session:

      - **Session แบบ queue-based (IPM/New — มี entry ใน session_queues)**:
        เพิกเฉยค่า `req.number_alpl` ที่ Agent ส่งมา แล้วใช้ ALPL ตามตำแหน่ง
        ปัจจุบันในคิวแทน (`session_queues[session_id]["queue"][position]`)
        เพราะ Agent ไม่รู้ (และไม่จำเป็นต้องรู้) ว่ากำลังวัดตัวไหนอยู่ในคิว
        มันรู้แค่ว่า "วัดเสร็จแล้ว ได้ value_x/value_y เท่านี้"
      - **Session แบบ manual (เดิม — ไม่มี entry ใน session_queues)**: ใช้
        `req.number_alpl` ตรงๆ ตามที่ Agent ส่งมา ไม่มีอะไรเปลี่ยนจากเดิม
    """
    is_manual = req.session_id is None
    qstate = None
    db = get_db()
    try:
        # กันการ insert ซ้ำถ้า Agent retry POST นี้ด้วย client_uuid เดิม (เช่น
        # ตอบกลับจาก request ครั้งก่อนหลุดหายระหว่างทาง ทั้งที่จริง backend
        # insert สำเร็จไปแล้ว) — เช็คก่อนทำอะไรอื่นเลย ถ้าเคยเห็น UUID นี้แล้ว
        # คืนผลเดิมไปตรงๆ ไม่ insert แถวใหม่ ไม่นับ measured_count ซ้ำ
        if req.client_uuid:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT measurement_id, session_id, result FROM measurements "
                    "WHERE client_uuid = %s",
                    (req.client_uuid,),
                )
                dup = cur.fetchone()
            if dup:
                with db.cursor() as cur:
                    cur.execute(
                        "SELECT measured_count, target_count FROM sessions WHERE session_id = %s",
                        (dup["session_id"],),
                    )
                    s = cur.fetchone() or {}
                log.info(
                    "Duplicate measurement POST (client_uuid=%s) — คืนผลเดิม measurement_id=%d",
                    req.client_uuid, dup["measurement_id"],
                )
                return {
                    "measurement_id": dup["measurement_id"],
                    "result":  dup["result"],
                    "status":  "duplicate_ignored",
                    "measured": s.get("measured_count"),
                    "target":   s.get("target_count"),
                }

        with db.cursor() as cur:
            if is_manual:
                # ── Manual add จากหน้า Database Editor (edit.html) ──────────
                # ไม่มี session ของ Agent ที่ running อยู่จริงให้อ้างอิงเลย แต่
                # measurements.session_id เป็น NOT NULL + FK ไป sessions บังคับ
                # ต้องมี session อยู่จริงเสมอ จึงสร้าง session "จบในตัว" ขึ้นมา 1
                # แถวแทน (state='stopped', target=measured=1, ended_at=NOW())
                # ไม่ใช่ session ของ Agent เลย แค่เป็นที่ผูก FK ให้ record นี้เท่านั้น
                cur.execute(
                    "INSERT INTO sessions "
                    "(number_alpl, state, target_count, measured_count, ended_at) "
                    "VALUES (%s, 'stopped', 1, 1, NOW())",
                    (req.number_alpl,),
                )
                session_id = cur.lastrowid
                number_alpl = req.number_alpl
                measure_type = "Manual"
                operator_name = None
                note = req.note
            else:
                session_id = req.session_id
                # Session ต้องอยู่ในสถานะ running
                cur.execute(
                    "SELECT state, target_count, measured_count FROM sessions WHERE session_id = %s",
                    (session_id,),
                )
                session = cur.fetchone()
                if not session or session["state"] != "running":
                    raise HTTPException(400, "Session is not running")

                qstate = session_queues.get(session_id)  # None ถ้าเป็น manual session (เดิม)
                measure_type = None
                operator_name = None
                note = None

                if qstate is not None:
                    # ── Queue-based (IPM / New) ─────────────────────────────
                    queue = qstate["queue"]
                    pos = qstate["position"]
                    if pos >= len(queue):
                        raise HTTPException(400, "Measurement queue หมดแล้วสำหรับ session นี้")
                    number_alpl = queue[pos]
                    measure_type = qstate["entry_mode"]  # 'IPM' หรือ 'New'
                    operator_name = qstate.get("operator")
                    note = qstate.get("note")
                else:
                    # ── Manual session แบบเดิม (ผ่าน Agent แต่ไม่มีคิว) ─────────
                    number_alpl = req.number_alpl

                # ── New mode: insert Part ตัวนี้แบบ lazy ถ้ายังไม่เคยมีอยู่จริง ──
                # ตัวแรกในคิวถูก insert ไปแล้วตอน start_session (จำเป็นเพราะ FK
                # ของ sessions) ตัวที่เหลือยังไม่เคย insert เลย — insert "ตอนนี้"
                # ที่ได้ผลวัดจริงจาก Agent แล้วเท่านั้น เพื่อไม่ให้ ALPL ที่ยังไม่
                # ทันวัด (เช่น กด Stop กลางคัน) กลายเป็น Part ค้างอยู่ใน DB ทั้งที่
                # ไม่มีประวัติจริง
                if qstate is not None and qstate.get("new_part_config") is not None:
                    cur.execute("SELECT 1 FROM parts WHERE number_alpl = %s", (number_alpl,))
                    if not cur.fetchone():
                        try:
                            _insert_part_row(cur, number_alpl, qstate["new_part_config"])
                        except pymysql.MySQLError as exc:
                            raise HTTPException(409, f"Insert Part ALPL {number_alpl} ไม่สำเร็จ: {exc}")

            # หา nominal/tolerance ผ่าน package_size ที่ผูกกับ part นี้ — schema
            # ใหม่เก็บ tolerance ตัวเดียวใช้ร่วมกันทั้งแกน X/Y (upper_tol/lower_tol)
            # แทนคอลัมน์แยกแกนแบบเดิม (ดู init.sql — package_size table)
            cur.execute(
                "SELECT ps.nominal_x, ps.nominal_y, ps.upper_tol, ps.lower_tol "
                "FROM parts p JOIN package_size ps ON p.package_size_id = ps.package_size_id "
                "WHERE p.number_alpl = %s",
                (number_alpl,),
            )
            part = cur.fetchone()
            if not part:
                raise HTTPException(404, "Part not found (หรือยังไม่ได้ตั้ง package_size ให้ part นี้)")

            # เช็ค OK/NG — tolerance ตัวเดียวใช้ร่วมกันทั้งแกน X และ Y
            ok_x = (part["nominal_x"] - part["lower_tol"]) <= req.value_x <= (part["nominal_x"] + part["upper_tol"])
            ok_y = (part["nominal_y"] - part["lower_tol"]) <= req.value_y <= (part["nominal_y"] + part["upper_tol"])
            result = "OK" if (ok_x and ok_y) else "NG"

            # resolve ชื่อ operator (จาก dropdown) เป็น operator_id ก่อน insert —
            # measurements.operator_id เป็น FK ไป operator table แล้ว (เดิมเป็น
            # คอลัมน์ VARCHAR ชื่อ "Oparetor" ที่เก็บชื่อ operator ตรงๆ)
            operator_id = _lookup_id(cur, "operator", "operator_id", "operator_name", operator_name)

            # Insert row ของ measurement (รวม measure_type/operator_id/note ถ้าเป็น
            # queue-based หรือ manual add — สำหรับ manual session (ผ่าน Agent) แบบเดิม
            # ทั้ง 3 ค่านี้จะเป็น NULL)
            try:
                cur.execute(
                    "INSERT INTO measurements "
                    "(session_id, number_alpl, value_x, value_y, result, measure_type, operator_id, note, client_uuid) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (session_id, number_alpl, req.value_x, req.value_y, result, measure_type, operator_id, note, req.client_uuid),
                )
            except pymysql.IntegrityError:
                # race เล็กๆ ที่ทฤษฎีมีได้: สอง request ที่มี client_uuid เดียวกัน
                # มาถึงพร้อมกันเป๊ะๆ ผ่านเช็ค dedup ด้านบนพร้อมกันทั้งคู่ (เช็คแล้ว
                # ยังไม่เจอ เพราะอีกฝั่งยัง insert ไม่เสร็จ) — unique index บน
                # client_uuid จะกันไม่ให้ insert ซ้ำจริงๆ ระดับ DB อยู่ดี แค่ต้อง
                # จับ error แล้วบอกให้รู้ว่าเป็นการซ้ำ ไม่ใช่ปล่อยเป็น 500 ดิบๆ
                raise HTTPException(409, "Measurement นี้ถูกบันทึกไปแล้ว (duplicate client_uuid)")
            measurement_id = cur.lastrowid

            if not is_manual:
                # เพิ่มตัวนับของ session — เฉพาะ session จริงของ Agent เท่านั้น
                # (manual session ที่สร้างเองข้างบน insert มาแบบ measured=target=1
                # อยู่แล้ว ไม่ต้องนับซ้ำ)
                cur.execute(
                    "UPDATE sessions SET measured_count = measured_count + 1 "
                    "WHERE session_id = %s",
                    (session_id,),
                )

                # อ่านค่าตัวนับล่าสุดอีกครั้ง เพื่อเช็คว่าครบ target แล้วหรือยัง
                cur.execute(
                    "SELECT measured_count, target_count FROM sessions WHERE session_id = %s",
                    (session_id,),
                )
                updated = cur.fetchone()
                measured = updated["measured_count"]
                target   = updated["target_count"]
            else:
                measured, target = 1, 1

        if not is_manual:
            # เพิ่มตำแหน่งในคิว (memory) แล้ว sync สำเนาลง DB ทันที (คอลัมน์
            # sessions.queue_state) — กัน backend restart กลาง session นี้แล้ว
            # ตำแหน่งคิวหาย ทำให้ measurement หลังจากนั้นถูกบันทึกผิด ALPL ไปเรื่อยๆ
            # แบบเงียบๆ (ดู lifespan() ที่โหลดค่านี้กลับตอน boot)
            if qstate is not None:
                qstate["position"] += 1
                with db.cursor() as cur:
                    cur.execute(
                        "UPDATE sessions SET queue_state = %s WHERE session_id = %s",
                        (json.dumps(qstate), session_id),
                    )

        # Auto-complete session เมื่อถึง target_count แล้ว — เฉพาะ session จริง
        # ของ Agent เท่านั้น (manual session จบในตัวเองไปแล้วตั้งแต่ insert)
        status = "complete" if is_manual else "continue"
        if not is_manual and measured >= target:
            with db.cursor() as cur:
                cur.execute(
                    "UPDATE sessions SET state = 'stopped', ended_at = NOW() "
                    "WHERE session_id = %s",
                    (session_id,),
                )
            status = "complete"
            session_queues.pop(session_id, None)  # session จบแล้ว ลบคิวออกจาก memory
            await push_event(
                "session_complete",
                {"session_id": session_id, "measured": measured, "target": target},
            )

        # ไม่ broadcast SSE เลยตอน manual add — เหตุผล: onNewMeasurement /
        # onSessionComplete ฝั่ง dashboard (index.html) ไม่ได้เช็คว่า session_id
        # ที่ได้รับตรงกับ session ที่กำลังแสดงอยู่ไหม เลยจะเขียนทับ measured_count/
        # telemetry ของ session จริงที่อาจกำลัง running อยู่พร้อมกันโดยไม่ตั้งใจ
        # (ดูรายละเอียดเพิ่มเติมในคำอธิบายที่คุยกันไว้) edit.html เองก็ไม่ได้พึ่ง
        # SSE อยู่แล้ว มัน refetch ตารางเองหลัง POST สำเร็จ
        if not is_manual:
            await push_event(
                "measurement",
                {
                    "measurement_id": measurement_id,
                    "session_id":     session_id,
                    "number_alpl":    number_alpl,
                    "value_x":        req.value_x,
                    "value_y":        req.value_y,
                    "result":         result,
                    "measured":       measured,
                    "target":         target,
                },
            )
        return {
            "measurement_id": measurement_id,
            "result":  result,
            "status":  status,
            "measured": measured,
            "target":  target,
        }
    finally:
        db.close()


@app.patch("/api/measurements/{measurement_id}")
async def update_measurement(measurement_id: int, data: Dict[str, Any] = Body(...)):
    """แก้ไข measurement แบบ partial (เฉพาะ field ที่ส่งมาใน body) — ใช้โดยหน้า
    Database Editor (edit.html) ตอนกด Edit แล้ว Save

    ทำไมต้องมี endpoint นี้แยกจาก create_measurement: create_measurement เป็น flow
    ของ Agent (insert ค่าใหม่ที่ TM-X วัดได้ พร้อมตัดสิน OK/NG จาก tolerance) ส่วนการ
    "แก้" ค่าที่บันทึกไว้แล้วเป็นการ override ด้วยมือจากหน้า editor ซึ่งไม่มีมาก่อน

    ใช้ pattern เดียวกับ update_part(): whitelist เฉพาะ field ที่อนุญาตให้แก้ได้
    (value_x, value_y, number_alpl, note) เพื่อกัน body ไปเขียนทับ column ที่ไม่ควร
    แก้ผ่าน endpoint นี้ (เช่น session_id, timestamp, image_path)

    `number_alpl` แก้ไขได้ (เพิ่มเข้ามาใหม่) — ใช้กรณี IPM พิมพ์/เลือก ALPL ผิดตัว
    (เลือกชิ้นที่มีอยู่จริงในระบบผิดตัว) วิธีแก้ที่ถูกคือ retarget measurement row
    นี้ไปที่ ALPL ที่ถูกต้อง ไม่ใช่ไปแก้ ALPL ที่ตัว Part (เพราะจะกลายเป็นเปลี่ยนชื่อ
    Part จริงที่มีประวัติของตัวเองอยู่แล้ว — ดู update_part สำหรับกรณีพิมพ์ ALPL
    ผิดตอน "New" ซึ่งเหมาะจะแก้ที่ตัว Part แทน)

    ไม่มี `result` ใน allowed อีกต่อไป (เดิมให้ผู้ใช้เลือก OK/NG เองตรงๆ) — เพราะ
    ตอนนี้ ALPL/value เปลี่ยนได้ ทำให้ tolerance ที่ใช้ตัดสิน OK/NG เปลี่ยนตามไปด้วย
    จึงคำนวณ result ใหม่เสมอหลัง update ทุกครั้ง (ไม่ว่าจะแก้ field ไหนก็ตาม) แทนที่
    จะรับค่าจาก frontend ตรงๆ กัน Result ค้างไม่ตรงกับ ALPL/ค่าที่วัดได้จริง
    """
    allowed = {"value_x", "value_y", "number_alpl", "note"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields:
        raise HTTPException(400, "No valid fields provided")
    set_clause = ", ".join(f"{k} = %s" for k in fields)
    db = get_db()
    try:
        with db.cursor() as cur:
            _block_if_session_running(cur, "แก้ไข")
            try:
                cur.execute(
                    f"UPDATE measurements SET {set_clause} WHERE measurement_id = %s",
                    (*fields.values(), measurement_id),
                )
            except pymysql.MySQLError as exc:
                raise HTTPException(
                    409,
                    f"บันทึกไม่สำเร็จ — ALPL ใหม่อาจยังไม่ได้ลงทะเบียนใน Parts: {exc}",
                )
            if cur.rowcount == 0:
                raise HTTPException(404, "Measurement not found")

            # คำนวณ OK/NG ใหม่จากค่า value_x/value_y/number_alpl "ปัจจุบัน" ของ row
            # นี้เสมอ (หลัง update) — ครอบคลุมทั้งกรณีแก้ ALPL, แก้ value, หรือแก้แค่ note
            cur.execute(
                "SELECT m.value_x, m.value_y, ps.nominal_x, ps.nominal_y, ps.upper_tol, ps.lower_tol "
                "FROM measurements m "
                "JOIN parts p ON m.number_alpl = p.number_alpl "
                "JOIN package_size ps ON p.package_size_id = ps.package_size_id "
                "WHERE m.measurement_id = %s",
                (measurement_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    404,
                    "ไม่พบ package_size ของ ALPL นี้ — คำนวณ OK/NG ใหม่ไม่ได้ (ตรวจสอบว่า Part ตั้ง Package Size ไว้แล้ว)",
                )
            ok_x = (row["nominal_x"] - row["lower_tol"]) <= row["value_x"] <= (row["nominal_x"] + row["upper_tol"])
            ok_y = (row["nominal_y"] - row["lower_tol"]) <= row["value_y"] <= (row["nominal_y"] + row["upper_tol"])
            new_result = "OK" if (ok_x and ok_y) else "NG"
            cur.execute(
                "UPDATE measurements SET result = %s WHERE measurement_id = %s",
                (new_result, measurement_id),
            )
        return {"ok": True, "result": new_result}
    finally:
        db.close()


@app.patch("/api/measurements/{measurement_id}/image")
async def update_image(measurement_id: int, req: ImageUpdate):
    """แนบ path ของรูปภาพเข้ากับ measurement หลังจากอัปโหลดขึ้น MinIO เรียบร้อยแล้ว

    ทำไมต้องแยก call นี้ออกจาก create_measurement: Agent อัปโหลดรูป inspection
    ขึ้น MinIO โดยตรงผ่าน presigned URL (ดู /api/upload-url) *หลังจาก* ค่า
    measurement ถูกบันทึกไปแล้ว endpoint นี้แค่ patch object key ที่ได้กลับมา
    หลังอัปโหลดเสร็จ การ broadcast 'image_updated' ทำให้ dashboard เปลี่ยน
    ปุ่มรูปภาพในแถวนั้นได้โดยไม่ต้อง refresh ตารางทั้งหมด
    """
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(
                "UPDATE measurements SET image_path = %s WHERE measurement_id = %s",
                (req.image_path, measurement_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Measurement not found")
        await push_event(
            "image_updated",
            {"measurement_id": measurement_id, "image_path": req.image_path},
        )
        return {"ok": True}
    finally:
        db.close()


@app.delete("/api/measurements/{measurement_id}")
async def delete_measurement(measurement_id: int):
    """ลบ measurement 1 row (เช่น ลบค่าที่อ่านผิดพลาด/เป็นการทดสอบ)"""
    db = get_db()
    try:
        with db.cursor() as cur:
            _block_if_session_running(cur, "ลบ")
            cur.execute(
                "DELETE FROM measurements WHERE measurement_id = %s", (measurement_id,)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Measurement not found")
        return {"ok": True}
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Upload / Image URL endpoints
# ══════════════════════════════════════════════════════════════════════════════
class UploadUrlRequest(BaseModel):
    filename:       str
    measurement_id: int


@app.post("/api/upload-url")
async def get_upload_url(req: UploadUrlRequest):
    """สร้าง presigned URL ของ MinIO ให้ Agent อัปโหลดรูปขึ้นไปได้โดยตรง

    ทำไมต้องใช้ presigned URL: Agent อัปโหลดรูปขึ้น MinIO ตรงๆ โดยไม่ต้องผ่าน
    backend ตัวนี้ ทำให้ backend ไม่ต้องเสียเวลารับไฟล์ขนาดใหญ่ และโฟกัสกับงาน
    DB/session ได้ URL หมดอายุใน 1 ชั่วโมง ซึ่งเพียงพอให้ Agent อัปโหลดเสร็จ
    หลังจากถ่ายภาพ
    """
    object_key = f"measurements/{req.measurement_id}/{req.filename}"
    mc = get_minio()
    url = mc.presigned_put_object(MINIO_BUCKET, object_key, expires=timedelta(hours=1))
    return {"presigned_url": url, "object_key": object_key}


@app.get("/api/image-url/{measurement_id}")
async def get_image_url(measurement_id: int):
    """สร้าง presigned URL ของ MinIO ให้ dashboard ใช้สำหรับ *ดู* รูปภาพ

    ทำไมต้องสร้าง URL ใหม่ทุกครั้งแทนเก็บ URL ถาวร: presigned URL จะหมดอายุ
    (ที่นี่คือ 1 ชั่วโมง) เพื่อความปลอดภัย ดังนั้นทุกครั้งที่ผู้ปฏิบัติงานกดไอคอนรูป
    (showImage ในฝั่ง frontend) เราจะออก URL ใหม่ที่อายุสั้นแทนการพึ่ง URL
    ที่สร้างไว้ก่อนหน้านี้
    """
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT image_path FROM measurements WHERE measurement_id = %s",
                (measurement_id,),
            )
            row = cur.fetchone()
        if not row or not row["image_path"]:
            raise HTTPException(404, "Image not found")
        mc = get_minio()
        url = mc.presigned_get_object(
            MINIO_BUCKET, row["image_path"], expires=timedelta(hours=1)
        )
        return {"url": url}
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# CSV Export
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/export/csv")
async def export_csv(
    number_alpl: Optional[int] = None,
    result:      Optional[str] = None,
    date_from:   Optional[str] = None,
    date_to:     Optional[str] = None,
    session_id:  Optional[int] = None,
):
    """Export ประวัติ measurement (พร้อม filter) เป็นไฟล์ CSV ให้ดาวน์โหลด

    ทำไมต้องใช้ Pandas: เป็นวิธีที่ง่ายที่สุดในการแปลง row จาก DB ให้เป็น CSV
    ที่ถูกต้อง (จัดการ type, encoding, ค่าที่ขาดหายไป) โดยไม่ต้องเขียน CSV เอง
    ใช้ utf-8-sig encoding เพื่อให้เปิดใน Excel ได้ถูกต้องแม้มีตัวอักษรไทยอยู่ใน
    ไฟล์ (BOM ช่วยไม่ให้ตัวอักษรเพี้ยน) ใช้ filter เดียวกันกับ list_measurements
    เพื่อให้ข้อมูลที่แสดงในตารางตรงกับที่ export ออกมา
    """
    conditions, params = [], []
    if number_alpl is not None:
        conditions.append("m.number_alpl = %s"); params.append(number_alpl)
    if result:
        conditions.append("m.result = %s"); params.append(result)
    if date_from:
        conditions.append("m.timestamp >= %s"); params.append(date_from)
    if date_to:
        conditions.append("m.timestamp <= %s"); params.append(date_to)
    if session_id is not None:
        conditions.append("m.session_id = %s"); params.append(session_id)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(
                f"{MEASUREMENTS_SELECT} {where} ORDER BY m.timestamp DESC", params
            )
            rows = cur.fetchall()
    finally:
        db.close()

    df  = pd.DataFrame(rows)
    buf = StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=measurements.csv"},
    )


# ══════════════════════════════════════════════════════════════════════════════
# Static dashboard files (index.html / edit.html)
# ══════════════════════════════════════════════════════════════════════════════
# ต้องอยู่ล่างสุดของไฟล์เสมอ — mount ที่ "/" ทำหน้าที่เป็น catch-all ให้ทุก
# path ที่ไม่ตรงกับ route ไหนเลยด้านบน ถ้า register ไว้ก่อน (เช่นบนสุดของไฟล์)
# มันจะดักจับ request ของ /api/... ไปหมดก่อนถึง route จริง ทำให้ API พังทันที
#
# โครงสร้างจริงของโปรเจกต์เป็นแบบนี้ (คนละโฟลเดอร์กับ main.py):
#   TM-X_Project/
#     Backend-server/main.py   (ไฟล์นี้)
#     Backend-pc_station/agent_real.py
#     Frontend/index.html, edit.html, ...
# ดังนั้นต้องถอยขึ้นไป 1 ชั้นจาก main.py แล้วเข้าโฟลเดอร์ Frontend แทนที่จะใช้
# โฟลเดอร์เดียวกับไฟล์นี้ตรงๆ (ที่พังก่อนหน้านี้เพราะ index.html ไม่ได้อยู่ใน
# Backend-server/ ด้วย)
#
# html=True ทำให้เข้า "/" แล้วได้ index.html อัตโนมัติ และเข้า "/edit.html"
# ได้ตรงๆ — เหตุผลที่ทำแบบนี้แทนรัน web server แยก: จะได้มีแค่ process เดียว
# (uvicorn) ให้ autostart/ผูก host=127.0.0.1 ตัวเดียวจบ ไม่ต้องเปิดอีก process
# มาเสิร์ฟไฟล์ static ต่างหาก
_frontend_dir = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Frontend")
)
app.mount(
    "/",
    StaticFiles(directory=_frontend_dir, html=True),
    name="static",
)