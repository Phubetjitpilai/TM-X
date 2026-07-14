"""
Agent เวอร์ชันทดสอบ (minimal):
  1. รับคำสั่ง Start + template จาก Backend ผ่าน POST /command
  2. ต่อ TM-X → R0 → PW,1,<template>
  3. รอพิมพ์เริ่มที่ terminal (แทน trigger จาก Micro)
  4. ส่ง T1 + GM,0,0 จำนวน 5 รอบ → หาคู่ฐานนิยม → ได้ value_x, value_y
  5. POST ค่าเข้า backend ที่ /api/measurements (format ตาม MeasurementCreate
     ใน main.py: session_id, number_alpl, value_x, value_y, client_uuid)
"""
import socket
import time
import threading
import uuid
from collections import Counter

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

# การตั้งค่า IP และ Port ให้ตรงกับ TM-X (เหมือน tcp.py)
TMX_IP = '192.168.10.11'
TMX_PORT = 8600
BUFFER_SIZE = 1024
BACKEND_URL = "http://localhost:8000"
TMX_ROUNDS = 5

http_app = FastAPI()


def send_command(sock, command):
    """ส่งคำสั่งไปยัง TM-X และรอรับผลลัพธ์ตอบกลับ (ยกมาจาก tcp.py ตรงๆ)
    ไม่ print คำสั่ง/response แต่ละตัวแล้ว — Ball ขอให้ log แสดงแค่ค่า
    value_x/value_y สุดท้ายที่ถูกเลือกเท่านั้น
    """
    cmd_to_send = command + '\r'  # ต้องต่อท้ายด้วยตัวคั่น CR (\r) เสมอ
    sock.sendall(cmd_to_send.encode('ascii'))
    time.sleep(0.1)  # หน่วงเวลาให้กล้องประมวลผลเล็กน้อย
    response = sock.recv(BUFFER_SIZE).decode('ascii').strip()
    return response


def read_one_round(sock):
    """1 รอบ: T1 (trigger) + GM (ดึงค่า) → คืน (x_str, y_str)
    ตัด +/- ทิ้ง, ข้าม placeholder 9999.999/9999.9999, เอา 2 ค่าสุดท้าย
    """
    send_command(sock, "T1")
    response_data = send_command(sock, "GM,0,0").split(',')

    values = []
    for i in response_data:
        i = i.strip('-').strip('+')
        if i in ("9999.999", "9999.9999"):
            continue
        values.append(i)

    return values[-2], values[-1]


def pick_mode_pair(pairs):
    """หา "คู่" (x, y) ที่ปรากฏบ่อยที่สุดจากทั้ง 5 รอบ — นับเป็นคู่ไม่แยกแกน
    เพื่อให้ x กับ y ที่เลือกมาจากรอบการวัดเดียวกันจริงๆ
    """
    counts = Counter(pairs)
    most_common_pair, _ = counts.most_common(1)[0]
    return most_common_pair


def post_to_backend(session_id, number_alpl, value_x, value_y):
    """POST ค่าเข้า backend — format ตรงตาม MeasurementCreate ใน main.py"""
    resp = httpx.post(
        f"{BACKEND_URL}/api/measurements",
        json={
            "session_id":  session_id,
            "number_alpl": number_alpl,
            "value_x":     value_x,
            "value_y":     value_y,
            "client_uuid": str(uuid.uuid4()),
        },
        timeout=10,
    )
    return resp


def measurement_flow(session_id, template_name, number_alpl, target_count):
    """Flow หลัก — รันใน thread แยกเพื่อไม่ block FastAPI server"""
    print(f"\n✅ ได้รับคำสั่ง Start — template={template_name!r}, จำนวน {target_count} ชิ้น")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(5.0)
    client_socket.connect((TMX_IP, TMX_PORT))

    # Reset (เข้าโหมดดำเนินงาน) — sleep 0.5 ตาม tcp.py ที่ทดสอบผ่านแล้ว
    send_command(client_socket, "R0")
    time.sleep(0.5)

    # Load Program ตาม template ที่ backend ส่งมา (zero-pad เป็น 3 หลัก)
    send_command(client_socket, f"PW,1,{str(template_name).zfill(3)}")
    time.sleep(1.0)

    for piece in range(1, (target_count or 1) + 1):
        # รอสัญญาณว่าชิ้นงานพร้อม (แทน trigger จาก Micro ด้วยการพิมพ์ไปก่อน)
        input(f"\nชิ้นที่ {piece}/{target_count} — พิมพ์เริ่ม: ")

        # T1+GM 5 รอบ เก็บเป็น "คู่" (x, y) ของแต่ละรอบ
        pairs = [read_one_round(client_socket) for _ in range(TMX_ROUNDS)]

        # หาคู่ฐานนิยม → ตำแหน่ง 0 = value_x, ตำแหน่ง 1 = value_y
        mode_pair = pick_mode_pair(pairs)
        value_x = float(mode_pair[0])
        value_y = float(mode_pair[1])

        print(f"Value X : {value_x}")
        print(f"Value Y : {value_y}")

        resp = post_to_backend(session_id, number_alpl, value_x, value_y)
        data = resp.json()
        print(f"→ ส่งให้ Backend แล้ว (result={data.get('result')}, {data.get('measured')}/{data.get('target')})")

        # backend ตอบ complete = วัดครบ session แล้ว หยุดเลย
        if data.get("status") == "complete":
            break

    # จบการทำงาน — กลับโหมดตั้งค่า แล้วปิด connection
    send_command(client_socket, "S0")
    time.sleep(0.5)
    client_socket.close()
    print("\n✅ จบ session — ปิดการเชื่อมต่อ TM-X แล้ว")


class CommandRequest(BaseModel):
    action: str
    session_id: int | None = None
    template_name: str | None = None
    number_alpl: int | None = None
    target_count: int | None = None


@http_app.post("/command")
async def command(req: CommandRequest):
    if req.action == "start":
        threading.Thread(
            target=measurement_flow,
            args=(req.session_id, req.template_name, req.number_alpl, req.target_count),
            daemon=True,
        ).start()
    return {"status": "ok", "action": req.action}


if __name__ == "__main__":
    # port ต้องตรงกับ AGENT_PORT ใน main.py ของ backend (default 9998)
    print("Agent (minimal) กำลังรอคำสั่ง Start จาก Backend ที่ port 9998...")
    uvicorn.run(http_app, host="0.0.0.0", port=9998)
