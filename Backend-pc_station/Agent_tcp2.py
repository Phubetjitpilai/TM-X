"""
Agent เวอร์ชันทดสอบ (minimal):
  1. รับคำสั่ง Start + template จาก Backend ผ่าน POST /command
  2. ต่อ TM-X → R0 → PW,1,<template>
  3. รอพิมพ์เริ่มที่ terminal (แทน trigger จาก Micro)
  4. ส่ง T1 + GM,0,0 จำนวน 5 รอบ แล้ว print ค่าที่ได้ออกมาเฉยๆ
  * ยังไม่ส่งค่าขึ้น Backend และไม่มี error handling — ไว้ทดสอบ flow ล้วนๆ
"""
import socket
import time
import threading

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

# การตั้งค่า IP และ Port ให้ตรงกับ TM-X (เหมือน tcp.py)
TMX_IP = '192.168.10.11'
TMX_PORT = 8600
BUFFER_SIZE = 1024

http_app = FastAPI()


def send_command(sock, command):
    """ส่งคำสั่งไปยัง TM-X และรอรับผลลัพธ์ตอบกลับ (ยกมาจาก tcp.py ตรงๆ)"""
    # ต้องต่อท้ายด้วยตัวคั่น (Delimiter) เสมอ ในที่นี้คือ CR (\r)
    cmd_to_send = command + '\r'
    sock.sendall(cmd_to_send.encode('ascii'))
    time.sleep(0.1)  # หน่วงเวลาให้กล้องประมวลผลเล็กน้อย
    response = sock.recv(BUFFER_SIZE).decode('ascii').strip()
    print(f"[ส่งคำสั่ง]: {command.ljust(10)} | [ตอบกลับ]: {response}")
    return response


def measurement_flow(template_name: str):
    """Flow หลัก — รันใน thread แยกเพื่อไม่ block FastAPI server
    (input() กับ socket เป็น blocking ทั้งคู่ ถ้ารันใน event loop ตรงๆ
    endpoint /command จะค้างไปด้วย)
    """
    print(f"\n✅ ได้รับคำสั่ง Start — template_name={template_name!r}")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(5.0)

    print(f"Connecting to TM-X at {TMX_IP}:{TMX_PORT}...")
    client_socket.connect((TMX_IP, TMX_PORT))
    print("Connected successfully!\n")

    # Reset (เข้าโหมดดำเนินงาน) — sleep 0.5 ตาม tcp.py ที่ทดสอบผ่านแล้ว
    send_command(client_socket, "R0")
    time.sleep(0.5)

    # Load Program ตาม template ที่ backend ส่งมา (zero-pad เป็น 3 หลัก)
    program_no = template_name.zfill(3)
    send_command(client_socket, f"PW,1,{program_no}")
    time.sleep(1.0)

    # รอสัญญาณว่าชิ้นงานพร้อม (แทน trigger จาก Micro ด้วยการพิมพ์ไปก่อน)
    input("พิมพ์เริ่ม: ")

    # T1 (trigger สั่งวัด) + GM (ดึงค่า) 5 รอบ แล้ว print ค่าออกมาเฉยๆ
    for j in range(5):
        send_command(client_socket, "T1")
        response_data = send_command(client_socket, "GM,0,0").split(',')

        values = []
        for i in response_data:
            i = i.strip('-').strip('+')
            if i == "9999.999":
                continue
            values.append(i)

        print(f"Round {j + 1} | Values : {values[-2:]}\n")

    # จบการทำงาน — กลับโหมดตั้งค่า แล้วปิด connection
    send_command(client_socket, "S0")
    time.sleep(0.5)
    client_socket.close()
    print("\nConnection closed.")


class CommandRequest(BaseModel):
    action: str
    session_id: int | None = None
    template_name: str | None = None
    number_alpl: str | None = None
    target_count: int | None = None


@http_app.post("/command")
async def command(req: CommandRequest):
    if req.action == "start":
        # รัน flow ใน thread แยก — ดู docstring ของ measurement_flow
        threading.Thread(target=measurement_flow, args=(req.template_name or "",), daemon=True).start()
    return {"status": "ok", "action": req.action}


if __name__ == "__main__":
    # port ต้องตรงกับ AGENT_PORT ใน main.py ของ backend (default 9998)
    print("Agent (minimal) กำลังรอคำสั่ง Start จาก Backend ที่ port 9998...")
    uvicorn.run(http_app, host="0.0.0.0", port=9998)
