import socket
import time

# การตั้งค่า IP และ Port ให้ตรงกับ TM-X
TMX_IP = '192.168.10.11'
TMX_PORT = 8600
BUFFER_SIZE = 1024
values = []

def send_command(sock, command):
    """
    ฟังก์ชันสำหรับส่งคำสั่งไปยัง TM-X และรอรับผลลัพธ์ตอบกลับ
    """
    # ต้องต่อท้ายด้วยตัวคั่น (Delimiter) เสมอ ในที่นี้คือ CR (\r)
    cmd_to_send = command + '\r'
    
    # ส่งข้อมูลไปยังกล้อง
    sock.sendall(cmd_to_send.encode('ascii'))
    time.sleep(0.1) # หน่วงเวลาให้กล้องประมวลผลเล็กน้อย
    
    try:
        # รับข้อความตอบกลับจากกล้อง
        response = sock.recv(BUFFER_SIZE).decode('ascii').strip()
        # print(f"[ส่งคำสั่ง]: {command.ljust(15)} | [ตอบกลับ]: {response}")
        return response
    except Exception as e:
        print(f"Error reading response: {e}")
        return None

def main():
    # TCP Socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(5.0) 
    
    try:
        print(f"Connecting to TM-X at {TMX_IP}:{TMX_PORT}...")
        client_socket.connect((TMX_IP, TMX_PORT))
        print("Connected successfully!\n")
        
        # ----------------------------------------------------
        # ----------------------------------------------------
        
        # Reset
        send_command(client_socket, "R0")
        time.sleep(0.5)
        
        # Load Program
        send_command(client_socket, "PW,3,021")
        print("Waiting for program to load...\n")
        time.sleep(1.0) 

        # response_data = send_command(client_socket, "GM,0,0")
        # print(response_data)
    
        # Get Measurement
        for j in range(5):
            response_data = send_command(client_socket, "GM,0,0").split(',')
            # time.sleep(0.5)


            for i in response_data:
                i = i.strip('-')
                i = i.strip('+')
                # print(i)

                if i == "9999.999":
                    pass
                else:
                    # val = float(i[3:])
                    val = i
                    values.append(val)
        # send_command(client_socket, "GR,0")
        # time.sleep(0.5)

            print(f"Round {j+1} | Values : {values[-2:]}\n")
        # Stop
        send_command(client_socket, "S0")
        time.sleep(0.5)
        

    except Exception as e:
        print(f"Connection failed or error occurred: {e}")
    finally:
        client_socket.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    main()