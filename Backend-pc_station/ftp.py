from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

def start_ftp():
    authorizer = DummyAuthorizer()
    
    # กำหนด Username, Password, โฟลเดอร์ที่ใช้รับไฟล์ และสิทธิ์ 'elradfmw' (อ่าน/เขียน/แก้ไขได้เต็มที่)
    authorizer.add_user("INTERN_USER", "123456", "C:\\Users\\PMehom\\Desktop\\TM-X\\Images", perm="elradfmw")
    
    handler = FTPHandler
    handler.authorizer = authorizer
    
    # ระบุ IP Address ของ PC คุณ (0.0.0.0 หมายถึงรับทุก IP ในเครื่อง)
    # พอร์ตมาตรฐานของ FTP คือ 21
    server = FTPServer(("0.0.0.0", 21), handler)
    print("FTP Server is running... Waiting for Keyence images.")
    server.serve_forever()

if __name__ == "__main__":
    start_ftp()