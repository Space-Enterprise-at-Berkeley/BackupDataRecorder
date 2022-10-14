import psql_handler
import threading
from socket import *
import time

class DataRecorder:
    def __init__(self, PSQL_CONFIG, DATABASE_CONFIG, SOCKET_CONFIG, RECORDER_CONFIG):
        self.PSQL_CONFIG = PSQL_CONFIG
        self.DATABASE_CONFIG = DATABASE_CONFIG
        self.SOCKET_CONFIG = SOCKET_CONFIG
        self.RECORDER_CONFIG = RECORDER_CONFIG
        self.buffer_size = int(self.RECORDER_CONFIG["buffer"])
        self.loop_time = eval(self.RECORDER_CONFIG["loop_time"])
        self.buffer = []
        self.packet_num = 0
    
    def setup_server(self):
        self.connection = psql_handler.load_database(self.PSQL_CONFIG)
        psql_handler.load_table(self.connection, self.DATABASE_CONFIG)
        
    def buffer_data(self, data):
        self.buffer.append(data)
        
    def save_loop(self):
        while True:
            if len(self.buffer) < self.buffer_size:
                time.sleep(self.loop_time)
            else:
                self.save_data()
        
    def save_data(self):
        buffer_copy = self.buffer
        self.buffer = []
        psql_handler.save_data(self.connection, self.DATABASE_CONFIG["table_name"], self.DATABASE_CONFIG["columns"], buffer_copy)
        
    def setup_socket(self):
        print("Setting up socket")
        self.sock = socket(eval(self.SOCKET_CONFIG["family"]), eval(self.SOCKET_CONFIG["type"]))
        self.sock.bind(("localhost", eval(self.SOCKET_CONFIG["port"])))
        
    def start_recording(self):
        packet_size = eval(self.SOCKET_CONFIG["packet_size"])
        t = threading.Thread(target=self.save_loop)
        t.start()
        print("="*20)
        print("="*20)
        print("Started Recording")
        while True:
            data = self.sock.recv(packet_size)
            self.buffer_data((self.packet_num, data))
            self.packet_num += 1
            