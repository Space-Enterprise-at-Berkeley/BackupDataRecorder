from socket import *
import time

PORT = 42069
ADDRESS = "localhost"

sock = socket(AF_INET, SOCK_DGRAM)
sock.connect((ADDRESS, PORT))

for i in range(50000):
    sock.send((i).to_bytes(256, "big"))
    time.sleep(.0001)

