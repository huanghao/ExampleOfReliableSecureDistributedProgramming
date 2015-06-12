import sys
import socket

addr = ('127.0.0.1', 4000)
if len(sys.argv) > 1:
    data = sys.argv[1]
else:
    data = 'hello'

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.sendto(data.encode(), addr)
