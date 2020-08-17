"""
Hello world server in Python
Binds REP socket to tcp://*:5555
Expects b"Hello" from client, replies with b"World"
"""

import time
import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://0.0.0.0:5555")

while True:
  # Wait for next request from client
  message = socket.recv()
  print("Received request: %s"%message)
  if(message == b'Hello'):

    # Do some 'work'
    time.sleep(1)

    # Send reply back to client
    socket.send(b"World")

  elif(message == b'Stop'):
    break

