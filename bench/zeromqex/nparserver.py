import numpy as np
import matplotlib.pyplot as plt
import time
import zmq

def array_to_msg(nparray):
  _shape = np.array(nparray.shape, dtype='int32')
  return [nparray.dtype.name.encode(),_shape.tostring(),nparray.tostring()]

def msg_to_array(msg):
  _dtype_name = msg[0].decode()
  _shape = np.frombuffer(msg[1], np.int32)
  _array = np.frombuffer(msg[2], _dtype_name)
  return _array.reshape(tuple(_shape))

# Bind to socket
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://0.0.0.0:5555")

# Create np array
arr = np.random.rand(100,100)

msg = array_to_msg(arr)

while True:
  message = socket.recv()
  if(message == b'available'):
    socket.send_multipart(msg)
    datmsg  = socket.recv_multipart()
    arrb = msg_to_array(datmsg)
    break

print(np.min(arrb/arr),np.max(arrb/arr))

