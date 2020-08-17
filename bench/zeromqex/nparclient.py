import numpy as np
import matplotlib.pyplot as plt
import time
import zmq 

def msg_to_array(msg):
  _dtype_name = msg[0].decode()
  _shape = np.frombuffer(msg[1], 'int32')
  _array = np.frombuffer(msg[2], _dtype_name)
  return _array.reshape(tuple(_shape))

def array_to_msg(nparray):
  _shape = np.array(nparray.shape, dtype='int32')
  return [nparray.dtype.name.encode(),_shape.tostring(),nparray.tostring()]

print("Connecting to numpy server...")
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://oas.stanford.edu:5555")

# Tell server we are ready
socket.send(b"available")

# Receive data from server
message = socket.recv_multipart()
arr = msg_to_array(message)

# Do something with data
arrb = 2*arr

# Send back to server
msgb = array_to_msg(arrb)
socket.send_multipart(msgb)

#plt.figure()
#plt.imshow(arr,cmap='jet')
#plt.show()

