import numpy as np
import matplotlib.pyplot as plt
import time
import zlib
import dill as pickle
import zmq

def send_zipped_pickle(socket, obj, flags=0, protocol=-1):
  """pickle an object, and zip the pickle before sending it"""
  p = pickle.dumps(obj, protocol)
  z = zlib.compress(p)
  return socket.send(z, flags=flags)

def recv_zipped_pickle(socket, flags=0, protocol=-1):
  """inverse of send_zipped_pickle"""
  z = socket.recv(flags)
  p = zlib.decompress(z)
  return pickle.loads(p)

# Bind to socket
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://0.0.0.0:5555")

# Create np array
arr1 = np.random.rand(1000,1000)
arr2 = np.zeros([1000,1000],dtype='float32')

mydict = {}
mydict['msg']  = ''
mydict['arr1'] = arr1
mydict['arr2'] = arr2

while True:
  message = socket.recv()
  if(message == b'available'):
    send_zipped_pickle(socket,mydict)
    mydict = recv_zipped_pickle(socket)
    if(mydict['msg'] == 'stop'):
      break

out = mydict['arr2']/mydict['arr1']

print(np.min(out),np.max(out))

