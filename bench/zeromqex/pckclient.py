import numpy as np
import matplotlib.pyplot as plt
import time
import zlib
import pickle
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

print("Connecting to numpy server...")
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://oas.stanford.edu:5555")

# Tell server we are ready
socket.send(b"available")

# Receive data from server (waits here until server is started)
rdict = recv_zipped_pickle(socket)

rdict['arr2'] = 2*rdict['arr1']
rdict['msg'] = 'stop'

send_zipped_pickle(socket,rdict)

#socket.send(b"stop")


