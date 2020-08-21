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

def generate_chunks(n):
  i = 0
  while i < n:
    mydict = {}
    mydict['dat'] = np.random.rand(1000,1000).astype('float32')
    yield mydict
    i += 1

def send_next_chunk(socket,gen,flags=0, protocol=-1):
  try:
    chunk = next(gen)
    send_zipped_pickle(socket,chunk)
  except StopIteration:
    chunk = {}
    send_zipped_pickle(socket,chunk)

# Bind to socket
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://0.0.0.0:5555")

# Create the chunks
nimg = 2
chunks = generate_chunks(nimg)

imgs = []
while(len(imgs) < nimg):
  rdict = recv_zipped_pickle(socket)
  if(rdict['msg'] == "available"):
    # Send work
    send_next_chunk(socket,chunks)
  elif(rdict['msg'] == "result"):
    imgs.append(rdict['dat'])
    socket.send(b"")

