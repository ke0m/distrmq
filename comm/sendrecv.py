"""
Functions for sending and receiving data over
sockets using zmq

@author: Joseph Jennings
@version: 2020.08.16
"""
import pickle
import zlib, lz4.frame
import types

def send_next_chunk(socket,gen,flags=0, protocol=-1, zlevel=-1):
  """
  Sends the next chunk to the workers

  Parameters:
    gen - a generator that returns the next chunk
  """
  if(isinstance(gen,types.GeneratorType)):
    try:
      chunk = next(gen)
      send_zipped_pickle(socket,chunk,protocol=protocol,zlevel=zlevel)
    except StopIteration:
      chunk = {}
      send_zipped_pickle(socket,chunk)
  else:
    raise Exception("Please provide a valid generator as input")

def notify_server(socket):
  """
  Notifies a server that the client is ready
  for data and computation

  Parameters:
    socket - the ZMQ socket
  """
  mydict = dict({'msg': "available"})
  send_zipped_pickle(socket,mydict)

def send_zipped_pickle(socket, obj, flags=0, protocol=-1, zlevel=0):
  """pickle an object, and zip the pickle before sending it"""
  p = pickle.dumps(obj, protocol)
  z = lz4.frame.compress(p,compression_level=zlevel)
  return socket.send(z, flags=flags)

def recv_zipped_pickle(socket, flags=0, protocol=-1):
  """inverse of send_zipped_pickle"""
  z = socket.recv(flags)
  p = lz4.frame.decompress(z)
  return pickle.loads(p)

