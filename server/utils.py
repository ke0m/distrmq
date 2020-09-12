"""
Utility functions for the server side

@author: Joseph Jennings
@version: 2020.09.03
"""
import zmq

def splitnum(num,div):
  """ Splits a number into nearly even parts """
  splits = []
  igr,rem = divmod(num,div)
  for i in range(div):
    splits.append(igr)
  for i in range(rem):
    splits[i] += 1

  return splits

def startserver(address="tcp://0.0.0.0:5555"):
  """
  Starts the server. A ZMQ REP socket

  Parameters:
   address - the address at which to bind the server
             socket

  Returns a ZMQ context and a REP socket
  """
  context = zmq.Context()
  socket = context.socket(zmq.REP)
  socket.bind(address)

  return context, socket

def stopserver(context,socket,address="tcp://0.0.0.0:5555"):
  """
  Restarts the server. A ZMQ REP socket

  Parameters:
    context - an active ZMQ context
    socket  - a bound ZMQ REP socket
  """
  socket.close()
  context.destroy()

