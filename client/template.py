"""
A template for a client (worker) that will
be launched on a cluster

@author: Joseph Jennings
@version: 2020.08.17
"""
import zmq # ZMQ sockets
from comm.sendrecv import notify_server, send_zipped_pickle, recv_zipped_pickle # Comm functions
from foo import foo # Function that will do the work

# Connect to socket
context = zmq.Context()
socket = context.socket(zmq.REQ) # this is a "request" type ZMQ socket
socket.connect("tcp://serveraddr:5555")

# Listen for work from server
while True:
  # Notify we are ready
  notify_server(socket)
  # Get work
  chunk = recv_zipped_pickle(socket)
  # If chunk is empty, keep listening
  if(chunk == {}):
    continue
  # If I received something, do some work
  ochunk = {}
  ochunk['result'] = foo(chunk)
  # Return other parameters if desired
  ochunk['other']  = chunk['other']
  # Tell server this is the result
  ochunk['msg'] = "result"
  # Send back the result
  send_zipped_pickle(socket,ochunk)
  # Receive 'thank you'
  socket.recv()

