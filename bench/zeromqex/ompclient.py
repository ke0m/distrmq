import zmq
from comm.sendrecv import notify_server, send_zipped_pickle, recv_zipped_pickle
from oway.ompwrapper import ompwrap
from socket import gethostname

# Connect to socket
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://maz-login01:5555")

while True:
  notify_server(socket)
  chunk = recv_zipped_pickle(socket)
  if(chunk == {}):
    continue
  ochunk = {}
  ochunk['result'] = ompwrap(chunk['dat'],chunk['scale'],chunk['ntry'],chunk['nthrds'])
  ochunk['scale'] = chunk['scale']
  ochunk['msg'] = "result"
  send_zipped_pickle(socket,ochunk)
  socket.recv()

