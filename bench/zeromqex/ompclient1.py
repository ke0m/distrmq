import zmq
from comm.sendrecv import notify_server, send_zipped_pickle, recv_zipped_pickle
from oway.ompwrapper import ompwrap

#print("Connecting to numpy server...")
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://oas.stanford.edu:5555")

while True:
  mydict = dict({'msg': "available"})
  send_zipped_pickle(socket,mydict)
  chunk = recv_zipped_pickle(socket)
  if(chunk == {}):
    continue
  ochunk = {}
  ochunk['result'] = ompwrap(chunk['dat'],chunk['scale'],chunk['ntry'],chunk['nthrds'])
  ochunk['scale'] = chunk['scale']
  ochunk['msg'] = "result"
  send_zipped_pickle(socket,ochunk)
  socket.recv()

