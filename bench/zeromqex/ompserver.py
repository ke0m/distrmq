import numpy as np
import zmq
from server.distribute import dstr_collect, dstr_sum
from client.sshworkers import launch_sshworkers, kill_sshworkers
from comm.sendrecv import recv_zipped_pickle, send_next_chunk

def generate_chunks(n):
  i = 0
  while i < n:
    mydict = {}
    mydict['dat'] = np.zeros([6000000],dtype='float32') + 20
    mydict['scale'] = i+1
    mydict['ntry'] = 100000
    mydict['nthrds'] = 40
    yield mydict
    i += 1

# Start workers
hosts = ['thing','fantastic','storm','jarvis','torch']
cfile = "/homes/sep/joseph29/projects/scaleup/bench/zeromqex/ompclient.py"
launch_sshworkers(cfile,hosts=hosts,sleep=1,verb=1)

# Create the chunks
nimg = len(hosts)
chunks = generate_chunks(nimg)

# Bind to socket
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://0.0.0.0:5555")

#imgs = []; scales = []
#while(len(imgs) < nimg):
#  rdict = recv_zipped_pickle(socket)
#  if(rdict['msg'] == "available"):
#    # Send work
#    send_next_chunk(socket,chunks)
#  elif(rdict['msg'] == "result"):
#    scales.append(rdict['scale'])
#    imgs.append(rdict['result'])
#    socket.send(b"")

okeys = ['result','scale']
#output = dstr_collect(okeys,nimg,chunks,socket)
output = dstr_sum('scale','result',nimg,chunks,socket,6000000)

print(output)
#print(output['scale'])
#for k in range(len(output['result'])):
#  print(output['result'][k])

#print(scales)
#print(imgs[0],imgs[1],imgs[2],imgs[3],imgs[4])

kill_sshworkers(cfile,hosts,verb=False)

