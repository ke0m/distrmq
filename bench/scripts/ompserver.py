import numpy as np
import zmq
from server.distribute import dstr_collect, dstr_sum
from client.pbsworkers import launch_pbsworkers, kill_pbsworkers
import time

def generate_chunks(n):
  i = 0
  while i < n:
    mydict = {}
    mydict['dat'] = np.zeros([600000],dtype='float32') + 20
    mydict['scale'] = i+1
    mydict['ntry'] = 100000
    mydict['nthrds'] = 16
    yield mydict
    i += 1

# Start workers
cfile = "/data/sep/joseph29/projects/distrmq/bench/scripts/ompclient.py"
logpath = "./log"
wrkrs,status = launch_pbsworkers(cfile,nworkers=5,queues=['default'],
                                 logpath=logpath,slpbtw=0.5,chkrnng=True)

print(status)

# Create the chunks
nimg = status.count('R')
chunks = generate_chunks(nimg)

# Bind to socket
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://0.0.0.0:5555")

okeys = ['result','scale']
#output = dstr_collect(okeys,nimg,chunks,socket)
output = dstr_sum('scale','result',nimg,chunks,socket,600000)

#print(output['scale'])
#for k in range(len(output['result'])):
#  print(output['result'][k])
print(output)

kill_pbsworkers(wrkrs)

