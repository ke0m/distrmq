import numpy as np
import zmq
from server.distribute import dstr_collect, dstr_sum
from server.utils import startserver, stopserver
from client.pbsworkers import launch_pbsworkers, kill_pbsworkers, restart_pbsworkers
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
wrkrs,status = launch_pbsworkers(cfile,nworkers=3,queue='default',
                                 logpath=logpath,slpbtw=0.5,chkrnng=True)

print(status)

# Bind to socket
context,socket = startserver()

for itry in range(6):
  okeys = ['result','scale']
  #output = dstr_collect(okeys,nimg,chunks,socket)

  # Create the chunks
  if(itry == 2 or itry == 4):
    kill_pbsworkers(wrkrs,clean=False)
    stopserver(context,socket)
    wrkrs,status = restart_pbsworkers(wrkrs)
    context,socket = startserver()

  nimg = status.count('R')
  chunks = generate_chunks(nimg)
  output = dstr_sum('scale','result',nimg,chunks,socket,600000)
  print(output)

#print(output['scale'])
#for k in range(len(output['result'])):
#  print(output['result'][k])
print(output)

kill_pbsworkers(wrkrs)

