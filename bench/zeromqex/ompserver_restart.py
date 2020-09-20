import numpy as np
import zmq
from server.distribute import dstr_collect, dstr_sum
from server.utils import startserver, stopserver
from client.sshworkers import launch_sshworkers, kill_sshworkers
import time

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
hosts = ['fantastic','storm']
cfile = "/homes/sep/joseph29/projects/distrmq/bench/zeromqex/ompclient.py"
launch_sshworkers(cfile,hosts=hosts,sleep=1,verb=1)

context,socket = startserver()

for i in range(4):
  if(i == 2):
    kill_sshworkers(cfile,hosts,verb=False)
    stopserver(context,socket)
    time.sleep(3)
    launch_sshworkers(cfile,hosts=hosts,sleep=1,verb=1)
    context,socket = startserver()

  # Create the chunks
  nimg = len(hosts)
  chunks = generate_chunks(nimg)

  okeys = ['result','scale']
  output = dstr_collect(okeys,nimg,chunks,socket)
  #output = dstr_sum('scale','result',nimg,chunks,socket,6000000)

  print(output['scale'])
  for k in range(len(output['result'])):
    print(output['result'][k])
  #print(output)

kill_sshworkers(cfile,hosts,verb=False)

