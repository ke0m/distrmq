import numpy as np
from server.distribute import dstr_collect, dstr_sum
from server.utils import startserver, stopserver
from client.slurmworkers import launch_slurmworkers, kill_slurmworkers, restart_slurmworkers
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
cfile = "/home/joseph29/projects/distrmq/bench/zeromqex/ompclient.py"
logpath = "./log"
wrkrs,status = launch_slurmworkers(cfile,nworkers=15,queue='sep',block=['maz132'],
                                   logpath=logpath,slpbtw=4.0,mode='adapt')

print(status)

# Start server
context,socket = startserver()

for itry in range(6):

  # Create the chunks
  if(itry == 2 or itry == 4):
    print("restarting workers ...")
    #wrkrs[0].delete(); wrkrs[0].submit(restart=True,sleep=3)
    stopserver(context,socket)
    status = restart_slurmworkers(wrkrs,limit=False,slpbtw=4)
    context,socket = startserver()
    #wrkrs1,status1 = launch_slurmworkers(cfile,nworkers=5,queue='twohour',
    #                                   logpath=logpath,slpbtw=0.5,chkrnng=True)
    #status += status1
    #wrkrs  += wrkrs1
  print(status)

  nimg = status.count('R')
  chunks = generate_chunks(nimg)
  okeys = ['result','scale']
  #output = dstr_collect(okeys,nimg,chunks,socket)
  output = dstr_sum('scale','result',nimg,chunks,socket,6000000)
  print(output)

#print(output['scale'])
#for k in range(len(output['result'])):
#  print(output['result'][k])
print(output)

kill_slurmworkers(wrkrs)

