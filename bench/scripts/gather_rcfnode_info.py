import numpy as np
from client.pbsworkers import pbsworker, get_nodes_inuse, get_workers_status
from genutils.ptyprint import create_inttag
import time

nbeg = 1; nend = 148

# First get nodes in use
used = get_nodes_inuse()
print(used)

vrybad = ['rcf033','rcf034','rcf035','rcf036','rcf037','rcf038','rcf039','rcf040','rcf131','rcf147','rcf148']

ntry = 5
cmd = 'sleep 30 && echo "Hello from $HOSTNAME"'

good = []; bad = []

# Loop over each node in the list
for inode in range(nbeg,nend+1):
  # Get the hostname
  hname = 'rcf' + create_inttag(inode,nend)
  if(hname in used or hname in vrybad): continue
  # Create the worker
  print("Submitting to node %s"%(hname))
  wkr = pbsworker(cmd,name='info-',logpath='./log')
  wkr.submit(queue='default',host=hname)
  status = []
  for itry in range(ntry):
    time.sleep(2)
    status += get_workers_status([wkr])
  if(status.count('R') >= 3):
    print("Good")
    good.append(hname)
  else:
    print("Bad")
    bad.append(hname)
    wkr.delete()

print(good)

print(bad)
