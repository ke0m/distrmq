from server.distribute import dstr_collect, dstr_sum
from client.sshworkers import launch_sshworkers, kill_sshworkers

# Start workers
hosts = ['worker1','worker2','worker3'] # For SSH provide host names
# For SLURM/PBS, provide job information
cfile = "/homes/sep/joseph29/projects/scaleup/bench/zeromqex/ompclient.py"
launch_sshworkers(cfile,hosts=hosts,sleep=1,verb=1)

# Create the chunks
nchunk = 10
chunks = generate_chunks(nchunk)

# Bind to socket
context = zmq.Context()
socket = context.socket(zmq.REP) # This a "reply" type ZMQ socket
socket.bind("tcp://0.0.0.0:5555")

okeys = ['result','other'] # Tell client which keys to return
# Distribute work and collect
output = dstr_collect(okeys,nimg,chunks,socket)
# Distribute work and sum
#output = dstr_sum('scale','result',nimg,chunks,socket,6000000)

# Process received output
print(output['scale'])
for k in range(len(output['result'])):
  print(output['result'][k])
#print(output)

# Kill workers
kill_sshworkers(cfile,hosts,verb=False)

