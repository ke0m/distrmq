from comm.sendrecv import recv_zipped_pickle, send_next_chunk
from client.slurmworkers import launch_tsworkers, restart_workers
import numpy as np
import time

def dstr_collect(keys,n,gen,socket):
  """
  Distributes data to workers
  and collects the results based on the keys passed

  Parameters:
    keys   - list of keys to expect to receive from client
    n      - length of input generator
    gen    - an input generator that gives a chunk
    socket - a ZMQ socket

  Returns a dictionary with keys of keys and values
  returned by the client
  """
  # Create the outputs
  odict = {}
  for ikey in keys: odict[ikey] = []
  # Control key
  ckey = keys[0]
  # Send and collect work
  while(len(odict[ckey]) < n):
    # Talk to client
    rdict = recv_zipped_pickle(socket)
    if(rdict['msg'] == "available"):
      # Send work
      send_next_chunk(socket,gen)
    elif(rdict['msg'] == "result"):
      # Save the results
      for ikey in keys:
        odict[ikey].append(rdict[ikey])
      # Send a "thank you" back
      socket.send(b"")

  return odict

def dstr_collect_adapt(keys,n,gen,socket,workers,interval=5):
  """
  Distributes data to workers
  and collects the results based on the keys passed

  Parameters:
    keys     - list of keys to expect to receive from client
    n        - length of input generator
    gen      - an input generator that gives a chunk
    socket   - a ZMQ socket
    workers  - a list of slurm workers
    interval - frequency (in minutes) for growing and restarting workers [5]

  Returns a dictionary with keys of keys and values
  returned by the client
  """
  # Create the outputs
  odict = {}
  for ikey in keys: odict[ikey] = []
  # Control key
  ckey = keys[0]
  # Start the timer
  btime = time.time()

  # Send and collect work
  while(len(odict[ckey]) < n):
    # Talk to client
    rdict = recv_zipped_pickle(socket)
    if(rdict['msg'] == "available"):
      # Send work
      send_next_chunk(socket,gen)
    elif(rdict['msg'] == "result"):
      # Save the results
      for ikey in keys:
        odict[ikey].append(rdict[ikey])
      # Send a "thank you" back
      socket.send(b"")
    itime = (time.time() - btime)/60.0
    # Attempt to get more workers
    if(itime >= interval):
      launch_tsworkers(workers)
      # Restart if workers are nearing completion
      restart_workers(workers)
      # Restart the time
      btime = time.time()

  return odict

def dstr_sum(ckey,rkey,n,gen,socket,shape):
  """
  Distributes data to workers
  and sums over the collected results

  Parameters:
    ckey   - a control key for managing submissions
    rkey   - a result key for summing the results
    n      - length of input generator
    gen    - an input generator that gives a junk
    socket - a ZMQ socket
    shape  - the shape of the output array

  Returns:
    Sums over the work returned by workers to give an
    output array of size shape
  """
  # Create the outputs
  out = np.zeros(shape,dtype='float32')
  nouts = []
  # Send and sum over collected results
  while(len(nouts) < n):
    rdict = recv_zipped_pickle(socket)
    if(rdict['msg'] == "available"):
      # Send work
      send_next_chunk(socket,gen)
    elif(rdict['msg'] == "result"):
      nouts.append(rdict[ckey])
      out += rdict[rkey]
      socket.send(b"")

  return out

