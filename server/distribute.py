from comm.sendrecv import recv_zipped_pickle, send_next_chunk
import numpy as np
from genutils.ptyprint import printprogress

def dstr_collect(keys,n,gen,socket,zlevel=-1,verb=False):
  """
  Distributes data to workers
  and collects the results based on the keys passed

  Parameters:
    keys     - list of keys to expect to receive from client
    n        - length of input generator
    gen      - an input generator that gives a chunk
    socket   - a ZMQ socket
    zlevel   - level of compression [0]

  Returns a dictionary with keys of keys and values
  returned by the client
  """
  # Create the outputs
  odict = {}
  for ikey in keys: odict[ikey] = []
  # Control key
  ckey = keys[0]
  # Verbosity
  old = -1
  # Send and collect work
  while(len(odict[ckey]) < n):
    if(verb):
      if(old < len(odict[ckey])):
        printprogress(ckey+":",len(odict[ckey]),n)
        old = len(odict[ckey])
    # Talk to client
    rdict = recv_zipped_pickle(socket)
    if(rdict['msg'] == "available"):
      # Send work
      send_next_chunk(socket,gen,zlevel)
    elif(rdict['msg'] == "result"):
      # Save the results
      for ikey in keys:
        odict[ikey].append(rdict[ikey])
      # Send a "thank you" back
      socket.send(b"")

  if(verb): printprogress(ckey+":",len(odict[ckey]),n)

  return odict

def dstr_sum(ckey,rkey,n,gen,socket,shape,ikey='idx',zlevel=-1):
  """
  Distributes data to workers
  and sums over the collected results

  Parameters:
    ckey     - a control key for managing submissions
    rkey     - a result key for summing the results
    n        - length of input generator
    gen      - an input generator that gives a junk
    socket   - a ZMQ socket
    shape    - the shape of the output array
    ikey     - key for sending the index for chunked transfer ['idx']
    zlevel   - level of compression [0]

  Returns:
    Sums over the work returned by workers to give an
    output array of size shape
  """
  # Create the outputs
  out = np.zeros(shape,dtype='float32')
  chunks,nhx = False,1
  if(len(shape) > 3):
    chunks = True
    nhx = shape[1]
  nouts = []
  # Send and sum over collected results
  while(len(nouts)//nhx < n):
    rdict = recv_zipped_pickle(socket)
    if(rdict['msg'] == "available"):
      # Send work
      send_next_chunk(socket,gen,zlevel=zlevel)
    elif(rdict['msg'] == "result"):
      nouts.append(rdict[ckey])
      if(chunks):
        out[0,rdict[ikey]] += rdict[rkey]
      else:
        out += rdict[rkey]
      socket.send(b"")

  return out

