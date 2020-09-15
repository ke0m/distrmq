"""
Functions to launch SLURM clients (workers)

@author: Joseph Jennings
@version: 2020.09.11
"""
import os, getpass, time
import random
import string
from genutils.ptyprint import create_inttag
import subprocess

def launch_slurmworkers(wrkfile,nworkers=1,ncore=48,mem=60,wtime=30,queue='sep',
                        block=[],logpath=".",name='worker-',pyexec=None,slpbtw=0.5,
                        chkrnng=False,mode='quiet',verb=False):
  """
  Creates workers (specified by the wrkfile) on nworkers SLURM nodes

  Parameters:
    wrkfile   - the .py file that describes the worker
    nworkers  - the number of SLURM workers to create [1]
    ncore     - number of cores per worker [48]
    mem       - memory per worker [60 GB]
    wtime     - wall time for worker in minutes [30]
    queue     - a partition or queue for job submission ['sep']
    block     - nodes to which we want to avoid submitting []
    logpath   - path to a directory for containing log files ['.']
    name      - worker name ['worker']
    pyexec    - path to the python executable to start the worker
               (default is /home/joseph29/opt/miniconda3/envs/py37/bin/python)
    slpbtw    - sleep in between job submissions [0.5]
    chkrnng   - do the best we can to ensure workers are running before finising [False]
    mode      - current mode of cluster ['quiet'], 'busy' or 'adapt'
    verb      - verbosity flag [False]
  """
  if(pyexec is None):
    pyexec = '/home/joseph29/opt/miniconda3/envs/py37/bin/python'

  cmd = '%s %s'%(pyexec,wrkfile)

  if(mode == 'busy'):
    wrkrs,wrkstatus = launch_slurmworkers_busy(wrkfile,nworkers,ncore,mem,wtime,queue,
                                               block,logpath,name,pyexec,slpbtw,verb)

  elif(mode == 'adapt'):
    if(type(queue) == list):
      swrkrs,sstatus = launch_slurmworkers_adapt(wrkfile,nworkers,ncore,mem,wtime,queue[0],
                                                 block,logpath,name,pyexec,slpbtw,verb)
      lworkers = nworkers - sstatus.count('R')
      # If any queued, submit to twohour
      if(lworkers > 0):
        twrkrs,twrkstatus = launch_slurmworkers_adapt(wrkfile,lworkers,ncore,mem,wtime,queue[1],
                                                      block,logpath,name,pyexec,slpbtw,verb)
        cwrkrs = swrkrs + twrkrs
        wrkrs,wrkstatus = trim_tsworkers(cwrkrs,nworkers)
      else:
        wrkrs     = swrkrs
        wrkstatus = sstatus
    else:
      wrkrs,wrkstatus = launch_slurmworkers_adapt(wrkfile,nworkers,ncore,mem,wtime,queue,
                                                  block,logpath,name,pyexec,slpbtw,verb)

  elif(mode == 'quiet'):
    wrkrs = []
    for iwrk in range(nworkers):
      # Create the worker
      wrkrs.append(slurmworker(cmd,logpath=logpath,name=name,verb=verb))
      # Submit the worker
      wrkrs[iwrk].submit(ncore=ncore,mem=mem,wtime=wtime,queue=queue,block=block,sleep=slpbtw)

    # Get status of all workers
    wrkstatus = get_workers_status(wrkrs)

    if(chkrnng):
      # First get status of workers
      nchk = 20; ichk = 0
      while(wrkstatus.count('R') < nworkers and ichk <= nchk):
        # Wait for a second and get status again
        time.sleep(1)
        wrkstatus = get_workers_status(wrkrs)
        # Keep track of checks
        ichk += 1

  return wrkrs,wrkstatus

def launch_slurmworkers_busy(wrkfile,nworkers=1,ncore=48,mem=60,wtime=30,queue='sep',
                             block=[],logpath=".",name='worker-',pyexec=None,slpbtw=2.0,
                             verb=False):
  """
  Creates workers (specified by the wrkfile) on nworkers SLURM nodes.
  Best to use when the cluster is busy

  Parameters:
    wrkfile   - the .py file that describes the worker
    nworkers  - the number of SLURM workers to create [1]
    ncore     - number of cores per worker [48]
    mem       - memory per worker [60 GB]
    wtime     - wall time for worker in minutes [30]
    queue     - a list of queue names to which jobs can be submitted [ ['sep','default'] ]
    block     - nodes to which we want to avoid submitting []
    logpath   - path to a directory for containing log files ['.']
    name      - worker name ['worker']
    pyexec    - path to the python executable to start the worker
               (default is /home/joseph29/opt/miniconda3/envs/py37/bin/python)
    slpbtw    - sleep in between job submissions [0.5]
    verb      - verbosity flag [False]
  """
  if(pyexec is None):
    pyexec = '/home/joseph29/opt/miniconda3/envs/py37/bin/python'

  cmd = '%s %s'%(pyexec,wrkfile)

  wrkrs = []
  while(len(wrkrs) < nworkers):
    # Get number of workers to launch
    nlnch = nworkers - len(wrkrs)
    # Launch the workers
    twrkrs = []
    for iwrk in range(nlnch):
      # Create the worker
      twrkrs.append(slurmworker(cmd,logpath=logpath,name=name,verb=verb))
      # Submit the worker
      twrkrs[iwrk].submit(ncore=ncore,mem=mem,wtime=wtime,queue=queue,block=block,sleep=slpbtw)

    # Update status of all workers
    twrkstatus = get_workers_status(twrkrs)

    # If a worker has completed, delete it
    for iwrkr in range(len(twrkrs)):
      if(twrkstatus[iwrkr] == 'R' and twrkrs[iwrkr].workerid not in get_ids(wrkrs)):
        wrkrs.append(twrkrs[iwrkr])

    print("Obtained %d workers"%(len(wrkrs)),end='\r')

  return wrkrs,['R']*nworkers

def launch_slurmworkers_adapt(wrkfile,nworkers=1,ncore=48,mem=60,wtime=30,queue='sep',
                              block=[],logpath=".",name='worker-',pyexec=None,slpbtw=2.0,
                              verb=False):
  """
  An adaptive mode for launching SLURM workers
  Attempts to launch to number of workers requested
  Will stop launching if two or more workers are queued ('PD').
  Marks remaining as 'TS'

  Parameters:
    wrkfile   - the .py file that describes the worker
    nworkers  - the number of SLURM workers to create [1]
    ncore     - number of cores per worker [48]
    mem       - memory per worker [60 GB]
    wtime     - wall time for worker in minutes [30]
    queue     - a list of queue names to which jobs can be submitted 'sep'
    block     - nodes to which we want to avoid submitting []
    logpath   - path to a directory for containing log files ['.']
    name      - worker name ['worker']
    pyexec    - path to the python executable to start the worker
               (default is /home/joseph29/opt/miniconda3/envs/py37/bin/python)
    slpbtw    - sleep in between job submissions [0.5]
    verb      - verbosity flag [False]
  """
  if(pyexec is None):
    pyexec = '/home/joseph29/opt/miniconda3/envs/py37/bin/python'

  cmd = '%s %s'%(pyexec,wrkfile)

  wrkrs = []; status = [];
  for iwrk in range(nworkers):
    # Create the worker
    wrkrs.append(slurmworker(cmd,logpath=logpath,name=name,verb=verb))
    if(status.count('PD') >= 2):
      # If two in queue, set status to "To submit"
      wrkrs[iwrk].set_sub_pars(ncore=ncore,mem=mem,wtime=wtime,queue=queue,block=block)
      status.append('TS')
      wrkrs[iwrk].status = status[iwrk]
    else:
      # Otherwise, submit the worker and keep track of status
      wrkrs[iwrk].submit(ncore=ncore,mem=mem,wtime=wtime,queue=queue,block=block,sleep=slpbtw)
      # Get the worker status
      status.append(wrkrs[iwrk].get_status(get_squeue()))

  # Do another check if workers are pending and submit if not
  status = launch_tsworkers(wrkrs)

  return wrkrs,status

def trim_tsworkers(wrkrs,ndes):
  """
  Trims unnecessary TS workers in the list of workers

  Parameters:
    wrkrs - a list of workers of mixed status
    ndes  - number of desired running workers

  Returns a list of trimed workers and their status
  """
  nwrkr = len(wrkrs)
  status = get_workers_status(wrkrs)
  rwrkrs = []; twrkrs = []

  # First split the R and TS workers
  for iwrk in range(nwrkr):
    if(status[iwrk] == 'R' or status[iwrk] == 'PD'):
      rwrkrs.append(wrkrs[iwrk])
    elif(status[iwrk] == 'TS'):
      twrkrs.append(wrkrs[iwrk])

  while(len(rwrkrs) < ndes):
    rwrkrs.append(twrkrs[iwrk])

  return rwrkrs, get_workers_status(rwrkrs)

def launch_tsworkers(wrkrs,slpbtw=5.0):
  """
  Submits workers that are in a 'TS' state

  Parameters:
    workers - a list of workers

  Returns an updates status of the workers
  """
  # First get the status of the workers
  status = get_workers_status(wrkrs)
  if(status.count('PD') >= 2): return status
  else:
    for iwrk in range(len(wrkrs)):
      if(status[iwrk] == 'TS' and status.count('PD') < 2):
        wrkrs[iwrk].submit(sleep=slpbtw)
        status[iwrk] = wrkrs[iwrk].get_status(get_squeue())

  # Update the status of the workers
  return get_workers_status(wrkrs)

def restart_slurmworkers(wrkrs,limit=True,perc=0.75,slpbtw=3.0):
  """
  Restart SLURM workers

  Parameters:
    workers - a list of SLURM workers
    limit   - restart based on the time limit [True]
              (if False, restarts all)
    perc    - percentage of wall time at which to restart
  """
  # First update the status and the times
  status = get_workers_status(wrkrs)

  numpd = 0
  for iwrk in range(len(wrkrs)):
    if(status[iwrk] == 'R'):
      if(limit):
        # Get the times
        rtime = wrkrs[iwrk].get_rtime()
        wtime = wrkrs[iwrk].get_wtime()
        if(rtime/wtime >= perc):
          # Kill the worker
          wrkrs[iwrk].delete()
          if(numpd < 2):
            # Start it again
            wrkrs[iwrk].submit(restart=True,sleep=slpbtw)
            status[iwrk] = wrkrs[iwrk].get_status(get_squeue())
            if(status[iwrk] == 'PD'):
              numpd += 1
          else:
            status[iwrk] == 'TS'
      else:
        #TODO: check the queue of the worker
        #      if that queue has more than 2 workers in PD
        #      don't restart, but set as 'TS'
        # Kill the worker
        wrkrs[iwrk].delete()
        if(status.count('PD') < 4):
          # Start it again
          wrkrs[iwrk].submit(restart=True,sleep=slpbtw)
          #status[iwrk] = wrkrs[iwrk].get_status(get_squeue())
        else:
          status[iwrk] == 'TS'
      #TODO: set the queue option here
      status = get_workers_status(wrkrs)

  # Return the status of the workers
  return get_workers_status(wrkrs)

def get_workers_status(workers,qsplit=False):
  """
  Gets the status of a list of SLURM workers

  Parameters:
    workers - a list of SLURM workers
    qsplit  - splits the status between the queues

  Returns a list of workers status.
  If qsplit is True, returns a dictionary of lists where
  the keys are the names of the queues ('sep' and 'twohour')
  """
  # Check length of workers
  if(len(workers) == 0):
    raise Exception("Length of workers must be > 0")

  # Get squeue file
  info = get_squeue()

  status = [ workers[iwrk].get_status(info) for iwrk in range(len(workers)) ]

  if(qsplit):
    for iwrkr in range(len(workers)):
      sstatus = []; tstatus = []
      if(workers[iwrkr].get_queue() == 'sep'):
        sstatus.append(status[iwrkr])
      elif(workers[iwrkr].get_queue() == 'twohour'):
        tstatus.append(status[iwrkr])
    return {"sep:",sstatus,"twohour:",tstatus}
  else:
    return status

def get_workers_times(workers):
  """
  Gets the run time of a list of workers

  Parameters:
    workers - a list of SLURM workers

  Returns a list of worker run times
  """
  # Check length of workers
  if(len(workers) == 0):
    raise Exception("Length of workers must be > 0")

  # Get squeue file
  info = get_squeue()

  return [ workers[iwrk].get_status(info,time=True)[1] for iwrk in range(len(workers)) ]

def get_ids(workers):
  """
  Returns a list of worker ids

  Parameters:
    workers - a list of workers
  """
  return [wrkr.workerid for wrkr in workers]

def kill_slurmworkers(workers=None,user=None,state=None,clean=True) -> None:
  """
  Deletes all workers in provided worker list

  Parameters:
    workers - a list of active workers
    user    - kill all workers associated with user
    state   - state of workers to kill ('R' or 'PD')
    clean   - clean up squeue, node and log files
  """
  if(workers is not None):
    if(len(workers) == 0):
      raise Exception("Length of workers must be > 0")

    for wrkr in workers:
      # Get the job id and remove the associated files
      if(wrkr.status == 'TS'): continue
      if(clean):
        wid = wrkr.workerid
        sp = subprocess.check_call('rm -f *%s* %s/*%s*'%(wid,wrkr.logpath,wid),shell=True)
      # Remove the worker from the queue
      wrkr.delete()
    # Remove the squeue file if it exists
    if(os.path.exists('squeue.out')):
      sp = subprocess.check_call('rm -f squeue.out',shell=True)
  else:
    # Get worker info
    info = get_squeue()
    # Kill all workers
    for line in info:
      # Get the submission id
      subid = line.split()[0]
      if(state is not None):
        # Get the state
        status = line.split()[4]
        if(status == state):
          # Cancel the worker
          cmd = 'scancel %s'%(subid)
          sp = subprocess.check_call(cmd,shell=True)
      else:
        # Cancel the worker
        cmd = 'scancel %s'%(subid)
        sp = subprocess.check_call(cmd,shell=True)

class slurmworker:
  """
  A class for a worker on a SLURM cluster
  """

  def __init__(self,cmd,name='worker',logpath=".",verb=False):
    """
    slurmworker constructor

    Parameters:
      cmd     - the command that defines the worker
      logpath - path to log file ["."]
      user    - user on cluster  ['joseph29']
      verb    - verbosity flag [False]

    Returns a slurm worker object
    """
    self.workerid = self.id_generator()
    self.__cmd    = cmd
    self.subid    = None
    self.status   = None
    self.nodename = None
    self.logpath  = logpath
    self.verb     = verb
    self.nsub     = 0
    self.outfile  = None
    self.errfile  = None
    self.name     = name
    self.user     = getpass.getuser()
    self.__rtime  = 0
    # Submission parameters
    self.__ncore  = None; self.__mem   = None; self.__wtime = None
    self.__queue  = None; self.__block = None

  def set_sub_pars(self,ncore=48,mem=60,wtime=30,queue='sep',block=[]) -> None:
    """ Sets the submission parameters (so job can be submitted later) """
    self.__ncore = ncore; self.__mem = mem; self.__wtime = wtime
    self.__queue = queue; self.__block = block

  def submit(self,ncore=48,mem=60,wtime=30,queue='sep',block=[],sleep=0.5,restart=False) -> None:
    """
    Submit a slurm worker

    Parameters:
      ncore   - number of core for this worker [48]
      mem     - memory in GB for this worker [60]
      wtime   - wall time for job in minutes [30]
      queue   - worker to which to submit the worker ['sep']
      block   - a list of nodes to which we don't want to submit []
      sleep   - amount of time in seconds to wait between submissions [0.5]
      restart - flag inidicating we are restarting the worker
    """
    # Build output log files
    self.outfile = self.logpath + '/' + self.name + self.workerid + '_out.log'
    self.errfile = self.logpath + '/' + self.name + self.workerid + '_err.log'

    if(self.status == 'TS'):
      if(self.__ncore is not None): ncore = self.__ncore
      if(self.__mem   is not None): mem   = self.__mem
      if(self.__wtime is not None): wtime = self.__wtime
      if(self.__queue is not None): queue = self.__queue
      if(self.__block is not None): block = self.__block

    if(restart):
      if(self.__ncore is not None): ncore = self.__ncore
      if(self.__mem   is not None): mem   = self.__mem
      if(self.__wtime is not None): wtime = self.__wtime
      if(self.__queue is not None): queue = self.__queue
      if(self.__block is not None): block = self.__block

    # Convert min time to string for script
    wtimef = format_mins(wtime)

    # Create the SLURM script
    slurmout = """#! /bin/bash
#SBATCH --job-name %s
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=%d
#SBATCH --mem=%dgb
#SBATCH --partition=%s
#SBATCH --time=%s
#SBATCH --exclude=%s
#SBATCH --output=%s
#SBATCH --error=%s
cd $SLURM_SUBMIT_DIR
#
echo $SLURMD_NODENAME > %s-node.txt
%s
#
# End of script"""%(self.name+self.workerid,ncore,mem,queue,wtimef,",".join(block),
                    self.outfile,self.errfile,self.workerid,self.__cmd)
    # Write the script to file
    script = self.name + self.workerid + ".sh"
    with open(script,'w') as f:
      f.write(slurmout)

    # Submit the script
    sp = subprocess.Popen(['sbatch',script],stdout=subprocess.PIPE)
    out,err = sp.communicate()
    self.subid = out.decode("utf-8").split()[-1]
    time.sleep(sleep)

    # Keep track of submissions
    self.nsub += 1

    # Save the submission parameters
    self.__ncore = ncore
    self.__meme  = mem
    self.__wtime = wtime
    self.__queue = queue
    self.__block = block

  def delete(self) -> None:
    """ Deletes the worker """
    if(self.subid is None):
      raise Exception("Cannot delete worker if has not been submitted")

    cmd = 'scancel %s'%(self.subid)
    sp = subprocess.check_call(cmd,shell=True)

  def get_status(self,sqstring,time=False):
    """
    Returns the worker's status

    Parameters:
      sqfile - a file generated from squeue command
      time   - return the current run time as well [False]
    """
    for line in sqstring:
      if(self.workerid in line):
        split = line.split()
        # Save the status and the time
        self.status  = split[4]
        self.__rtime = unformat_mins(split[5])
        if(time): return self.status, self.__rtime
        else: return self.status
    # Could not find worker, assume it is complete
    if(self.status == 'R'):
      self.status = 'CG'
    elif(self.status == 'TS'):
      self.status = 'TS'
    elif(self.status is None):
      print("Seems we are encounting a faulty node... Setting as 'TS' for now")
      self.status = 'TS'
      #raise Exception("Something is wrong. Worker not found nor waiting for submission")

    return self.status

  def get_rtime(self):
    """ Gets the current job runtime """
    return self.__rtime

  def get_wtime(self):
    """ Gets the job wall time """
    return self.__wtime

  def get_queue(self):
    """ Gets the queue to which the job has been submitted """
    return self.__queue

  def id_generator(self,size=6, chars=string.ascii_uppercase + string.digits):
    """ Creates a random string with uppercase letters and integers """
    return ''.join(random.choice(chars) for _ in range(size))

def get_squeue():
  """ Gets the output of squeue """
  sp = subprocess.Popen(['squeue','-u',getpass.getuser(),'-o','%.18i %.9P %.17j %.10u %.2t %.10M %.6D %R'],stdout=subprocess.PIPE)
  out,err = sp.communicate()
  info = out.decode("utf-8").split("\n")
  if(len(info) == 1):
    raise Exception("Must start workers before checking their status")
  # Remove the header and footer
  del info[0]; del info[-1]

  return info

def format_mins(mins):
  """ Formats the minutes to pass to a SLURM script """
  hors,mins= divmod(mins,60)
  imins = int(mins)
  secs = (mins - imins)*60
  horsf = create_inttag(int(hors),10)
  minsf = create_inttag(imins,10)
  secsf = create_inttag(secs,10)

  return "%s:%s:%s"%(horsf,minsf,secsf)

def unformat_mins(mins):
  """
  Takes the formatted mins and returns a
  floating point representation of minuutes
  """
  times = mins.split(":")
  if(len(times) == 1):
    [secs] = times
    hors = mins = 0
  if(len(times) == 2):
    [mins,secs] = times
    hors = 0
  elif(len(times) == 3):
    [hors,mins,secs] = times

  return float(hors)*60 + float(mins) + float(secs)/60

