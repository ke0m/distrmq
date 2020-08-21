"""
Functions to launch SLURM clients (workers)

@author: Joseph Jennings
@version: 2020.08.20
"""
import os, getpass, time
import random
import string
from genutils.ptyprint import create_inttag
import subprocess

def launch_slurmworkers(wrkfile,nworkers=1,ncore=48,mem=60,wtime=30,queues=['sep','twohour'],
                        logpath=".",name='worker-',pyexec=None,slpbtw=0.5,
                        chkrnng=False,qtransfer=True,verb=False):
  """
  Creates workers (specified by the wrkfile) on nworkers SLURM nodes

  Parameters:
    wrkfile   - the .py file that describes the worker
    nworkers  - the number of SLURM workers to create [1]
    ncore     - number of cores per worker [48]
    mem       - memory per worker [60 GB]
    wtime     - wall time for worker in minutes [30]
    queues    - a list of queue names to which jobs can be submitted [ ['sep','default'] ]
    logpath   - path to a directory for containing log files ['.']
    name      - worker name ['worker']
    pyexec    - path to the python executable to start the worker
               (default is /home/joseph29/opt/miniconda3/envs/py37/bin/python)
    slpbtw    - sleep in between job submissions [0.5]
    chkrnng   - do the best we can to ensure workers are running before finising [False]
    qtransfer - transfer a worker to another queue if it is queued [True]
    verb      - verbosity flag [False]
  """
  if(pyexec is None):
    pyexec = '/home/joseph29/opt/miniconda3/envs/py37/bin/python'

  cmd = '%s %s'%(pyexec,wrkfile)

  wrkrs = []
  for iwrk in range(nworkers):
    # Create the worker
    wrkrs.append(slurmworker(cmd,logpath=logpath,name=name,verb=verb))
    # Submit the worker
    wrkrs[iwrk].submit(ncore=ncore,mem=mem,wtime=wtime,queue=queues[0],sleep=slpbtw)

  # Get status of all workers
  wrkstatus = get_workers_status(wrkrs)

  #TODO: might consider switching queues or removing if stuck pending
  if(chkrnng):
    # First get status of workers
    nchk = 20; ichk = 0
    while(wrkstatus.count('R') < nworkers and ichk <= nchk):
      # Wait for a second and get status again
      time.sleep(1)
      wrkstatus = get_workers_status(wrkrs)
      # Keep track of checks
      ichk += 1

  if(len(queues) > 1):
    if(qtransfer):
      for ist in wrkstatus:
        if(ist == 'PD'):
          # Delete the worker
          wrkrs[ist].delete()
          # Resubmit to the other queue
          wrkrs[ist].submit(ncore=ncore,mem=mem,wtime=wtime,queue=queues[1],sleep=slptbw)
      # Get status again
      wrkstatus = get_workers_status(wrkrs)

  return wrkrs,wrkstatus

def get_workers_status(workers):
  """
  Gets the status of a list of workers

  Parameters:
    workers - a list of workers

  Returns a list of workers status
  """
  # Check length of workers
  if(len(workers) == 0):
    raise Exception("Length of workers must be > 0")

  # Get squeue file
  cmd = 'squeue -u' + ' "%s" '%(workers[0].user) + '-o "%.18i %.9P %.17j %.10u %.2t %.10M %.6D %R" > squeue.out'
  sp = subprocess.check_call(cmd,shell=True)
  with open('squeue.out','r') as f:
    info = f.readlines()
  if(len(info) == 1):
    raise Exception("Must start workers before checking their status")
  # Remove the header
  del info[0]

  status = []
  for wrkr in workers:
    # Get the status of each worker
    status.append(wrkr.get_status(info))

  return status

def kill_slurmworkers(workers=None,user=None,state=None,clean=True):
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
      if(clean):
        wid = wrkr.workerid
        sp = subprocess.check_call('rm -f *%s* %s/*%s*'%(wid,wrkr.logpath,wid),shell=True)
      # Remove the worker from the queue
      wrkr.delete()
    # Remove the squeue file if it exists
    if(os.path.exists('squeue.out')):
      sp = subprocess.check_call('rm -f squeue.out',shell=True)
  else:
    # Get name of user
    user = getpass.getuser()
    # Get worker info
    cmd = 'squeue -u' + ' "%s" '%(user) + '-o "%.18i %.9P %.17j %.10u %.2t %.10M %.6D %R" > squeue.out'
    sp = subprocess.check_call(cmd,shell=True)
    with open('squeue.out','r') as f:
      info = f.readlines()
    if(len(info) == 1):
      raise Exception("Must start workers before killing")
    del info[0]
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
    self.queue    = None
    self.name     = name
    self.user     = getpass.getuser()

  def submit(self,ncore=48,mem=60,wtime=30,queue='sep',sleep=0.5):
    """
    Submit a slurm worker

    Parameters:
      ncore - number of core for this worker [48]
      mem   - memory in GB for this worker [60]
      wtime - wall time for job in minutes [30]
      queue - worker to which to submit the worker ['sep']
      sleep - amount of time in seconds to wait between submissions [0.5]
    """
    # Convert min time to string for script
    wtimef = format_mins(wtime)

    # Build output log files
    self.outfile = self.logpath + '/' + self.name + self.workerid + '_out.log'
    self.errfile = self.logpath + '/' + self.name + self.workerid + '_err.log'

    # Create the SLURM script
    slurmout = """#! /bin/bash
#SBATCH --job-name %s
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=%d
#SBATCH --mem=%dgb
#SBATCH --partition=%s
#SBATCH --time=%s
#SBATCH --output=%s
#SBATCH --error=%s
cd $SLURM_SUBMIT_DIR
#
echo $SLURMD_NODENAME > %s-node.txt
%s
#
# End of script"""%(self.name+self.workerid,ncore,mem,queue,wtimef,
                    self.outfile,self.errfile,self.workerid,self.__cmd)
    # Write the script to file
    script = self.name + self.workerid + ".sh"
    with open(script,'w') as f:
      f.write(slurmout)

    # Submit the script
    sub = "sbatch %s > %s"%(script,self.workerid)
    if(self.verb): print(sub)
    sp = subprocess.check_call(sub,shell=True)
    # Get the submission id and remove the file
    with open(self.workerid,'r') as f:
      self.subid = f.readlines()[0].split()[-1]
    sp = subprocess.check_call('rm %s'%(self.workerid),shell=True)
    time.sleep(sleep)

    # Keep track of submissions
    self.queue = queue
    self.nsub += 1

  def delete(self):
    """ Deletes the worker """
    if(self.subid is None):
      raise Exception("Cannot delete worker if has not been submitted")

    cmd = 'scancel %s'%(self.subid)
    sp = subprocess.check_call(cmd,shell=True)

  def get_status(self,sqstring):
    """
    Returns the worker's status

    Parameters:
      sqfile - a file generated from squeue command
    """
    for line in sqstring:
      if(self.workerid in line):
        self.status = line.split()[4]
        return self.status

  def id_generator(self,size=6, chars=string.ascii_uppercase + string.digits):
    """ Creates a random string with uppercase letters and integers """
    return ''.join(random.choice(chars) for _ in range(size))

def format_mins(mins):
  """ Formats the minutes to pass to a SLURM script """
  hors,mins= divmod(mins,60)
  imins = int(mins)
  secs = (mins - imins)*60
  horsf = create_inttag(int(hors),10)
  minsf = create_inttag(imins,10)
  secsf = create_inttag(secs,10)

  return "%s:%s:%s"%(horsf,minsf,secsf)

