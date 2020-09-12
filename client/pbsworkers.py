"""
Functions to launch PBS clients (workers)

@author: Joseph Jennings
@version: 2020.09.05
"""
import os, getpass, time
import random
import string, re
from genutils.ptyprint import create_inttag
import subprocess

def launch_pbsworkers(wrkfile,nworkers=1,ncore=16,mem=60,wtime=60,queue='sep',
                      logpath=".",name='worker-',pyexec=None,slpbtw=0.5,
                      chkrnng=True,verb=False):
  """
  Creates workers (specified by the wrkfile) on nworkers PBS nodes

  Parameters:
    wrkfile   - the .py file that describes the worker
    nworkers  - the number of PBS workers to create [1]
    ncore     - number of cores per worker [48]
    mem       - memory per worker [60 GB]
    wtime     - wall time for worker in minutes [30]
    queue     - the queue to which to submit the worker ['sep']
    logpath   - path to a directory for containing log files ['.']
    name      - worker name ['worker']
    pyexec    - path to the python executable to start the worker
               (default is /data/sep/joseph29/anaconda3/envs/py37/bin/python)
    slpbtw    - sleep in between job submissions [0.5]
    chkrnng   - do the best we can to ensure workers are running before finising [True]
    verb      - verbosity flag [False]
  """
  # Make sure not too many workers
  if(nworkers > len(rcfnodes) and ncore == 16):
    raise Exception("Too many workers requested. Max workers = %d for 16 cores/worker"%(len(rcfnodes)))

  rcfnodesr = rcfnodes*(16//ncore)

  if(pyexec is None):
    pyexec = '/data/sep/joseph29/opt/anaconda3/envs/py37/bin/python'

  cmd = '%s %s'%(pyexec,wrkfile)

  wrkrs = []
  for iwrk in range(nworkers):
    # Create the worker
    wrkrs.append(pbsworker(cmd,logpath=logpath,name=name,verb=verb))
    # Submit the worker
    wrkrs[iwrk].submit(ncore=ncore,mem=mem,wtime=wtime,queue=queue,host=rcfnodesr[iwrk],sleep=slpbtw)

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

def restart_pbsworkers(workers,chkrnng=True,killed=True):
  """
  Restarts a provided list of workers
  Useful if simulations run longer than maximum walltime

  Parameters:
    workers - a list of running workers
    chkrnng - checks whether the workers are running [True]

  Returns the restarted workers and their status
  """
  if(not killed):
    # Kill the workers if live
    kill_pbsworkers(workers,clean=False)

  nworkers = len(workers)
  for iwrk in range(nworkers):
    # Submit the worker with the same parameters as before
    workers[iwrk].submit(restart=True)

  # Get status of all workers
  wrkstatus = get_workers_status(workers)

  if(chkrnng):
    nchk = 20; ichk = 0
    while(wrkstatus.count('R') < nworkers and ichk <= nchk):
      # Wait for a second and get status again
      time.sleep(1)
      wrkstatus = get_workers_status(workers)
      # Keep track of checks
      ichk += 1

  return workers,wrkstatus

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

  # Get qstat file
  cmd = 'qstat -u %s -n -1 > qstat.out'%(workers[0].user)
  sp = subprocess.check_call(cmd,shell=True)
  with open('qstat.out','r') as f:
    info = f.readlines()
  if(len(info) == 1):
    raise Exception("Must start workers before checking their status")
  # Remove the header
  del info[0:5]

  status = []
  for wrkr in workers:
    # Get the status of each worker
    status.append(wrkr.get_status(info))

  return status

def kill_pbsworkers(workers=None,user=None,state=None,clean=True) -> None:
  """
  Deletes all workers in provided worker list

  Parameters:
    workers - a list of active workers
    user    - kill all workers associated with user
    state   - state of workers to kill ('R' or 'Q')
    clean   - clean up qstat, node and log files
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
    # Remove the qstat file if it exists
    if(os.path.exists('qstat.out')):
      sp = subprocess.check_call('rm -f qstat.out',shell=True)
  else:
    # Get name of user
    user = getpass.getuser()
    # Get worker info
    cmd = 'qstat -u %s -n -1 > qstat.out'%(user)
    sp = subprocess.check_call(cmd,shell=True)
    with open('qstat.out','r') as f:
      info = f.readlines()
    if(len(info) == 1):
      raise Exception("Must start workers before killing")
    del info[0:5]
    # Kill all workers
    for line in info:
      # Get the submission id
      subid = line.split()[0].split('.')[0]
      if(state is not None):
        # Get the state
        status = line.split()[9]
        if(status == state):
          # Cancel the worker
          cmd = 'qdel %s'%(subid)
          sp = subprocess.check_call(cmd,shell=True)
      else:
        # Cancel the worker
        cmd = 'qdel %s'%(subid)
        sp = subprocess.check_call(cmd,shell=True)

def block_pbs_nodes(nodelist,logpath,queue='default',blocktime=60,sleep=0.5,
                    verb=False) -> None:
  """
  Blocks faulty PBS nodes

  Parameters:
    nodelist  - a list of fault nodes (e.g., ['rcf133'])
    logpath   - path for logfiles
    queue     - queue for submission ['default']
    blocktime - amount of time in minutes to block node [60]
    sleep     - amount of time in seconds to wait between submissions [0.5]
    verb      - verbosity flag [False]
  """
  # Compute the job time
  wtimef = format_mins(blocktime)
  btime  = blocktime * 60
  # Submit blocker for each node in the list
  for node in nodelist:
    outfile = logpath + '/' + 'blocker-%s_out.log'%(node)
    errfile = logpath + '/' + 'blocker-%s_err.log'%(node)
    pbsout = """#! /bin/bash
#PBS -N blocker
#PBS -l nodes=%s:ppn=8
#PBS -l mem=60gb
#PBS -q %s
#PBS -l walltime=%s
#PBS -o %s
#PBS -e %s
cd $PBS_O_WORKDIR
#
sleep %f
#
# End of script"""%(node,queue,wtimef,outfile,errfile,btime)
    # Write the script to file
    script = "blocker-%s.sh"%(node)
    with open(script,'w') as f:
      f.write(pbsout)

    # Submit the script
    sub = "qsub %s > %s"%(script,node)
    if(verb): print(sub)
    sp = subprocess.check_call(sub,shell=True)
    # Get the submission id and remove the file
    with open(node,'r') as f:
      subid = f.readlines()[0].split()[-1]
    sp = subprocess.check_call('rm %s'%(node),shell=True)
    time.sleep(sleep)

def get_nodes_inuse():
  """ Returns a list of the nodes currently in use """
  cmd = 'qstat -n -1 > qstat.out'
  sp = subprocess.check_call(cmd,shell=True)
  with open('qstat.out','r') as f:
    info = f.readlines()
  # Remove the header
  del info[0:5]

  usednodes = []
  for line in info:
    split = line.split()
    if(split[-1] == '--' or split[9] == 'C'):
      continue
    else:
      usednodes += (list(set(re.findall('rcf\d\d\d',split[-1]))))

  return usednodes

class pbsworker:
  """
  A class for a worker on a PBS cluster
  """

  def __init__(self,cmd,name='worker-',logpath=".",verb=False):
    """
    pbsmworker constructor

    Parameters:
      cmd     - the command that defines the worker
      logpath - path to log file ["."]
      user    - user on cluster  ['joseph29']
      verb    - verbosity flag [False]

    Returns a pbs worker object
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
    # Submission parameters
    self.__ncore  = None; self.__mem  = None; self.__wtime = None
    self.__queue  = None; self.__host = None

  def submit(self,ncore=16,mem=60,wtime=60,queue='sep',host=None,sleep=0.5,restart=False) -> None:
    """
    Submit a PBS worker

    Parameters:
      ncore   - number of core for this worker [16]
      mem     - memory in GB for this worker [60]
      wtime   - wall time for job in minutes [60]
      queue   - worker to which to submit the worker ['sep']
      host    - submit to a specific host on rcf [None]
      sleep   - amount of time in seconds to wait between submissions [0.5]
      restart - flag indicating we are restarting the worker
    """
    # Build output log files
    self.outfile = self.logpath + '/' + self.name + self.workerid + '_out.log'
    self.errfile = self.logpath + '/' + self.name + self.workerid + '_err.log'

    if(restart):
      if(self.__ncore is not None): ncore = self.__ncore
      if(self.__mem   is not None): mem   = self.__mem
      if(self.__wtime is not None): wtime = self.__wtime
      if(self.__queue is not None): queue = self.__queue
      if(self.__host  is not None): host  = self.__host

    # Convert min time to string for script
    wtimef = format_mins(wtime)

    if(host is None):
      host = '1'

    # Create the PBS script
    pbsout = """#! /bin/bash
#PBS -N %s
#PBS -l nodes=%s:ppn=%d
#PBS -l mem=%dgb
#PBS -q %s
#PBS -l walltime=%s
#PBS -o %s
#PBS -e %s
cd $PBS_O_WORKDIR
#
%s
#
# End of script"""%(self.name+self.workerid,host,ncore,mem,queue,wtimef,
                    self.outfile,self.errfile,self.__cmd)
    # Write the script to file
    script = self.name + self.workerid + ".sh"
    with open(script,'w') as f:
      f.write(pbsout)

    # Submit the script
    sub = "qsub %s > %s"%(script,self.workerid)
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

    # Save the submission parameters
    self.__ncore = ncore
    self.__mem   = mem
    self.__wtime = wtime
    self.__queue = queue
    self.__host  = host

  def delete(self) -> None:
    """ Deletes the worker """
    if(self.subid is None):
      raise Exception("Cannot delete worker if has not been submitted")

    cmd = 'qdel %s'%(self.subid)
    sp = subprocess.check_call(cmd,shell=True)

  def get_status(self,qsstring):
    """
    Returns the worker's status

    Parameters:
      qsstring - a file generated from qstat command
    """
    for line in qsstring:
      if(self.workerid in line):
        self.status = line.split()[9]
    #TODO: need to be able to handle multiple status
    # For now, just returning the last status if there exist multiple
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

# Nodes available for computation
rcfnodes = ['rcf003','rcf005','rcf006','rcf008','rcf009','rcf013',
            'rcf014','rcf015','rcf017','rcf019','rcf022',
            'rcf025','rcf028','rcf030','rcf032','rcf041','rcf042',
            'rcf043','rcf044','rcf045','rcf048','rcf049','rcf050',
            'rcf053','rcf055','rcf058','rcf059','rcf060','rcf065',
            'rcf066','rcf068','rcf069','rcf070','rcf071',
            'rcf074','rcf076','rcf078','rcf080','rcf082',
            'rcf087','rcf092','rcf095','rcf102','rcf104','rcf105',
            'rcf113','rcf114','rcf125','rcf126','rcf127','rcf128',
            'rcf132','rcf134','rcf137','rcf141','rcf142','rcf143',
            'rcf145','rcf146']

