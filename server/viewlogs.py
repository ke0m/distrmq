"""
Provides simultaneous views of worker logs

@author: Joseph Jennings
@version: 2020.08.21
"""
import sys, os, argparse, configparser
import signal
import random
import glob, re
import time
import subprocess

def signal_handler(sig,frame):
  sp = subprocess.check_call('clear',shell=True)
  sys.exit(0)

# Parse the config file
conf_parser = argparse.ArgumentParser(add_help=False)
conf_parser.add_argument("-c", "--conf_file",
                         help="Specify config file", metavar="FILE")
args, remaining_argv = conf_parser.parse_known_args()
defaults = {
    "delay": 30,
    "maxworker": 40,
    "height": 30,
    }
if args.conf_file:
  config = configparser.ConfigParser()
  config.read([args.conf_file])
  defaults = dict(config.items("defaults"))

# Parse the other arguments
# Don't surpress add_help here so it will handle -h
parser = argparse.ArgumentParser(parents=[conf_parser],description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)

# Set defaults
parser.set_defaults(**defaults)

# Input files
ioArgs = parser.add_argument_group('Required parameters')
ioArgs.add_argument("-logsdir",help="Path to log directory",required=True,type=str)
ioArgs.add_argument("-pattern",help="Pattern for reading in the logs",type=str)
# Other Parameters
othArgs = parser.add_argument_group('Other parameters')
othArgs.add_argument("-delay",help="Delay in between reads of logs (seconds) [30]",type=float)
othArgs.add_argument("-maxworker",help="Maximum number of workers running [40]",type=int)
othArgs.add_argument("-height",help="Maximum height of terminal (in workers) for display [30]",type=int)
# Enables required arguments in config file
for action in parser._actions:
  if(action.dest in defaults):
    action.required = False
args = parser.parse_args(remaining_argv)

# Get input arguments
logdir  = args.logsdir
pattern = args.pattern
delay   = args.delay
maxwrkr = args.maxworker

# Constant for screen height
height = args.height
if(maxwrkr > height):
  idx = sorted(random.sample(range(maxwrkr),height))

# Moves the cursor on the terminal
cursor_up = lambda lines: '\x1b[{0}A'.format(lines)
cursor_down = lambda lines: '\x1b[{0}B'.format(lines)

# Clear the screen to avoid messy printing
sp = subprocess.check_call('clear',shell=True)

# Handle a signal interruption from ctrl-c
signal.signal(signal.SIGINT,signal_handler)

# Read in all logs with a specific prefix
beg = time.time(); first = True
while True:
  logs = sorted(glob.glob(logdir + '/*'))
  disp = []
  if(first):
    print("\n\n")
  print("Time elapsed: %.2fs"%(time.time()-beg))
  for ilog in logs:
    if(re.search('%s'%(pattern),ilog)):
      # Read in the file
      with open(ilog,'r') as f:
        lines = f.readlines()
        if(len(lines) == 0):
          continue
        else:
          lastline = lines[-1]
      disp.append(lastline)

  # Print the output progress
  if(maxwrkr > height):
    disp = [disp[iline] if(iline) < len(disp) else "" for iline in idx]
  print("".join(disp))
  print(cursor_up(maxwrkr + 10))
  print(cursor_down(0))
  first = False
  time.sleep(delay)

