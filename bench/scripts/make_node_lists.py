import glob, re

ologs = glob.glob("./log/*out.log")
nodes = []

of = open('defnodes.txt','w')

for log in ologs:
  with open(log,'r') as l:
    info = l.readlines()

    node = re.findall('rcf\d\d\d',info[-1])[0]
    of.write(node+'\n')

of.close()

