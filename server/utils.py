"""
Utility functions for the server side

@author: Joseph Jennings
@version: 2020.08.17
"""

def splitnum(num,div):
  """ Splits a number into nearly even parts """
  splits = []
  igr,rem = divmod(num,div)
  for i in range(div):
    splits.append(igr)
  for i in range(rem):
    splits[i] += 1

  return splits

