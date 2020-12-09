#! /bin/bash
#PBS -N worker
#PBS -l nodes=1:ppn=16
#PBS -l mem=60gb
#PBS -q default
#PBS -l walltime=01:00:00
#PBS -o ./log/worker.out
#PBS -e ./log/worker.err
cd $PBS_O_WORKDIR
#
/data/sep/joseph29/opt/anaconda3/envs/py35/bin/python ./scripts/ompclient.py
#
# End of script

