#! /bin/bash
#PBS -N worker
#PBS -l nodes=1:ppn=16
#PBS -q default
#PBS -o ./log/worker.out
#PBS -e ./log/worker.err
cd $PBS_O_WORKDIR
#
sleep 10
echo "Hello" >> worker.txt
#
# End of script

