#!/bin/bash
#COBALT -A CSC250STDM12
#COBALT -n 4
#COBALT -t 10
#COBALT -q debug-flat-quad
#COBALT --attrs mcdram=cache:numa=quad

export MPICH_GNI_DYNAMIC_CONN=disabled

# specifiy the number of MPI process per compute node
nprocs_per_node=64

# calculate the number of logical cores to allocate per MPI process
cpus_per_task=$((64 / $nprocs_per_node))
cpus_per_task=$(($cpus_per_task * 4))


KNL_OPTS="-cc depth -d $cpus_per_task -j 4"

export JOBSIZE=4
export NP=$(($nprocs_per_node * $JOBSIZE))
RUN_CMMD=test
echo "--------------------------------------------------"
echo "---- Running on $NP MPI processes on ALCF Theta ----"
echo "---- SLURM_JOB_NUM_NODES = $JOBSIZE"
echo "---- Running $nprocs_per_node MPI processes per KNL node"
echo "---- command: srun -n $NP $KNL_OPTS"
echo "--------------------------------------------------"
echo ""
echo "---- KNL $RUN_CMMD ----"

export RUN_OPTS="-a 15 -d 100000 -c 999999 -m 0"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 15 -d 100000 -c 3 -m 0"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command
