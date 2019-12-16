#!/bin/bash
#COBALT -A CSC250STDM12
#COBALT -n 256
#COBALT -t 30
#COBALT -q default
#COBALT --attrs mcdram=cache:numa=quad

export MPICH_GNI_DYNAMIC_CONN=disabled

# specifiy the number of MPI process per compute node
nprocs_per_node=64

# calculate the number of logical cores to allocate per MPI process
cpus_per_task=$((64 / $nprocs_per_node))
cpus_per_task=$(($cpus_per_task * 4))


KNL_OPTS="-cc depth -d $cpus_per_task -j 4"


export JOBSIZE=256
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

export RUN_OPTS="-a 256 -d 2048 -c 1 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 2 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 4 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 8 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 16 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 32 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 64 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 128 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 256 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 512 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 1024 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 2048 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 4096 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 8192 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command

export RUN_OPTS="-a 256 -d 2048 -c 999999999 -m 1 -i 5"
export command="aprun -n $NP -N $nprocs_per_node $KNL_OPTS ./$RUN_CMMD $RUN_OPTS"
echo "command=$command"
$command
