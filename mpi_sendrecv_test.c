/*
 * Copyright (C) 2019, Northwestern University
 * See COPYRIGHT notice in top-level directory.
 *
 * This program evaluates the performance of all-to-all broadcast (Allgather and Allgatherv) algorithms proposed in our research paper.
 */

#include <mpi.h>
#include <unistd.h> /* getopt() */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#define DEBUG 0
#define ERR { \
    if (err != MPI_SUCCESS) { \
        int errorStringLen; \
        char errorString[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(err, errorString, &errorStringLen); \
        printf("Error at line %d: (%s)\n", __LINE__,errorString); \
    } \
}
#define MAP_DATA(a,b,c,d) ((a)*7+(b)*3+(c)*5+11*((a)-22)*((b)-56)+(d))

int pt2pt_statistics(int rank, int nprocs, int data_size, int ntimes, int runs){
    double total_start, total_timing, mean, var;
    int i, m, dst;
    char *send_buf= NULL;
    char *recv_buf = NULL;
    MPI_Status status;
    MPI_Request request;
    if (nprocs!=2) {
        return 1;
    }
    if (rank == 1) {
        send_buf = (char*) malloc(sizeof(char) * data_size);
    } else {
        recv_buf = (char*) malloc(sizeof(char) * data_size);
    }
    mean = 0;
    var = 0;
    MPI_Barrier(MPI_COMM_WORLD);
    total_timing = MPI_Wtime();
    for ( m = 0; m < ntimes; ++m ){
        total_start = MPI_Wtime();
        for ( i = 0; i < runs; ++i ) {
            if ( rank == 0 ) {
                dst = 1;
                MPI_Irecv(recv_buf, data_size, MPI_BYTE, dst, rank + dst, MPI_COMM_WORLD, &request);
            } else {
                dst = 0;
                MPI_Issend(send_buf, data_size, MPI_BYTE, dst, rank + dst, MPI_COMM_WORLD, &request);
            }
            MPI_Wait(&request,&status);
        }
        total_start = MPI_Wtime() - total_start;
        mean += total_start;
        var += total_start * total_start;
        MPI_Barrier(MPI_COMM_WORLD);
    }
    total_timing = MPI_Wtime() - total_timing;
    mean = mean / m;
    var = var - mean * mean;
    printf("rank %d, mean = %lf, var = %lf, ntimes = %d, total_timing = %lf, mean*ntimes = %lf\n", rank, mean, var, ntimes, total_timing, mean*m);
    if (rank == 1) {
        free(send_buf);
    } else {
        free(recv_buf);
    }

    return 0;

}

int main(int argc, char **argv){
    int rank, procs, i, ntimes = 0, data_size = 0, runs = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&procs);
    while ((i = getopt(argc, argv, "hk:d:i:")) != EOF){
        switch(i) {
            case 'd': 
                data_size = atoi(optarg);
                break;
            case 'k': 
                ntimes = atoi(optarg);
                break;
            case 'i': 
                runs = atoi(optarg);
                break;
            default:
                MPI_Finalize();
      	        return 0;
        }
    }

    pt2pt_statistics(rank, procs, data_size, ntimes, runs);
    MPI_Finalize();
    return 0;
}
