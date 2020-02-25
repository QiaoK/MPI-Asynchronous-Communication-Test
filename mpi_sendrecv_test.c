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
    double total_start, total_timing, mean, var, std;
    int i, m, dst;
    char *send_buf= NULL;
    char *recv_buf = NULL;
    double *time_list;
    FILE* stream;
    MPI_Status status;
    MPI_Request request;
    char *filename;
    if (nprocs!=2) {
        return 1;
    }
    time_list = (double*) malloc(sizeof(double) * ntimes);
    if (rank == 1) {
        send_buf = (char*) malloc(sizeof(char) * data_size);
    } else {
        recv_buf = (char*) malloc(sizeof(char) * data_size);
    }
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
        time_list[m] = MPI_Wtime() - total_start;
        MPI_Barrier(MPI_COMM_WORLD);
    }
    total_timing = MPI_Wtime() - total_timing;

    filename = "sendrecv_results.csv";
    stream = fopen(filename,"r");
    if (stream){
        fclose(stream);
        stream = fopen(filename,"a");
    } else {
        stream = fopen(filename,"w");
    }
    mean = 0;
    var = 0;
    for ( m = 0; m < ntimes; ++m ) {
        fprintf(stream,"%lf\n",time_list[m]);
        mean += time_list[m];
    }
    mean = mean/m;
    double x,y;
    for ( m = 0; m < ntimes; ++m ) {
        var += (time_list[m]-mean)*(time_list[m]-mean);
        x+=time_list[m]*time_list[m]
    }
    printf("rank %d, %lf\n",rank,x/m-mean*mean);
    var = var/m;
    std = sqrt(var);
    printf("rank %d, mean = %lf, std = %lf, ntimes = %d, total_timing = %lf, mean*ntimes = %lf\n", rank, mean, std, ntimes, total_timing, mean*m);
    if (rank == 1) {
        free(send_buf);
    } else {
        free(recv_buf);
    }
    fclose(stream);
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
