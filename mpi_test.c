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
#define MAP_DATA(a,b) ((a)*123+(b)*653+14*((a)-742)*((b)-15))
int err;
typedef struct{
    double post_request_time;
    double wait_all_time;
    double total_time;
}Timer;

static void
usage(char *argv0)
{
    char *help =
    "Usage: %s [OPTION]... [FILE]...\n"
    "       [-h] Print help\n"
    "       [-a] number of aggregators (in the context of ROMIO)\n"
    "       [-d] data size\n"
    "       [-c] maximum communication size\n"
    "       [-i] number of iteration\n"
    "       [-m] method\n"
    "           0: Both 1 and 2\n"
    "           1: All processes to c receivers\n"
    "           2: c processes to all processes\n";
    fprintf(stderr, help, argv0);
}

int fill_buffer(int rank, char *buf, int size){
    MPI_Count i;
    for ( i = 0; i < size; ++i ){
        buf[i] = MAP_DATA(rank,i);
    }
    return 0;
}

int all_to_many(int rank, int isagg, int procs, int cb_nodes, int data_size, int *rank_list, int comm_size, Timer *timer){
    double start, total_start;
    int i, j, k, steps;
    char *send_buf = (char*) malloc(sizeof(char)*data_size);
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->wait_all_time = 0;
    timer->total_time = 0;

    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        recv_buf = (char**) malloc(sizeof(char*) * procs);
        recv_buf[0] = (char*) malloc(sizeof(char) * data_size * procs);
        for ( i = 1; i < procs; ++i ){
            recv_buf[i] = recv_buf[i-1] + data_size;
        }
    } else{
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * cb_nodes);
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * cb_nodes);
    }

    fill_buffer(rank, send_buf, data_size);

    MPI_Barrier(MPI_COMM_WORLD);
    total_start = MPI_Wtime();
    if (comm_size > procs){
        // If the maximum communication size is greater than the number of processes, we just run many-to-all communication directly.
        start = MPI_Wtime();
        j = 0;
        if (isagg) {
            for ( i = 0; i < procs; ++i ){
                MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[j++]);
            }
        }
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Issend(send_buf, data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->wait_all_time += MPI_Wtime() - start;
        }
    }else {
        // We chop down the number of communications such that one waitall does not trigger more concurrent communication than comm_size.
        steps = (procs + comm_size - 1) / comm_size;
        for ( k = 0; k < steps; ++k ){
            start = MPI_Wtime();
            j = 0;
            // Post Irecv first
            if (isagg){
                for ( i = k; i < procs; i+=steps ){
                    MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[j++]);
                }
            }
            // Then Issend
            if ( rank % steps == k ){
                for ( i = 0; i < cb_nodes; ++i ){
                    MPI_Issend(send_buf, data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
                }
            }
            timer->post_request_time += MPI_Wtime() - start;
            if (j) {
                start = MPI_Wtime();
                MPI_Waitall(j, requests, status);
                timer->wait_all_time += MPI_Wtime() - start;
            }
        }
    }
    timer->total_time += MPI_Wtime() - total_start;
    free(send_buf);
    free(status);
    free(requests);
    if (isagg){
        free(recv_buf[0]);
        free(recv_buf);
    }
    return 0;
}

int many_to_all(int rank, int isagg, int procs, int cb_nodes, int data_size, int *rank_list, int comm_size, Timer *timer){
    double start, total_start;
    int i, j, k, x, steps;
    char *send_buf = (char*) malloc(sizeof(char)*data_size);
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->wait_all_time = 0;
    timer->total_time = 0;
    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        fill_buffer(rank, send_buf, data_size);
    } else{
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * cb_nodes);
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * cb_nodes);
    }
    recv_buf = (char**) malloc(sizeof(char*) * procs);
    recv_buf[0] = (char*) malloc(sizeof(char) * data_size * procs);
    for ( i = 1; i < cb_nodes; ++i ){
        recv_buf[i] = recv_buf[i-1] + data_size;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    total_start = MPI_Wtime();
    if ( comm_size > procs ){
        // If the maximum communication size is greater than the number of processes, we just run many-to-all communication directly.
        start = MPI_Wtime();
        j = 0;
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        if (isagg){
            for ( i = 0; i < procs; ++i ){
                MPI_Issend(send_buf, data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[j++]);
            }
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->wait_all_time += MPI_Wtime() - start;
        }
    } else{
        // We chop down the number of communications such that one waitall does not trigger more concurrent communication than comm_size.
        steps = (procs + comm_size - 1) / comm_size;
        for ( k = 0; k < steps; ++k ){
            // Post Irecv first
            start = MPI_Wtime();
            j = 0;
            if ( rank % steps == k ){
                for ( i = 0; i < cb_nodes; ++i ){
                    MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j]);
                    j++;
                }
            }
            // Then Issend
            x = 0;
            if (isagg){
                for ( i = k; i < procs; i+=steps ){
                    MPI_Issend(send_buf, data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[cb_nodes + x]);
                    x++;
                }
            }
            timer->post_request_time += MPI_Wtime() - start;
            // Waitall for Irecv
            start = MPI_Wtime();
            if (j){
                MPI_Waitall(j, requests, status);
            }
            // Waitall for Issend
            if (x){
                MPI_Waitall(x, requests + cb_nodes, status);
            }
            timer->wait_all_time += MPI_Wtime() - start;
        }
    }
    timer->total_time += MPI_Wtime() - total_start;
    free(recv_buf[0]);
    free(recv_buf);
    free(status);
    free(requests);
    if (isagg){
        free(send_buf);
    }
    return 0;
}

int create_aggregator_list(int rank, int procs,int cb_nodes,int **rank_list, int *is_agg){
    int *rank_list_ptr = (int*) malloc(sizeof(int)*cb_nodes);
    int i, remainder, ceiling, floor;
    remainder = procs / cb_nodes;
    ceiling = (procs + cb_nodes - 1) / cb_nodes;
    floor = procs / cb_nodes;
    for ( i = 0; i < cb_nodes; ++i ){
        if ( i < remainder ){
            rank_list_ptr[i] = ceiling * i;
        } else {
            rank_list_ptr[i] = ceiling * remainder + floor * (i - remainder);
        }
        if (rank_list_ptr[i] == rank){
            *is_agg = 1;
        }
    }
    *rank_list = rank_list_ptr;
    return 0;
}

int main(int argc, char **argv){
    int rank, procs, cb_nodes = 0, method = 0, data_size = 0, isagg, i, comm_size = 200000000, iter = 1;
    int *rank_list;
    Timer timer1,max_timer1,timer2,max_timer2;
    char filename[1024];
    FILE* stream;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&procs);
    while ((i = getopt(argc, argv, "hc:m:d:a:i:")) != EOF){
        switch(i) {
            case 'm': 
                method = atoi(optarg);
                break;
            case 'a': 
                cb_nodes = atoi(optarg);
                break;
            case 'd': 
                data_size = atoi(optarg);
                break;
            case 'c': 
                comm_size = atoi(optarg);
                break;
            case 'i': 
                iter = atoi(optarg);
                break;
            default:
                if (rank==0) usage(argv[0]);
                MPI_Finalize();
      	        return 0;
        }
    }
    isagg = 0;
    create_aggregator_list(rank, procs, cb_nodes, &rank_list, &isagg);
    if (rank == 0){
        printf("total number of processes = %d, cb_nodes = %d, data size = %d, comm_size = %d\n", procs, cb_nodes, data_size, comm_size);
        printf("aggregators = ");
        for ( i = 0; i < cb_nodes; ++i ){
            printf("%d, ",rank_list[i]);
        }
        printf("\n");
    }
    for ( i = 0; i < iter; ++i ){
        if (method == 0 || method == 1){
            all_to_many(rank, isagg, procs, cb_nodes, data_size, rank_list, comm_size, &timer1);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 3, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                printf("| --------------------------------------\n");
                printf("| All-to-many rank 0 request post time = %lf\n",timer1.post_request_time);
                printf("| All-to-many rank 0 waitall time = %lf\n",timer1.wait_all_time);
                printf("| All-to-many rank 0 total time = %lf\n",timer1.total_time);
                printf("| All-to-many max request post time = %lf\n",max_timer1.post_request_time);
                printf("| All-to-many max waitall time = %lf\n",max_timer1.wait_all_time);
                printf("| All-to-many max total time = %lf\n",max_timer1.total_time);
                sprintf(filename,"all_to_many_results.csv");
                stream = fopen(filename,"r");
                if (stream){
                    fclose(stream);
                    stream = fopen(filename,"a");
                } else {
                    stream = fopen(filename,"w");
                    fprintf(stream,"# of processes,");
                    fprintf(stream,"# of aggregators,");
                    fprintf(stream,"data size,");
                    fprintf(stream,"max comm,");
                    fprintf(stream,"rank 0 post_request_time,");
                    fprintf(stream,"rank 0 waitall time,");
                    fprintf(stream,"rank 0 total time,");
                    fprintf(stream,"max post_request_time,");
                    fprintf(stream,"max waitall time,");
                    fprintf(stream,"max total time\n");
                }
                fprintf(stream,"%d,",procs);
                fprintf(stream,"%d,",cb_nodes);
                fprintf(stream,"%d,",data_size);
                fprintf(stream,"%d,",comm_size);
                fprintf(stream,"%lf,",timer1.post_request_time);
                fprintf(stream,"%lf,",timer1.wait_all_time);
                fprintf(stream,"%lf,",timer1.total_time);
                fprintf(stream,"%lf,",max_timer1.post_request_time);
                fprintf(stream,"%lf,",max_timer1.wait_all_time);
                fprintf(stream,"%lf\n",max_timer1.total_time);
                fclose(stream);
            }
        }
        if (method == 0 || method == 2){
            many_to_all(rank, isagg, procs, cb_nodes, data_size, rank_list, comm_size, &timer2);
            MPI_Reduce((double*)(&timer2), (double*)(&max_timer2), 3, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                printf("| --------------------------------------\n");
                printf("| Many-to-all rank 0 request post time = %lf\n",timer2.post_request_time);
                printf("| Many-to-all rank 0 waitall time = %lf\n",timer2.wait_all_time);
                printf("| Many-to-all rank 0 total time = %lf\n",timer2.total_time);
	        printf("| Many-to-all max request post time = %lf\n",max_timer2.post_request_time);
	        printf("| Many-to-all max waitall time = %lf\n",max_timer2.wait_all_time);
	        printf("| Many-to-all max total time = %lf\n",max_timer2.total_time);
                sprintf(filename,"many_to_all_results.csv");
                stream = fopen(filename,"r");
                if (stream){
                    fclose(stream);
                    stream = fopen(filename,"a");
                } else {
                    stream = fopen(filename,"w");
                    fprintf(stream,"# of processes,");
                    fprintf(stream,"# of aggregators,");
                    fprintf(stream,"data size,");
                    fprintf(stream,"max comm,");
                    fprintf(stream,"rank 0 post_request_time,");
                    fprintf(stream,"rank 0 waitall time,");
                    fprintf(stream,"rank 0 total time,");
                    fprintf(stream,"max post_request_time,");
                    fprintf(stream,"max waitall time,");
                    fprintf(stream,"max total time\n");
                }
                fprintf(stream,"%d,",procs);
                fprintf(stream,"%d,",cb_nodes);
                fprintf(stream,"%d,",data_size);
                fprintf(stream,"%d,",comm_size);
                fprintf(stream,"%lf,",timer2.post_request_time);
                fprintf(stream,"%lf,",timer2.wait_all_time);
                fprintf(stream,"%lf,",timer2.total_time);
                fprintf(stream,"%lf,",max_timer2.post_request_time);
                fprintf(stream,"%lf,",max_timer2.wait_all_time);
                fprintf(stream,"%lf\n",max_timer2.total_time);
                fclose(stream);
            }
        }
        if (rank == 0){
            printf("| --------------------------------------\n");
        }
    }
    free(rank_list);
    MPI_Finalize();
    return 0;
}
