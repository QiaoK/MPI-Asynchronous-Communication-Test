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
#define PROC_NODE 64
#define ERR { \
    if (err != MPI_SUCCESS) { \
        int errorStringLen; \
        char errorString[MPI_MAX_ERROR_STRING]; \
        MPI_Error_string(err, errorString, &errorStringLen); \
        printf("Error at line %d: (%s)\n", __LINE__,errorString); \
    } \
}
#define MAP_DATA(a,b,c,d) ((a)*123+(b)*653+(c+a+b)*33+14*((a)-742)*((b)-15)+(d))
int err;
typedef struct{
    double post_request_time;
    double send_wait_all_time;
    double recv_wait_all_time;
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

int fill_buffer(int rank, char *buf, int size, int seed, int iter){
    MPI_Count i;
    for ( i = 0; i < size; ++i ){
        buf[i] = MAP_DATA(rank,i, seed, iter);
    }
    return 0;
}

int all_to_many_striped(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, x, temp;
    int myindex = 0;
    char **send_buf;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->recv_wait_all_time = 0;
    timer->send_wait_all_time = 0;
    timer->total_time = 0;

    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        recv_buf = (char**) malloc(sizeof(char*) * procs);
        recv_buf[0] = (char*) malloc(sizeof(char) * data_size * procs);
        for ( i = 1; i < procs; ++i ){
            recv_buf[i] = recv_buf[i-1] + data_size;
        }
        for ( i = 0; i < cb_nodes; ++i ){
            if (rank_list[i] == rank){
                myindex = i;
            }
        }
    } else{
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * cb_nodes);
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * cb_nodes);
    }

    send_buf = (char**) malloc(sizeof(char*) * cb_nodes);
    for ( i = 0; i < cb_nodes; ++i ){
        send_buf[i] = (char*) malloc(sizeof(char) * data_size);
        fill_buffer(rank, send_buf[i], data_size,i,iter);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    total_start = MPI_Wtime();

    //steps = (procs + comm_size - 1) / comm_size;

    if (comm_size > procs){
        // If the maximum communication size is greater than the number of processes, we just run many-to-all communication directly.
        start = MPI_Wtime();
        j = 0;
        if (isagg) {
            for ( i = 0; i < proc_node; ++i ){
                for ( x = i; x < procs; x+=proc_node ){
                    temp = (x + rank_list[myindex]) % procs;
                    MPI_Irecv(recv_buf[temp], data_size, MPI_BYTE, temp, rank + temp, MPI_COMM_WORLD, &requests[j++]);
                }
                
            }
        }
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Issend(send_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }
    }else {

    }
    timer->total_time += MPI_Wtime() - total_start;
    for (i = 0 ; i < cb_nodes; ++i){
        free(send_buf[i]);
    }
    free(send_buf);
    free(status);
    free(requests);
    if (isagg){
        free(recv_buf[0]);
        free(recv_buf);
    }
    return 0;
}

int all_to_many_balanced(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, k, x, temp;
    int myindex = 0;
    char **send_buf;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->recv_wait_all_time = 0;
    timer->send_wait_all_time = 0;
    timer->total_time = 0;

    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        recv_buf = (char**) malloc(sizeof(char*) * procs);
        recv_buf[0] = (char*) malloc(sizeof(char) * data_size * procs);
        for ( i = 1; i < procs; ++i ){
            recv_buf[i] = recv_buf[i-1] + data_size;
        }
        for ( i = 0; i < cb_nodes; ++i ){
            if (rank_list[i] == rank){
                myindex = i;
            }
        }
    } else{
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * cb_nodes);
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * cb_nodes);
    }

    send_buf = (char**) malloc(sizeof(char*) * cb_nodes);
    for ( i = 0; i < cb_nodes; ++i ){
        send_buf[i] = (char*) malloc(sizeof(char) * data_size);
        fill_buffer(rank, send_buf[i], data_size,i,iter);
    }

    if (comm_size > cb_nodes){
        comm_size = cb_nodes;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    total_start = MPI_Wtime();

    //steps = (procs + comm_size - 1) / comm_size;

    for ( k = 0; k < cb_nodes; k+=comm_size ){
        if ( cb_nodes - k < comm_size ){
            comm_size = cb_nodes - k;
        }
        j = 0;
        start = MPI_Wtime();
        if (isagg){
            for ( i = 0; i < comm_size; ++i ){
                for ( x = (myindex - k - i + cb_nodes) % cb_nodes; x < procs; x+=cb_nodes ){
                    MPI_Irecv(recv_buf[x], data_size, MPI_BYTE, x, rank + x, MPI_COMM_WORLD, &requests[j++]);
                }
            }
        }
        for ( i = 0; i < comm_size; ++i ){
            temp = (rank + k + i)%cb_nodes;
            MPI_Issend(send_buf[temp], data_size, MPI_BYTE, rank_list[temp], rank + rank_list[temp], MPI_COMM_WORLD, &requests[j++]);
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }
    }

    timer->total_time += MPI_Wtime() - total_start;
    for (i = 0 ; i < cb_nodes; ++i){
        free(send_buf[i]);
    }
    free(send_buf);
    free(status);
    free(requests);
    if (isagg){
        free(recv_buf[0]);
        free(recv_buf);
    }
    return 0;
}

int many_to_all_balanced_boundary(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, k, x, temp, myindex = 0;
    char **send_buf = NULL;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->send_wait_all_time = 0;
    timer->recv_wait_all_time = 0;
    timer->total_time = 0;
    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        send_buf = (char**) malloc(sizeof(char*) * procs);
        for ( i = 0; i < procs; ++i ){
            send_buf[i] = (char*) malloc(sizeof(char) * data_size);
            fill_buffer(rank, send_buf[i], data_size,i,iter);
        }
        for ( i = 0; i < cb_nodes; ++i ){
            if (rank_list[i] == rank){
                myindex = i;
            }
        }
    } else{
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * cb_nodes);
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * cb_nodes);
    }
    recv_buf = (char**) malloc(sizeof(char*) * cb_nodes);
    recv_buf[0] = (char*) malloc(sizeof(char) * data_size * cb_nodes);
    for ( i = 1; i < cb_nodes; ++i ){
        recv_buf[i] = recv_buf[i-1] + data_size;
    }

    if (comm_size > procs){
        comm_size = procs;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    total_start = MPI_Wtime();

    for ( k = 0; k < procs; k+=comm_size ){
        if ( procs - k < comm_size ){
            comm_size = procs - k;
        }
        j = 0;
        start = MPI_Wtime();
        for ( i = 0; i < cb_nodes; ++i ){
            for ( x = 0; x < comm_size; ++x ){
                if ( rank == (k + i * PROC_NODE + x) % procs ) {
                    MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
                }
            }
        }
        if (isagg){
            for ( i = 0; i < comm_size; ++i ){
                temp = (PROC_NODE * myindex + k + i) % procs;
                MPI_Issend(send_buf[temp], data_size, MPI_BYTE, temp, rank + temp, MPI_COMM_WORLD, &requests[j++]);
            }
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }

    }
    
    timer->total_time += MPI_Wtime() - total_start;
    free(recv_buf[0]);
    free(recv_buf);
    free(status);
    free(requests);
    if (isagg){
        for ( i = 0; i < procs; ++i ){
            free(send_buf[i]);
        }
        free(send_buf);
    }
    return 0;
}

int many_to_all_balanced(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, k, x, temp, myindex = 0;
    char **send_buf = NULL;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->send_wait_all_time = 0;
    timer->recv_wait_all_time = 0;
    timer->total_time = 0;
    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        send_buf = (char**) malloc(sizeof(char*) * procs);
        for ( i = 0; i < procs; ++i ){
            send_buf[i] = (char*) malloc(sizeof(char) * data_size);
            fill_buffer(rank, send_buf[i], data_size,i,iter);
        }
        for ( i = 0; i < cb_nodes; ++i ){
            if (rank_list[i] == rank){
                myindex = i;
            }
        }
    } else{
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * cb_nodes);
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * cb_nodes);
    }
    recv_buf = (char**) malloc(sizeof(char*) * cb_nodes);
    recv_buf[0] = (char*) malloc(sizeof(char) * data_size * cb_nodes);
    for ( i = 1; i < cb_nodes; ++i ){
        recv_buf[i] = recv_buf[i-1] + data_size;
    }

    if (comm_size > procs){
        comm_size = procs;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    total_start = MPI_Wtime();

    for ( k = 0; k < procs; k+=comm_size ){
        if ( procs - k < comm_size ){
            comm_size = procs - k;
        }
        j = 0;
        start = MPI_Wtime();
        for ( i = 0; i < cb_nodes; ++i ){
            for ( x = 0; x < comm_size; ++x ){
                if ( rank == (k + i + x) % procs ) {
                    MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
                }
            }
        }
        if (isagg){
            for ( i = 0; i < comm_size; ++i ){
                temp = (myindex + k + i) % procs;
                MPI_Issend(send_buf[temp], data_size, MPI_BYTE, temp, rank + temp, MPI_COMM_WORLD, &requests[j++]);
            }
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }

    }
    
    timer->total_time += MPI_Wtime() - total_start;
    free(recv_buf[0]);
    free(recv_buf);
    free(status);
    free(requests);
    if (isagg){
        for ( i = 0; i < procs; ++i ){
            free(send_buf[i]);
        }
        free(send_buf);
    }
    return 0;
}

int all_to_many_interleaved(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, k, x, steps, temp, myindex = 0;
    char **send_buf;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->recv_wait_all_time = 0;
    timer->send_wait_all_time = 0;
    timer->total_time = 0;

    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        recv_buf = (char**) malloc(sizeof(char*) * procs);
        recv_buf[0] = (char*) malloc(sizeof(char) * data_size * procs);
        for ( i = 1; i < procs; ++i ){
            recv_buf[i] = recv_buf[i-1] + data_size;
        }
        for ( i = 0; i < cb_nodes; ++i ){
            if (rank_list[i] == rank){
                myindex = i;
            }
        }
    } else{
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * cb_nodes);
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * cb_nodes);
    }

    send_buf = (char**) malloc(sizeof(char*) * cb_nodes);
    for ( i = 0; i < cb_nodes; ++i ){
        send_buf[i] = (char*) malloc(sizeof(char) * data_size);
        fill_buffer(rank, send_buf[i], data_size,i,iter);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    total_start = MPI_Wtime();
    if (comm_size > procs){
        // If the maximum communication size is greater than the number of processes, we just run many-to-all communication directly.
        start = MPI_Wtime();
        j = 0;
        if (isagg) {
            for ( i = 0; i < procs; ++i ){
                temp = ( i + rank_list[myindex] ) % procs;
                MPI_Irecv(recv_buf[temp], data_size, MPI_BYTE, temp, rank + temp, MPI_COMM_WORLD, &requests[j++]);
            }
        }
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Issend(send_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }
    }else {

    }
    timer->total_time += MPI_Wtime() - total_start;
    for (i = 0 ; i < cb_nodes; ++i){
        free(send_buf[i]);
    }
    free(send_buf);
    free(status);
    free(requests);
    if (isagg){
        free(recv_buf[0]);
        free(recv_buf);
    }
    return 0;
}

int all_to_many(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, k, x, steps;
    char **send_buf;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->recv_wait_all_time = 0;
    timer->send_wait_all_time = 0;
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

    send_buf = (char**) malloc(sizeof(char*) * cb_nodes);
    for ( i = 0; i < cb_nodes; ++i ){
        send_buf[i] = (char*) malloc(sizeof(char) * data_size);
        fill_buffer(rank, send_buf[i], data_size,i,iter);
    }

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
            MPI_Issend(send_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }
    }else {
        // Post Issend
        j=0;
        start = MPI_Wtime();
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Issend(send_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        timer->post_request_time += MPI_Wtime() - start;
        // We chop down the number of communications such that one waitall does not trigger more concurrent communication than comm_size.
        steps = (procs + comm_size - 1) / comm_size;
        for ( k = 0; k < steps; ++k ){
            x = 0;
            // Post Irecv
            if (isagg){
                start = MPI_Wtime();
                for ( i = k; i < procs; i+=steps ){
                    MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[cb_nodes+x]);
                    x++;
                }
                timer->post_request_time += MPI_Wtime() - start;
            }
            if (x) {
                start = MPI_Wtime();
                MPI_Waitall(x, requests + cb_nodes, status);
                timer->recv_wait_all_time += MPI_Wtime() - start;
            }
        }
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->send_wait_all_time += MPI_Wtime() - start;
        }
    }
    timer->total_time += MPI_Wtime() - total_start;
    for (i = 0 ; i < cb_nodes; ++i){
        free(send_buf[i]);
    }
    free(send_buf);
    free(status);
    free(requests);
    if (isagg){
        free(recv_buf[0]);
        free(recv_buf);
    }
    return 0;
}

int many_to_all_interleaved(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, k, x, steps;
    char **send_buf = NULL;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->send_wait_all_time = 0;
    timer->recv_wait_all_time = 0;
    timer->total_time = 0;
    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        send_buf = (char**) malloc(sizeof(char*) * procs);
        for ( i = 0; i < procs; ++i ){
            send_buf[i] = (char*) malloc(sizeof(char) * data_size);
            fill_buffer(rank, send_buf[i], data_size,i,iter);
        }
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
        j = 0;
        // If the maximum communication size is greater than the number of processes, we just run many-to-all communication directly.
        start = MPI_Wtime();
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        if (isagg){
            for ( i = 0; i < procs; ++i ){
                MPI_Issend(send_buf[i], data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[j++]);
            }
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }
    } else{
    }
    timer->total_time += MPI_Wtime() - total_start;
    free(recv_buf[0]);
    free(recv_buf);
    free(status);
    free(requests);
    if (isagg){
        for ( i = 0; i < procs; ++i ){
            free(send_buf[i]);
        }
        free(send_buf);
    }
    return 0;
}

int many_to_all(int rank, int isagg, int procs, int cb_nodes, int proc_node, int data_size, int *rank_list, int comm_size, Timer *timer, int iter){
    double start, total_start;
    int i, j, k, x, steps;
    char **send_buf = NULL;
    char **recv_buf = NULL;
    MPI_Status *status;
    MPI_Request *requests;
    timer->post_request_time = 0;
    timer->send_wait_all_time = 0;
    timer->recv_wait_all_time = 0;
    timer->total_time = 0;
    if (isagg){
        requests = (MPI_Request*) malloc(sizeof(MPI_Request) * (cb_nodes + procs));
        status = (MPI_Status*) malloc(sizeof(MPI_Status) * (cb_nodes + procs));
        send_buf = (char**) malloc(sizeof(char*) * procs);
        for ( i = 0; i < procs; ++i ){
            send_buf[i] = (char*) malloc(sizeof(char) * data_size);
            fill_buffer(rank, send_buf[i], data_size,i,iter);
        }
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
        j = 0;
        // If the maximum communication size is greater than the number of processes, we just run many-to-all communication directly.
        start = MPI_Wtime();
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j++]);
        }
        if (isagg){
            for ( i = 0; i < procs; ++i ){
                MPI_Issend(send_buf[i], data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[j++]);
            }
        }
        timer->post_request_time += MPI_Wtime() - start;
        if (j) {
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }
    } else{
        j = 0;
        // Post Irecv first
        start = MPI_Wtime();
        for ( i = 0; i < cb_nodes; ++i ){
            MPI_Irecv(recv_buf[i], data_size, MPI_BYTE, rank_list[i], rank + rank_list[i], MPI_COMM_WORLD, &requests[j]);
            j++;
        }
        timer->post_request_time += MPI_Wtime() - start;
        // We chop down the number of communications such that one waitall does not trigger more concurrent communication than comm_size.
        steps = (procs + comm_size - 1) / comm_size;
        for ( k = 0; k < steps; ++k ){
            // Then Issend
            x = 0;
            if (isagg){
                start = MPI_Wtime();
                for ( i = k; i < procs; i+=steps ){
                    //MPI_Issend(send_buf[i], data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[j + x]);
                    MPI_Issend(send_buf[i], data_size, MPI_BYTE, i, rank + i, MPI_COMM_WORLD, &requests[cb_nodes + x]);
                    x++;
                }
                timer->post_request_time += MPI_Wtime() - start;
            }
            // Waitall for Issend
            if (x){
                start = MPI_Wtime();
                MPI_Waitall(x, requests + cb_nodes, status);
                timer->send_wait_all_time += MPI_Wtime() - start;
            }
        }
        // Waitall for Irecv
        if (j){
            start = MPI_Wtime();
            MPI_Waitall(j, requests, status);
            timer->recv_wait_all_time += MPI_Wtime() - start;
        }
    }
    timer->total_time += MPI_Wtime() - total_start;
    free(recv_buf[0]);
    free(recv_buf);
    free(status);
    free(requests);
    if (isagg){
        for ( i = 0; i < procs; ++i ){
            free(send_buf[i]);
        }
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

int summarize_results(int procs, int cb_nodes, int data_size, int comm_size, char* filename, char* prefix, Timer timer1,Timer max_timer1){
    FILE* stream;
    printf("| --------------------------------------\n");
    printf("| %s rank 0 request post time = %lf\n", prefix, timer1.post_request_time);
    printf("| %s rank 0 send waitall time = %lf\n", prefix, timer1.send_wait_all_time);
    printf("| %s rank 0 recv waitall time = %lf\n", prefix, timer1.recv_wait_all_time);
    printf("| %s rank 0 total time = %lf\n", prefix, timer1.total_time);
    printf("| %s max request post time = %lf\n", prefix, max_timer1.post_request_time);
    printf("| %s max send waitall time = %lf\n", prefix, max_timer1.send_wait_all_time);
    printf("| %s max recv waitall time = %lf\n", prefix, max_timer1.recv_wait_all_time);
    printf("| %s max total time = %lf\n", prefix, max_timer1.total_time);
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
        fprintf(stream,"rank 0 send waitall time,");
        fprintf(stream,"rank 0 recv waitall time,");
        fprintf(stream,"rank 0 total time,");
        fprintf(stream,"max post_request_time,");
        fprintf(stream,"max send waitall time,");
        fprintf(stream,"max recv waitall time,");
        fprintf(stream,"max total time\n");
    }
    fprintf(stream,"%d,",procs);
    fprintf(stream,"%d,",cb_nodes);
    fprintf(stream,"%d,",data_size);
    fprintf(stream,"%d,",comm_size);
    fprintf(stream,"%lf,",timer1.post_request_time);
    fprintf(stream,"%lf,",timer1.send_wait_all_time);
    fprintf(stream,"%lf,",timer1.recv_wait_all_time);
    fprintf(stream,"%lf,",timer1.total_time);
    fprintf(stream,"%lf,",max_timer1.post_request_time);
    fprintf(stream,"%lf,",max_timer1.send_wait_all_time);
    fprintf(stream,"%lf,",max_timer1.recv_wait_all_time);
    fprintf(stream,"%lf\n",max_timer1.total_time);
    fclose(stream);
    return 0;
}

int main(int argc, char **argv){
    int rank, procs, cb_nodes = 1, method = 0, data_size = 0, proc_node = 1, isagg, i, comm_size = 200000000, iter = 1;
    int *rank_list;
    Timer timer1,max_timer1;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);
    MPI_Comm_size(MPI_COMM_WORLD,&procs);
    while ((i = getopt(argc, argv, "hp:c:m:d:a:i:")) != EOF){
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
            case 'p': 
                proc_node = atoi(optarg);
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
            all_to_many(rank, isagg, procs, cb_nodes, proc_node, data_size, rank_list, comm_size, &timer1, i);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                summarize_results(procs, cb_nodes, data_size, comm_size, "all_to_many_results.csv", "All to many", timer1, max_timer1);
            }
        }
        if (method == 0 || method == 2){
            many_to_all(rank, isagg, procs, cb_nodes, proc_node, data_size, rank_list, comm_size, &timer1, i);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                summarize_results(procs, cb_nodes, data_size, comm_size, "many_to_all_results.csv", "Many to all", timer1, max_timer1);
            }
        }
        if (method == 0 || method == 3){
            all_to_many_balanced(rank, isagg, procs, cb_nodes, proc_node, data_size, rank_list, comm_size, &timer1, i);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                summarize_results(procs, cb_nodes, data_size, comm_size, "all_to_many_balanced_results.csv", "All to many balanced", timer1, max_timer1);
            }
        }
        if (method == 0 || method == 4){
            many_to_all_balanced(rank, isagg, procs, cb_nodes, proc_node, data_size, rank_list, comm_size, &timer1, i);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                summarize_results(procs, cb_nodes, data_size, comm_size, "many_to_all_balanced_results.csv", "Many to all balanced", timer1, max_timer1);
            }
        }

        if (method == 0 || method == 5){
            many_to_all_balanced_boundary(rank, isagg, procs, cb_nodes, proc_node, data_size, rank_list, comm_size, &timer1, i);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                summarize_results(procs, cb_nodes, data_size, comm_size, "many_to_all_balanced_boundary_results.csv", "Many to all balanced boundary", timer1, max_timer1);
            }
        }

        if (method == 0 || method == 6){
            all_to_many_interleaved(rank, isagg, procs, cb_nodes, proc_node, data_size, rank_list, comm_size, &timer1, i);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                summarize_results(procs, cb_nodes, data_size, comm_size, "all_to_many_interleaved_results.csv", "All to many interleaved", timer1, max_timer1);
            }
        }

        if (method == 0 || method == 7){
            all_to_many_striped(rank, isagg, procs, cb_nodes, proc_node, data_size, rank_list, comm_size, &timer1, i);
            MPI_Reduce((double*)(&timer1), (double*)(&max_timer1), 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0){
                summarize_results(procs, cb_nodes, data_size, comm_size, "all_to_many_striped_results.csv", "All to many stiped", timer1, max_timer1);
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
