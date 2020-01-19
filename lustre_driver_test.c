#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <unistd.h> /* getopt() */

#define ADIOI_Calloc calloc
#define ADIOI_Malloc malloc
#define ADIOI_Free free
#define OST_STRIPE_SAME 0
#define OST_STRIPE_GREATER 1
#define OST_STRIPE_LESS 2
#define OST_STRIPE_ALL 3
#define DEBUG 0
/*
    1. a: sender rank
    2. b: receiver_rank
    3. c: byte shift for this sender at this receiver
*/
#define MAP_DATA(a,b,c) (1+(a)*3+(b)*5+(c)*7)


/*----< usage() >------------------------------------------------------------*/
/*
static void
usage()
{
    char *help =
    "Usage: %s [OPTION]... [FILE]...\n"
    "       [-h] Print help\n"
    "       [-p] number of MPI processes per node\n"
    "       [-b] message block unit size\n"
    "       [-n] number of iterations\n"
    "       [-t] test type (0-2)\n";
    printf("%s",help);
}
*/

int test_correctness(int rank, int nprocs, int* recv_size, char **recv_buf){
    int i,j;
    int result = 1;
    for ( i = 0; i < nprocs; i++ ){
        for ( j = 0; j < recv_size[i]; j++ ){
            if ((int)recv_buf[i][j]!=(int)((char)MAP_DATA(i,rank,j))){
                printf("unexpected result at aggregator %d from %d, %d!=%d\n", rank, i, (int)recv_buf[i][j], (int)((char)MAP_DATA(i,rank,j)));
                result = 0;
            }
        }
    }
    return result;
}

static int myCompareString (const void * a, const void * b) 
{ 
    return strcmp (*(char **) a, *(char **) b); 
} 
  
void stringSort(char *arr[], int n) 
{ 
    qsort (arr, n, sizeof (char *), myCompareString); 
} 
/*
  This function output meta information for local aggregator ranks with respect to process assignments at different nodes. The size of local aggregators respects to the co parameter. However, if co is unreasonably large (greater than local process size, it is forced to be local process size.)
  Input:
     1. rank: rank of this process.
     2. process_node_list: a mapping from a process to the node index it belongs to. Node index is determined by the order of its lowest local rank.
     3. nprocs: total number of process.
     4. nrecvs: total number of nodes.
     5. global_aggregator_size: size of global_aggregators.
     6. global_aggregators: an array of all aggregators.
     7. co: number of local aggregators per node.
     8 mode: 0: local aggregaotrs are evenly spreadout on a node, 1: local aggregators try to occupy the superset of global aggregators
  Output:
     1. is_aggregator_new: if this process is an aggregator in new context.
     2. local_aggregator_size: length of local_aggregators
     3. local_aggregators: an array of new global aggregators (must be a superset of global aggregators)
     4. nprocs_aggregator: length of aggregator_local_ranks
     5. aggregator_local_ranks: an array that tells what ranks this aggregator is responsible for performing proxy communication. (only significant if this process is an aggregator)
     6. aggregator_process_list: mapping from a process rank to the aggregator responsible for sending its data to other aggregators.
*/
int aggregator_meta_information(int rank, int *process_node_list, int nprocs, int nrecvs, int global_aggregator_size, int *global_aggregators, int co, int* is_aggregator_new, int* local_aggregator_size, int **local_aggregators, int* nprocs_aggregator, int **aggregator_local_ranks, int **process_aggregator_list, int mode){
    int i, j, k, local_node_aggregator_size, local_node_process_size, *local_node_aggregators, *temp_local_ranks, *check_local_aggregators, *check_global_aggregators, temp1, temp2, temp3, base, remainder, co2, test, *ptr;
    process_aggregator_list[0] = (int*) ADIOI_Malloc(sizeof(int)*nprocs);
    local_node_aggregators = (int*) ADIOI_Malloc(sizeof(int)*nprocs);
    temp_local_ranks = (int*) ADIOI_Malloc(sizeof(int)*nprocs);
    check_global_aggregators = (int*) ADIOI_Calloc(nprocs,sizeof(int));
    check_local_aggregators = (int*) ADIOI_Calloc(nprocs,sizeof(int));
    nprocs_aggregator[0] = 0;
    local_aggregator_size[0] = 0;
    is_aggregator_new[0] = 0;
    /* Check if a process is an aggregator*/
    for ( i = 0; i < nprocs; ++i ){
        for ( j = 0; j < global_aggregator_size; ++j ){
            if ( i == global_aggregators[j] ){
                check_global_aggregators[i] = 1;
                break;
            }
        }
    }
    for ( i = 0; i < nrecvs; ++i ){
        local_node_process_size = 0;
        /* Get process on the ith node and identify aggregators*/
        for ( j = 0; j < nprocs; ++j ){
            if ( process_node_list[j] == i ) {
                local_node_process_size++;
            }
        }
        if ( co > local_node_process_size ){
            local_aggregator_size[0] += local_node_process_size;
        }else{
            local_aggregator_size[0] += co;
        }
    }
    local_aggregators[0] = (int*) ADIOI_Malloc(local_aggregator_size[0]*sizeof(int));
    ptr = local_aggregators[0];
    /* For every node*/
    for ( i = 0; i < nrecvs; ++i ){
        local_node_process_size = 0;
        local_node_aggregator_size = 0;
        /* Get process on the ith node and identify aggregators*/
        for ( j = 0; j < nprocs; j++ ){
            if ( process_node_list[j] == i ) {
                temp_local_ranks[local_node_process_size] = j;
                local_node_process_size++;
                if ( check_global_aggregators[j] ) {
                    local_node_aggregators[local_node_aggregator_size] = j;
                    local_node_aggregator_size++;
                }
            }
        }
        /* Work out maximum number of intranode aggregator per node*/
        if( co > local_node_process_size){
            co2 = local_node_process_size;
        } else{
            co2 = co;
        }
        if (mode){
            if ( co2 > local_node_aggregator_size ){
                temp2 = local_node_aggregator_size;
                // Go through all local processes. fill the local aggregator array up to the number of element co2
                for ( j = 0; j < local_node_process_size; j++ ){
                    // Make sure that the added ranks do not repeat with the exiting aggregator ranks
                    test = 1;
                    for ( k = 0; k < temp2; k++ ){
                        if( temp_local_ranks[j] == local_node_aggregators[k]){
                            test = 0;
                            break;
                        }
                    }
                    if (test){
                        local_node_aggregators[local_node_aggregator_size] = temp_local_ranks[j];
                        local_node_aggregator_size++;
                    }
                    if (local_node_aggregator_size == co2){
                        break;
                    }
                }
            } else{
                local_node_aggregator_size = co2;
            }
        } else{
            local_node_aggregator_size = co2;
            remainder = local_node_process_size % local_node_aggregator_size;
            temp1 = (local_node_process_size + local_node_aggregator_size - 1) / local_node_aggregator_size;
            temp2 = local_node_process_size / local_node_aggregator_size;
            for ( j = 0; j < local_node_aggregator_size; ++j ){
                if( j < remainder ){
                    local_node_aggregators[j] = temp_local_ranks[temp1 * j];
                } else{
                    local_node_aggregators[j] = temp_local_ranks[temp1 * remainder + temp2 * ( j - remainder )];
                }
            }
        }

        memcpy(ptr, local_node_aggregators, sizeof(int) * local_node_aggregator_size);
        ptr += local_node_aggregator_size;
        for ( j = 0; j < local_node_aggregator_size; ++j ){
            check_local_aggregators[local_node_aggregators[j]] = 1;
            if (rank == local_node_aggregators[j]){
                is_aggregator_new[0] = 1;
            }
        }
        if ( local_node_aggregator_size ){
            /* We bind non-aggregators to local aggregators with the following rules. 
               1. Every local aggregator is responsible for either temp1 or temp2 number of non-aggregators.
               2. A local aggregator is responsible for itself.
            */
            remainder = local_node_process_size % local_node_aggregator_size;
            temp1 = (local_node_process_size + local_node_aggregator_size - 1) / local_node_aggregator_size;
            temp2 = local_node_process_size / local_node_aggregator_size;
            // Search index
            base = 0;
            for ( j = 0; j < local_node_aggregator_size; ++j ){
                test = 1;
                // Determine comm group size.
                if ( j < remainder ){
                    temp3 = temp1;
                } else{
                    temp3 = temp2;
                }
                /*
                   The algorithm is as the following.
                   Scan the local process list, if local process is a local aggregator and it is not currently iterated (local_node_aggregators[j]), simply jump. This local aggregator is handled in another j loop (by searching or passing)
                   If it is current local aggregator, just add to the list. For non-aggregators, just keep adding them into the list until the entire group is filled.
                   When the group is one element away from being filled and the current local aggregator has not been added. We directly add it.
                */
                for ( k = 0; k < temp3; ++k ){
                    if ( k == temp3 -1 && test ){
                        process_aggregator_list[0][ local_node_aggregators[j] ] = local_node_aggregators[j];
                        break;
                    }
                    while ( (check_local_aggregators[temp_local_ranks[base]] && temp_local_ranks[base] != local_node_aggregators[j]) ){
                        base++;
                    }
                    if (check_local_aggregators[temp_local_ranks[base]]){
                        test = 0;
                    }
                    process_aggregator_list[0][ temp_local_ranks[base] ] = local_node_aggregators[j];
                    base++;
                }
            }
        }
    }
    /* if this process is an aggregator (in context of enlarged aggregator list), work out the process ranks that it is responsible for.*/
    if (is_aggregator_new[0]){
        for ( i = 0; i < nprocs; ++i ){
            if (process_aggregator_list[0][i] == rank ){
                nprocs_aggregator[0]++;
            }
        }
        aggregator_local_ranks[0] = (int*) ADIOI_Malloc(nprocs_aggregator[0]*sizeof(int));
        j = 0;
        for ( i = 0; i < nprocs; ++i ){
            if (process_aggregator_list[0][i] == rank ){
                aggregator_local_ranks[0][j] = i;
                j++;
            }
        }
    }
    ADIOI_Free(local_node_aggregators);
    ADIOI_Free(temp_local_ranks);
    ADIOI_Free(check_local_aggregators);
    ADIOI_Free(check_global_aggregators);
    return 0;
}

/*
  Input:
       1. rank: process rank with respect to communicator comm.
       2. nprocs: total number of processes in the communicator comm.
       3. comm: a communicator comm that contains this process.
  Output:
       1. local_ranks : An array (size nprocs_node) that records the sorted (ascending) ranks of all processes on the same node with respect to communicator comm.
       2. nrecvs : An integer that tells how many nodes we have.
       3. node_size: An array (size nrecvs) that contains the size of processes per node for all nodes.
       4. nprocs_node: The number of processes at local node.
       5. global_receivers: An array (size nrecvs) that contains the ranks of proxy processes with respect to communicator comm.
       6. process_node_list : An array (size nprocs) that maps a process to a node (the node index correspond to the order of global receivers)
*/
int gather_node_information(int rank, int nprocs,int *nprocs_node,int *nrecvs, int** node_size, int** local_ranks, int** global_receivers, int **process_node_list, MPI_Comm comm){
    char *local_processor_name, **global_process_names, *temp, **unique_nodes;
    int i,j, name_len;
    /* The ranks of receivers are known at the beginning, the rest of code just simulate the operations for getting intra-node processes.*/
    /*Get processes on the same compute node dynamically*/
    if(rank==0){
        printf("Applying dynamic process rank assignment across nodes.\n");
    }
    local_processor_name = (char*) ADIOI_Calloc((MPI_MAX_PROCESSOR_NAME+1),sizeof(char));
    global_process_names = (char**) ADIOI_Malloc(sizeof(char*)*nprocs);
    global_process_names[0] = (char*) ADIOI_Malloc(sizeof(char) * (MPI_MAX_PROCESSOR_NAME+1) * nprocs);
    for (i=1; i<nprocs; i++){
        global_process_names[i] = global_process_names[i-1] + sizeof(char) * (MPI_MAX_PROCESSOR_NAME+1); 
    }
    MPI_Get_processor_name( local_processor_name, &name_len);
    MPI_Allgather(local_processor_name, (MPI_MAX_PROCESSOR_NAME+1), MPI_BYTE,
                  global_process_names[0], (MPI_MAX_PROCESSOR_NAME+1), MPI_BYTE,
                  comm);
    /* Figure out local process ranks (on the same node)*/
    local_ranks[0] = (int*) ADIOI_Malloc(sizeof(int) * nprocs);
    j=0;
    for (i=0; i<nprocs; i++){
        if(strcmp(local_processor_name,global_process_names[i]) == 0){
            local_ranks[0][j]=i;
            j++;
        }
    }
    nprocs_node[0] = j;
    temp=global_process_names[0];
    /* Sort string names in order for counting unique number of strings.*/
    stringSort(global_process_names,nprocs);
    /* Need to figure out how many nodes we have.*/
    nrecvs[0] = nprocs;
    /* Figure out total number of nodes and store into nrecvs*/
    for (i=1; i<nprocs; i++){
        if (strcmp(global_process_names[i-1],global_process_names[i])==0){
            nrecvs[0]--;
        }
    }
    /* Figure out the name of all nodes (stored in unique nodes referencing the global process name) and work out how many nodes in total (nrecvs as return value)*/
    unique_nodes = (char**) ADIOI_Malloc(sizeof(char*) * nrecvs[0]);
    unique_nodes[0] = global_process_names[0];
    j=1;
    for (i=1; i<nprocs; i++){
        if (strcmp(global_process_names[i-1],global_process_names[i])!=0){
            unique_nodes[j]=global_process_names[i];
            j++;
        }
    }
    global_receivers[0] = (int*) ADIOI_Malloc(sizeof(int) * nrecvs[0]);
    node_size[0] = (int*) ADIOI_Malloc(sizeof(int) * nrecvs[0]);
    process_node_list[0] = (int*) ADIOI_Malloc(sizeof(int) * nprocs);
    /* Figure out the size of processes at all nodes, also create process node map (mapping process to the node index it belongs to)*/
    for (i=0; i<nrecvs[0]; i++){
        node_size[0][i]=0;
        for (j=0; j<nprocs; j++){
            if (strcmp(temp+j*(MPI_MAX_PROCESSOR_NAME+1),unique_nodes[i])==0){
                process_node_list[0][j] = i;
                node_size[0][i]++;
            }
        }   
    }
    /* Retrieve the process with least rank per node.*/
    for (i=0; i<nrecvs[0]; i++){
        for (j=0; j<nprocs; j++){
            if (strcmp(temp+j*(MPI_MAX_PROCESSOR_NAME+1),unique_nodes[i])==0){
                global_receivers[0][i] = j;
                j=nprocs;
            }
        }
        
    }
    ADIOI_Free(local_processor_name);
    ADIOI_Free(global_process_names[0]);
    ADIOI_Free(global_process_names);
    ADIOI_Free(unique_nodes);
    return 0;
}
/*
  Same function as previous one for test purpose (processes are not necessarily physically placed on different nodes). nprocs_node is an input and nrecvs is computed based on it.
  Input:
       1. rank: process rank with respect to communicator comm.
       2. nprocs: total number of processes in the communicator comm.
       3. type: 0: contiguous process ranks for nodes, 1: round robin process ranks for nodes.
  Output:
       1. local_ranks : An array (size nprocs_node) that records the ranks of all processes on the same node with respect to communicator comm.
       2. nprocs_node: The number of processes at local node.
       3. nrecvs : An integer that tells how many nodes we have.
       4. node_size: An array (size nrecvs) that contains the size of processes per node for all nodes.
       5. global_receivers: An array (size nrecvs) that contains the ranks of proxy processes with respect to communicator comm.
       6. process_node_list : An array (size nprocs) that maps a process to a node (the node index correspond to the order of global receivers)
*/
int static_node_assignment(int rank, int nprocs, int type, int *nprocs_node,int *nrecvs, int** node_size, int** local_ranks, int** global_receivers, int **process_node_list){
    int i, remainder, temp, shift;
    if ( rank == 0 ){
        printf("static node assignment for %d node ( %d processes per node) of type %d\n", nprocs, nprocs_node[0], type );
    }
    if (type == 1){
        nrecvs[0] = (nprocs+nprocs_node[0]-1)/nprocs_node[0];
        local_ranks[0] = (int*) ADIOI_Malloc(sizeof(int) * nprocs_node[0]);
        global_receivers[0] = (int*) ADIOI_Malloc(sizeof(int) * nrecvs[0]);
        node_size[0] = (int*) ADIOI_Malloc(sizeof(int) * nrecvs[0]);
        process_node_list[0] = (int*) ADIOI_Malloc(sizeof(int) * nprocs);
        remainder = nprocs % nprocs_node[0];
        temp = nprocs / nprocs_node[0];
        if ( rank < remainder * nrecvs[0] ){
            shift = rank % nrecvs[0];
        } else{
            shift = (rank - remainder * nrecvs[0]) % temp; 
        }
        for ( i = 0; i < nrecvs[0]; i++){
            global_receivers[0][i] = i;
            if ( i == nrecvs[0] - 1 ){
                node_size[0][i] = nprocs - nprocs_node[0] * (nrecvs[0]-1);
            }else{
                node_size[0][i] = nprocs_node[0];
            }
        }
        if ( rank <  nrecvs[0] * remainder && rank % nrecvs[0] == temp ){
            nprocs_node[0] = remainder;
        }
        for ( i = 0; i < nprocs_node[0]; i++ ) {
            if ( i < remainder ){
                local_ranks[0][i] = shift + nrecvs[0] * i;
            } else{
                local_ranks[0][i] = shift + temp * (i - remainder) + remainder * nrecvs[0];
            }
        }
        for ( i = 0; i < nprocs; i++ ){
            if ( i < remainder * nrecvs[0] ){
                process_node_list[0][i] = i % nrecvs[0];
            } else{
                process_node_list[0][i] = (i - remainder * nrecvs[0] ) % temp;
            }

        }
    } else{
        nrecvs[0] = (nprocs+nprocs_node[0]-1)/nprocs_node[0];
        local_ranks[0] = (int*) ADIOI_Malloc(sizeof(int) * nprocs_node[0]);
        global_receivers[0] = (int*) ADIOI_Malloc(sizeof(int) * nrecvs[0]);
        node_size[0] = (int*) ADIOI_Malloc(sizeof(int) * nrecvs[0]);
        process_node_list[0] = (int*) ADIOI_Malloc(sizeof(int) * nprocs);
        for ( i = 0; i < nprocs_node[0]; i++ ){
            local_ranks[0][i] = (rank/nprocs_node[0])*nprocs_node[0] + i;
        }
        for ( i = 0; i < nrecvs[0]; i++ ){
            global_receivers[0][i] = i*nprocs_node[0];
            if ( i == nrecvs[0] - 1 ){
                node_size[0][i] = nprocs - nprocs_node[0] * (nrecvs[0]-1);
            }else{
                node_size[0][i] = nprocs_node[0];
            }
        }
        for ( i = 0; i < nprocs; i++ ){
            process_node_list[0][i] = i / nprocs_node[0];
        }
        if ( rank >= (nrecvs[0]-1) * nprocs_node[0] ){
            nprocs_node[0] = nprocs - nprocs_node[0] * (nrecvs[0]-1);
        }
    }

    return 0;
}

/*
  Input:
       1. rank: process rank
       2. nprocs: total number of processes
       3. local_ranks: process ranks at local node.
       4. global_receivers: proxy process at every node.
       5. nrecvs: size of global_receivers
       6. blocklen: message size unit
       7. type : Which pattern to be tested.
  Output:
       1. recv_size : An array (size nprocs) tells the size of messages to be received from the rest of processes.
       2. send_size : An array (size nprocs) tells the size of messages to be sent to the rest of processes.
       3. recv_buf : An array of receive buffer pointers (of size nprocs) for this process.
       4. send_buf : An array of send buffer pointers (of size nprocs) for this process.
*/

int initialize_setting(int rank, int nprocs, int *local_ranks, int *global_receivers, int nrecvs, int blocklen, int **recv_size, int **send_size, char ***recv_buf, char ***send_buf, int** global_aggregators, int *global_aggregator_size, int* is_aggregator, int type){
    int i, j;
    send_size[0] = (int*) ADIOI_Calloc(nprocs, sizeof(int));
    recv_size[0] = (int*) ADIOI_Calloc(nprocs, sizeof(int));
    recv_buf[0] = (char**) ADIOI_Calloc(nprocs, sizeof(char*));
    send_buf[0] = (char**) ADIOI_Calloc(nprocs, sizeof(char*));
    is_aggregator[0] = 0;
    switch(type) {
        case OST_STRIPE_SAME :
            global_aggregator_size[0] = nrecvs;
            global_aggregators[0] = (int*) ADIOI_Calloc(global_aggregator_size[0], sizeof(int));
            for ( i = 0; i < global_aggregator_size[0]; i++ ){
                global_aggregators[0][i] = global_receivers[i];
                if ( rank == global_aggregators[0][i] ){
                    is_aggregator[0] = 1;
                }
            }
            if (rank == local_ranks[0]){
                for ( i=0; i < nprocs; i++ ){
                    recv_size[0][i] = 1 + i % blocklen;
                    recv_buf[0][i] = (char*) ADIOI_Calloc(recv_size[0][i], sizeof(char));
                }
            }
            for ( i = 0; i < nrecvs; i++ ){
                send_size[0][global_receivers[i]] = 1 + rank % blocklen;
                send_buf[0][global_receivers[i]] = (char*) ADIOI_Calloc(send_size[0][global_receivers[i]], sizeof(char));
                for ( j = 0; j < send_size[0][global_receivers[i]]; j++ ){
                    send_buf[0][global_receivers[i]][j] = MAP_DATA(rank,global_receivers[i],j);
                }
            }
            break;
        case OST_STRIPE_GREATER :
            global_aggregator_size[0] = nprocs/2;
            global_aggregators[0] = (int*) ADIOI_Calloc(global_aggregator_size[0], sizeof(int));
            for ( i = 0; i < global_aggregator_size[0]; i++ ){
                global_aggregators[0][i] = 2 * i + 1;
                if ( rank == global_aggregators[0][i] ){
                    is_aggregator[0] = 1;
                }
            }
            if (rank % 2){
                for ( i=0; i < nprocs; i++ ){
                    recv_size[0][i] = 1 + i % blocklen;
                    recv_buf[0][i] = (char*) ADIOI_Calloc(recv_size[0][i], sizeof(char));
                }
            }
            for ( i = 0; i < nprocs; i++ ){
                if (i % 2){
                    send_size[0][i] = 1 + rank % blocklen;
                    send_buf[0][i] = (char*) ADIOI_Calloc(send_size[0][i], sizeof(char));
                    for ( j = 0; j < send_size[0][i]; j++ ){
                        send_buf[0][i][j] = MAP_DATA(rank,i,j);
                    }
                }
            }
            break;
        case OST_STRIPE_LESS :
            global_aggregator_size[0] = nprocs/2;
            global_aggregators[0] = (int*) ADIOI_Calloc(global_aggregator_size[0], sizeof(int));
            for ( i = 0; i < global_aggregator_size[0]; i++ ){
                global_aggregators[0][i] = i;
                if ( rank == global_aggregators[0][i] ){
                    is_aggregator[0] = 1;
                }
            }
            if (rank < nprocs/2){
                for ( i=0; i < nprocs; i++ ){
                    recv_size[0][i] = 1 + i % blocklen;
                    recv_buf[0][i] = (char*) ADIOI_Calloc(recv_size[0][i], sizeof(char));
                }
            }
            for ( i = 0; i < nprocs/2; i++ ){
                send_size[0][i] = 1 + rank % blocklen;
                send_buf[0][i] = (char*) ADIOI_Calloc(send_size[0][i], sizeof(char));
                for ( j = 0; j < send_size[0][i]; j++ ){
                    send_buf[0][i][j] = MAP_DATA(rank,i,j);
                }
            }
            break;
        case OST_STRIPE_ALL :
            global_aggregator_size[0] = nprocs;
            global_aggregators[0] = (int*) ADIOI_Calloc(global_aggregator_size[0], sizeof(int));
            for ( i = 0; i < global_aggregator_size[0]; i++ ){
                global_aggregators[0][i] = i;
                if ( rank == global_aggregators[0][i] ){
                    is_aggregator[0] = 1;
                }
            }
            for ( i=0; i < nprocs; i++ ){
                recv_size[0][i] = 1 + i % blocklen;
                recv_buf[0][i] = (char*) ADIOI_Calloc(recv_size[0][i], sizeof(char));
            }
            for ( i = 0; i < nprocs; i++ ){
                send_size[0][i] = 1 + rank % blocklen;
                send_buf[0][i] = (char*) ADIOI_Calloc(send_size[0][i], sizeof(char));
                for ( j = 0; j < send_size[0][i]; j++ ){
                    send_buf[0][i][j] = MAP_DATA(rank,i,j);
                }
            }
            break;
    }
    return 0;
}
/*
    In depth clean up of the following variables
    1. recv_size
    2. send_size
    3. recv_buf
    4. send_buf
*/
int clean_up(int nprocs, int **recv_size, int **send_size, char ***recv_buf, char ***send_buf){
    int i;
    for ( i = 0; i < nprocs; i++ ){
        if (recv_size[0][i]>0){
            ADIOI_Free(recv_buf[0][i]);
        }
        if (send_size[0][i]>0){
            ADIOI_Free(send_buf[0][i]);
        }
    }
    ADIOI_Free(recv_size[0]);
    ADIOI_Free(send_size[0]);
    ADIOI_Free(recv_buf[0]);
    ADIOI_Free(send_buf[0]);
    return 0;
}

int clean_aggregator_meta(int is_aggregator, int* aggregator_local_ranks, int* global_aggregators, int *process_aggregator_list){
    if (is_aggregator){
        ADIOI_Free(aggregator_local_ranks);
    }
    ADIOI_Free(global_aggregators);
    ADIOI_Free(process_aggregator_list);
    return 0;
}


/*
  Core communication function for three phase IO.
  aggregators gather data from non-aggregators first. Then, aggregators perform all-to-all communication to achieve the communication goal.
  Input:
       1. myrank: process rank
       2. nprocs: total number of processes
       3. local_aggregator_size: length of local_aggregators
       4. is_aggregator: if this process is an aggregator. (0 or 1)
       6. local_aggregators: rank of all local aggregators.
       7. process_aggregator_list: mapping from a process rank to the local aggregator rank responsible for sending its data to other aggregators.
       8. recv_size : An array (size nprocs) tells the size of messages to be received from the rest of processes.
       9. send_size : An array (size nprocs) tells the size of messages to be sent to the rest of processes.
       10. send_buf : An array of send buffer pointers (of size nprocs) for this process.
       11. iter: iteration index (only matters for communication tag)
       12. win_info: used for creating RMA window.
       13. intra_comm: communicator of local aggregator group.
       14. comm: communicator of this communication.
  Output:
       1. recv_types : Receive type that encapsulates the recv_buf. All non-contiguous regions should be filled with correct values in the end.
*/
int collective_write3(int myrank, int nprocs, int local_aggregator_size, int is_aggregator, int* local_aggregators, int *process_aggregator_list, int *recv_size, int *send_size, MPI_Datatype *recv_types, char **send_buf, int iter, MPI_Info win_info, MPI_Comm intra_comm, MPI_Comm comm){
    /*
      i, w, temp, temp2 : general purpose temporary integer variable
      ptr: general purpose temporal data pointer variable
      j : reserved for MPI request counting.
      temp_buf_size: dynamically allocated memory for data.
      total_send_size: total message size to be sent out from this process.
      local_lens: contains send size of ranks this aggregator that it is responsible for in inclusive prefix-sum form. This variable is significant only at the aggregators.
      aggregate_buf: buffer for an aggregator to gather data from the ranks it is responsible for. Data are contiguous and ordered by ranks of senders.
    */
    char *aggregate_buf = NULL, *ptr= NULL;
    int i, j, w, local_rank, nprocs_aggregator;
    MPI_Request *req = NULL;
    MPI_Status *sts = NULL;
    int *local_send_size = NULL, *array_of_blocklengths = NULL;
    MPI_Datatype *new_types;
    MPI_Aint total_send_size, temp = 0, temp2 = 0, *array_of_displacements = NULL, *local_lens = NULL;
    MPI_Win win_data;

    MPI_Comm_rank(intra_comm, &local_rank);
    MPI_Comm_size( intra_comm, &nprocs_aggregator ); 
    //printf("local_rank= %d, rank = %d, process_aggregator_list[%d] = %d\n",local_rank,myrank,myrank,process_aggregator_list[myrank]);

    array_of_blocklengths = (int*) ADIOI_Malloc(nprocs * sizeof(int));
    array_of_displacements = (MPI_Aint*) ADIOI_Malloc(nprocs * sizeof(MPI_Aint));
    /* Intra-node gather for send size to a proxy process.*/
    /* Exchange for local send/recv size*/
    j = 0;
    if (is_aggregator){
        req = (MPI_Request *) ADIOI_Malloc((local_aggregator_size * 2 + 1) * sizeof(MPI_Request));
        sts = (MPI_Status *) ADIOI_Malloc((local_aggregator_size * 2 + 1) * sizeof(MPI_Status));
        local_lens = (MPI_Aint*) ADIOI_Malloc(sizeof(MPI_Aint)*nprocs*nprocs_aggregator);
    } else{
        req = (MPI_Request *) ADIOI_Malloc(sizeof(MPI_Request));
        sts = (MPI_Status *) ADIOI_Malloc(sizeof(MPI_Status));
    }
    /* Count total message size to be sent/recv from this process.*/
    total_send_size = 0;
    for ( i = 0; i < nprocs; i++){
        total_send_size += send_size[i];
    }
    /* End of intra-group data size exchange*/
    /* Gather data from non-aggregators to the aggregator that is responsible for sending their data.*/
    MPI_Win_allocate_shared(sizeof(char)*total_send_size+sizeof(int)*nprocs, sizeof(char), win_info, intra_comm, &ptr, &win_data);
    //MPI_Win_fence(MPI_MODE_NOPRECEDE,win_data);
    //MPI_Win_lock_all(0 /* assertion */, win_data);
    //MPI_Win_lock(MPI_LOCK_SHARED, local_rank, 0, win_data);
    memcpy(ptr,send_size,nprocs*sizeof(int));
    ptr += nprocs*sizeof(int);
    for (i = 0; i < nprocs; i++){
        if (send_size[i] ){
            memcpy(ptr, send_buf[i] ,send_size[i]*sizeof(char));
            ptr += sizeof(char) * send_size[i];
        }
    }
    //MPI_Win_unlock_all(win_data);
    //MPI_Win_unlock(local_rank, win_data);
    //MPI_Barrier(intra_comm);
    MPI_Win_fence(MPI_MODE_NOSUCCEED, win_data);
    /* End of intragroup data gather to aggregators*/
    /* No further communication for non-aggregators. From now on, all communication are internode communications.*/
    if (is_aggregator){
        /* Convert the send size into a inclusive prefix sum format.*/
        MPI_Win_shared_query(win_data, 0, &temp, &i, &aggregate_buf);
        temp = 0;
        for ( i = 0; i < nprocs_aggregator; i++ ){
            // We get the header for send size per process in the local aggregator communicator
            MPI_Win_shared_query(win_data, i, &temp2, &w, &local_send_size);
            for ( w = 0; w < nprocs; w++ ){
                temp += local_send_size[w];
                local_lens[ i * nprocs + w ] = temp;
            }
        }
        j=0;
        new_types = (MPI_Datatype*) ADIOI_Malloc(local_aggregator_size*sizeof(MPI_Datatype));
        /* intergroup communication among all aggregators*/
        for ( i = 0; i < local_aggregator_size; i++){
            /* prepare s_buf*/
            /* temp2 is send size to local_aggregators[i]*/
            temp2 = 0;
            // Iterate through all local processes and arrange the data to be sent to local_aggregators[i] by order of recv ranks into new_types.
            for ( w = 0; w < nprocs_aggregator; w++ ) {
                // Recall that local_lens is inclusive prefix sum of send length.
                if ( w == 0 && local_aggregators[i] == 0 ){
                    array_of_blocklengths[w] = local_lens[w * nprocs + local_aggregators[i]];
                    MPI_Address(aggregate_buf + sizeof(int) * nprocs, array_of_displacements + w);
                } else {
                    /* Inclusive prefix sum (current index - previous) index works out the value at current index.*/
                    array_of_blocklengths[w] = local_lens[w * nprocs + local_aggregators[i]] - local_lens[w * nprocs + local_aggregators[i] - 1];
                    MPI_Address(aggregate_buf + sizeof(int) * nprocs * (w + 1) + local_lens[w * nprocs + local_aggregators[i] - 1], array_of_displacements + w);
                }
                temp2 += array_of_blocklengths[w];
            }
            /* temp is recv size from local_aggregators[i]*/
            temp = 0;
            for ( w = 0; w < nprocs; w++ ){
                if (local_aggregators[i] == process_aggregator_list[w] ){
                    temp += recv_size[w];
                }
            }
            /* Intergroup send/recv among local aggregators, global aggregators are subset of them*/
            MPI_Type_create_hindexed(nprocs_aggregator, array_of_blocklengths, array_of_displacements, MPI_BYTE, new_types+i);
            MPI_Type_commit(new_types+i);
            if (temp2){
                MPI_Issend(MPI_BOTTOM, 1, new_types[i], local_aggregators[i], local_aggregators[i] + myrank + 100 * iter, comm, &req[j++]);
            }
            if (temp){
                MPI_Irecv(MPI_BOTTOM, 1, recv_types[i], local_aggregators[i], local_aggregators[i] + myrank + 100 * iter, comm, &req[j++]);
            }
        }
        // wait for all irecv/isend to complete
        MPI_Waitall(j, req, sts);
        ADIOI_Free(local_lens);
        for (i = 0; i < local_aggregator_size; i++){
            MPI_Type_free(new_types + i);
        }
        ADIOI_Free(new_types);
    }
    ADIOI_Free(array_of_blocklengths);
    ADIOI_Free(array_of_displacements);
    ADIOI_Free(req);
    ADIOI_Free(sts);
    MPI_Win_free(&win_data);
    return 0;
}


/*
  Core communication function for three phase IO.
  aggregators gather data from non-aggregators first. Then, aggregators perform all-to-all communication to achieve the communication goal.
  Input:
       1. myrank: process rank
       2. nprocs: total number of processes
       3. nprocs_aggregator: length of aggregator_local_ranks
       4. global_aggregator_size: length of local_aggregators
       5. local_aggregator_size: length of local_aggregators
       6. is_global_aggregator: if this process is a global aggregator. (0 or 1)
       7. is_local_aggregator: if this process is a local aggregator. (0 or 1)
       8. aggregator_local_ranks: process ranks this aggregator is responsible for serving as a proxy for message send. (significant if is_aggregator is 1)
       9. global_aggregators: rank of all global aggregators.
       10. local_aggregators: rank of all local aggregators.
       11. process_aggregator_list: mapping from a process rank to the local aggregator rank responsible for sending its data to other aggregators.
       12. recv_size : An array (size nprocs) tells the size of messages to be received from the rest of processes.
       13. send_size : An array (size nprocs) tells the size of messages to be sent to the rest of processes.
       14. send_buf : An array of send buffer pointers (of size nprocs) for this process.
       15. iter: iteration index (only matters for communication tag)
       16. comm: communicator of this communication.
  Output:
       1. recv_types : Receive type that encapsulates the recv_buf. All non-contiguous regions should be filled with correct values in the end.
*/
int collective_write2(int myrank, int nprocs, int nprocs_aggregator, int global_aggregator_size, int local_aggregator_size, int is_global_aggregator, int is_local_aggregator, int* aggregator_local_ranks, int* global_aggregators, int* local_aggregators, int *process_aggregator_list, int *recv_size, int *send_size, MPI_Datatype *recv_types, char **send_buf, int iter, MPI_Comm comm){
    /*
      i, w, temp, temp2 : general purpose temporary integer variable
      ptr: general purpose temporal data pointer variable
      j : reserved for MPI request counting.
      temp_buf_size: dynamically allocated memory for data.
      total_send_size: total message size to be sent out from this process.
      local_lens: contains send size of ranks this aggregator that it is responsible for in inclusive prefix-sum form. This variable is significant only at the aggregators.
      aggregate_buf: buffer for an aggregator to gather data from the ranks it is responsible for. Data are contiguous and ordered by ranks of senders.
    */
    char *aggregate_buf = NULL, *ptr= NULL;
    int i, j, w;
    MPI_Request *req = NULL;
    MPI_Status *sts = NULL;
    int *local_send_size = NULL, *array_of_blocklengths = NULL;
    MPI_Datatype new_type, *new_types = NULL;
    MPI_Aint total_send_size, temp = 0, temp2 = 0, temp_buf_size = 0, *array_of_displacements = NULL, *local_lens = NULL;
    array_of_blocklengths = (int*) ADIOI_Malloc(nprocs * sizeof(int));
    array_of_displacements = (MPI_Aint*) ADIOI_Malloc(nprocs * sizeof(MPI_Aint));
    /* Intra-node gather for send size to a proxy process.*/
    /* Exchange for local send/recv size*/
    if (is_local_aggregator||is_global_aggregator){
        j = 0;
        if (is_global_aggregator){
            j += local_aggregator_size;
        }
        if (is_local_aggregator){
            j += global_aggregator_size;
        }
        if (nprocs_aggregator > j && is_local_aggregator){
            req = (MPI_Request *) ADIOI_Malloc((nprocs_aggregator + 1) * sizeof(MPI_Request));
            sts = (MPI_Status *) ADIOI_Malloc((nprocs_aggregator + 1) * sizeof(MPI_Status));
        } else{
            req = (MPI_Request *) ADIOI_Malloc((j + 1) * sizeof(MPI_Request));
            sts = (MPI_Status *) ADIOI_Malloc((j + 1) * sizeof(MPI_Status));
        }
    } else{
        req = (MPI_Request *) ADIOI_Malloc(sizeof(MPI_Request));
        sts = (MPI_Status *) ADIOI_Malloc(sizeof(MPI_Status));
    }
    j = 0;
    if (is_local_aggregator){
        local_send_size = (int*) ADIOI_Malloc(sizeof(int)*nprocs*nprocs_aggregator);
        local_lens = (MPI_Aint*) ADIOI_Malloc(sizeof(MPI_Aint)*nprocs*nprocs_aggregator);
        for ( i = 0; i < nprocs_aggregator; i++){
            if (aggregator_local_ranks[i] != myrank){
                MPI_Irecv(local_send_size + i * nprocs, nprocs, MPI_INT, aggregator_local_ranks[i], aggregator_local_ranks[i] + myrank + 100 * iter, comm, &req[j++]);
            } else{
                memcpy(local_send_size + i * nprocs, send_size, sizeof(int) * nprocs);
            }
        }
    }
    if (process_aggregator_list[myrank] != myrank){
        MPI_Isend(send_size, nprocs, MPI_INT, process_aggregator_list[myrank], myrank + process_aggregator_list[myrank] + 100 * iter, comm, &req[j++]);
    }
    /* Count total message size to be sent/recv from this process.*/
    total_send_size = 0;
    for ( i = 0; i < nprocs; i++){
        total_send_size += send_size[i];
    }
    MPI_Waitall(j, req, sts);
    /* End of intra-group data size exchange*/

    /* Gather data from non-aggregators to the aggregator that is responsible for sending their data.*/
    j = 0;
    if (is_local_aggregator){
        /* Convert the send size into a inclusive prefix sum format.*/
        temp = 0;
        for ( i = 0; i < nprocs_aggregator; i++ ){
            for ( w = 0; w < nprocs; w++ ){
                temp += local_send_size[ i * nprocs + w ];
                local_lens[ i * nprocs + w ] = temp;
            }
        }
        /* Allocate data buffer: local gather data for processes on the same node + concatenating data on this process*/
        temp_buf_size = local_lens[ nprocs_aggregator * nprocs - 1 ];
        if (temp_buf_size){
            aggregate_buf = (char*) ADIOI_Malloc(sizeof(char) * temp_buf_size);
        }
        /* Copy send data from send buffer to a contiguous memory space (to be sent out at once.)*/
        ptr = aggregate_buf;
        for ( i = 0; i < nprocs_aggregator; i++){
            if ( i == 0 ){
                temp = local_lens[nprocs - 1];
            }else{
                temp = local_lens[(i + 1) * nprocs - 1] - local_lens[ i * nprocs - 1 ];
            }
            if ( temp ){
                MPI_Irecv(ptr, temp, MPI_BYTE, aggregator_local_ranks[i], aggregator_local_ranks[i] + myrank + 100 * iter, comm, &req[j++]);
                ptr += temp;
            }
        }
    }

    if (total_send_size){
        for (i = 0; i < nprocs; i++){
            array_of_blocklengths[i] = send_size[i];
            MPI_Address(send_buf[i], array_of_displacements + i);
        }
        MPI_Type_create_hindexed(nprocs, array_of_blocklengths, array_of_displacements, MPI_BYTE, &new_type);
        MPI_Type_commit(&new_type);
        MPI_Issend(MPI_BOTTOM, 1, new_type, process_aggregator_list[myrank], myrank + process_aggregator_list[myrank] + 100 * iter, comm, &req[j++]);
    }
    MPI_Waitall(j, req, sts);
    if (total_send_size){
        MPI_Type_free(&new_type);
    }
    /* End of intragroup data gather to aggregators*/
    /* No further communication for non-aggregators. From now on, all communication are internode communications.*/
    j=0;
    if (is_global_aggregator){
        for ( i = 0; i < local_aggregator_size; i++){
            /* temp is recv size from local_aggregators[i]*/
            temp = 0;
            for ( w = 0; w < nprocs; w++ ){
                if (local_aggregators[i] == process_aggregator_list[w] ){
                    temp += recv_size[w];
                }
            }
            if (temp){
                MPI_Irecv(MPI_BOTTOM, 1, recv_types[i], local_aggregators[i], local_aggregators[i] + myrank + 100 * iter, comm, &req[j++]);
            }
        }
    }
    if (is_local_aggregator){
        new_types = (MPI_Datatype*) ADIOI_Malloc(global_aggregator_size*sizeof(MPI_Datatype));
        /* intergroup communication among all aggregators*/
        for ( i = 0; i < global_aggregator_size; i++){
            /* prepare s_buf*/
            /* temp2 is send size to global_aggregators[i]*/
            temp2 = 0;
            // Iterate through all local processes and arrange the data to be sent to global_aggregators[i] by order of recv ranks into new_types.
            for ( w = 0; w < nprocs_aggregator; w++ ) {
                // Recall that local_lens is inclusive prefix sum of send length.
                if ( w == 0 && global_aggregators[i] == 0 ){
                    array_of_blocklengths[w] = local_lens[w * nprocs + global_aggregators[i]];
                    MPI_Address(aggregate_buf, array_of_displacements + w);
                } else {
                    /* Inclusive prefix sum (current index - previous) index works out the value at current index.*/
                    array_of_blocklengths[w] = local_lens[w * nprocs + global_aggregators[i]] - local_lens[w * nprocs + global_aggregators[i] - 1];
                    MPI_Address(aggregate_buf + local_lens[w * nprocs + global_aggregators[i] - 1], array_of_displacements + w);
                }
                temp2 += array_of_blocklengths[w];
            }
            /* Intergroup send/recv among local aggregators, global aggregators are subset of them*/
            MPI_Type_create_hindexed(nprocs_aggregator, array_of_blocklengths, array_of_displacements, MPI_BYTE, new_types+i);
            MPI_Type_commit(new_types+i);
            if (temp2){
                MPI_Issend(MPI_BOTTOM, 1, new_types[i], global_aggregators[i], global_aggregators[i] + myrank + 100 * iter, comm, &req[j++]);
            }
        }
    }
    if (j>0){
        // wait for all irecv/isend to complete
        MPI_Waitall(j, req, sts);
    }
    if (is_local_aggregator){
        ADIOI_Free(local_send_size);
        ADIOI_Free(local_lens);
        for (i = 0; i < global_aggregator_size; i++){
            MPI_Type_free(new_types + i);
        }
        ADIOI_Free(new_types);
        if (temp_buf_size){
            ADIOI_Free(aggregate_buf);
        }
    }
    ADIOI_Free(array_of_blocklengths);
    ADIOI_Free(array_of_displacements);
    ADIOI_Free(req);
    ADIOI_Free(sts);
    return 0;
}


/*
  Input:
       1. myrank: process rank
       2. nprocs: total number of processes
       3. nprocs_node: number of processes at local node.
       4. nrecvs: number of nodes among all pocesses.
       5. local_ranks: process ranks at local node.
       6. global_receivers: proxy process at every node.
       7. process_node_list: mapping from process to the node index it belongs.
       8. recv_size : An array (size nprocs) tells the size of messages to be received from the rest of processes.
       9. send_size : An array (size nprocs) tells the size of messages to be sent to the rest of processes.
       10. send_buf : An array of send buffer pointers (of size nprocs) for this process.
  Output:
       1. recv_buf : An array of receive buffer pointers (of size nprocs) for this process. It must have the correct messages in the end.
*/
int collective_write(int myrank, int nprocs, int nprocs_node, int nrecvs, int* local_ranks, int* global_receivers, int *process_node_list, int *recv_size, int *send_size, char **recv_buf, char **send_buf, int iter, MPI_Comm comm){
    char *aggregate_buf = NULL, *local_buf = NULL, *tmp_buf = NULL, *ptr, *ptr2, *s_buf2 = NULL, **r_buf = NULL, **ptrs = NULL;
    int i, j, w, v, temp=0, temp2=0;
    MPI_Request *intra_req, *req = NULL;
    MPI_Status *intra_sts, *sts = NULL;
    int *global_s_lens = NULL, *global_r_lens = NULL, *local_lens = NULL, node_message_size=0, r_rank, node_recv_size=0, *s_lens = NULL, *r_lens = NULL, total_recv_size, total_send_size, aggregate_buffer_size = 0;
    /* Count total message size to be sent/recv from this process. (To be optimized)*/
    total_send_size = 0;
    total_recv_size = 0;
    for (i = 0; i < nprocs; i++){
        total_send_size += send_size[i];
        total_recv_size += recv_size[i];
    }
    /*
      temp, temp2 : general purpose temporary integer variable
      total_recv_size: total message size to be received for this process.
      node_message_size: for proxy process only. Total message size to be sent out from this node.
      node_recv_size: for proxy process only. Total message size to be received from global nodes.
      j : reserved for MPI request counting.
    */
    /*
      Dynamic memory allocation for pointers unrelated to data size can be executed in advance.
      Those variables that are related to data size are freed immediately after usage.
    */
    if (myrank==local_ranks[0]){
        /* Request and status used for intra-node communication (proxy has receive + nproces_node number of operations at most)*/
        intra_req = (MPI_Request *) ADIOI_Malloc((nprocs_node + 2 * nrecvs) * sizeof(MPI_Request));
        intra_sts = (MPI_Status *) ADIOI_Malloc((nprocs_node + 2 * nrecvs) * sizeof(MPI_Status));
        /* Requeust and status used for inter-node communication*/
        req = intra_req + nprocs_node;
        sts = intra_sts + nprocs_node;
        /* Intra-node send/receive size (every process locally * every process globally)*/
        s_lens = (int*) ADIOI_Malloc(2*sizeof(int)*nprocs*nprocs_node);
        r_lens = s_lens + nprocs * nprocs_node;
        /* Inter-node send size (to all nodes)*/
        global_s_lens = (int*) ADIOI_Malloc(2*sizeof(int)*nrecvs);
        /* Inter-node recv size (from all nodes)*/
        global_r_lens = global_s_lens + nrecvs;
        /* A buffer for r_buf (each row of r_buf stores all messages received from inter-node process)*/
        ptrs = (char**) ADIOI_Malloc(sizeof(char*)*nrecvs);
        /* r_buf is a two dimensional array. Every row contains buffer to be received from proxy processes at a node. */
        r_buf = (char **) ADIOI_Malloc(nrecvs*sizeof(char*));
        // Buffer for receiving send/recv size from processes on local node.
        local_lens = (int*) ADIOI_Malloc(sizeof(int)*nprocs*nprocs_node*2);
    }else{
        /* Non-proxy process can only receive or send one at a time*/
        intra_req = (MPI_Request *) ADIOI_Malloc(sizeof(MPI_Request));
        intra_sts = (MPI_Status *) ADIOI_Malloc(sizeof(MPI_Status));
        // Buffer for sending send and receive size at the same node
        local_lens = (int*) ADIOI_Malloc(sizeof(int)*nprocs*2);
    }
    /* Intra-node gather for send size to a proxy process.*/
    j = 0;
    /* Send local send size to proxy process*/
    /* Condition for the proxy process, (note it may not be an aggregator)*/

    if (myrank==local_ranks[0]){
        /* Proxy process receive from local processes for their send size*/
        memcpy(s_lens, send_size, sizeof(int) * nprocs);
        memcpy(r_lens, recv_size, sizeof(int) * nprocs);
        for (i=1; i<nprocs_node; i++){
            MPI_Irecv(local_lens + i * nprocs * 2, 2 * nprocs, MPI_INT, local_ranks[i], local_ranks[i] + local_ranks[0] + 100 * iter, comm, &intra_req[j++]);

        }
    } else{
        memcpy(local_lens,send_size,sizeof(int)*nprocs);
        memcpy(local_lens+nprocs,recv_size,sizeof(int)*nprocs);
        MPI_Isend(local_lens, 2 * nprocs, MPI_INT, local_ranks[0], myrank + local_ranks[0] + 100 * iter, comm, &intra_req[j++]);
    }
    if (j) {
        MPI_Waitall(j, intra_req, intra_sts);
    }
    #if DEBUG==1
    MPI_Barrier(comm);
    if (myrank==0){
        printf("rank %d starting intra-node gather recv size\n",myrank);
    }
    #endif
    /* Local processes send their receive size to the proxy process (for later use after inter-node communication.)*/
    j = 0;
    if (myrank==local_ranks[0]){
        for ( i = 1; i < nprocs_node; i++ ){
            memcpy(s_lens + i * nprocs, local_lens + nprocs * 2 * i, sizeof(int) * nprocs);
            memcpy(r_lens + i * nprocs, local_lens + nprocs * ( 2 * i + 1 ), sizeof(int) * nprocs);
        }
        /* s_lens and r_lens are converted to prefix-sum representation (exclusive) (for the purpose of optimization used later)*/
        for ( i = 0; i < nprocs*nprocs_node; i++ ){
            temp2 = s_lens[i];
            s_lens[i] = node_message_size;
            node_message_size += temp2;

            temp = r_lens[i];
            r_lens[i] = node_recv_size;
            node_recv_size += temp;
        }
        /* local_lens will not be used anymore*/
        ADIOI_Free(local_lens);
        /* End of intra-node gather for send/recv size.
           s_lens stores size nprocs_node * nprocs number of integers that indicate which local process sends to which aggregator (at local proxy process).
        */
        /* We copy all the data into a contiguous buffer and send at once.*/
        /* We aggregate all messages to be sent to a local proxy that does everything for this node at once.*/
        if (node_message_size < node_recv_size){
            aggregate_buffer_size = node_recv_size;
        }else{
            aggregate_buffer_size = node_message_size;
        }
        temp = aggregate_buffer_size;
        if (aggregate_buffer_size){
            /* s_buf2 is the buffer used for reordering aggregate_buf*/
            s_buf2 = (char*) ADIOI_Malloc(sizeof(char)*node_message_size);
        }
    }
    if (total_send_size < total_recv_size){
        aggregate_buffer_size += total_recv_size;
    }else{
        aggregate_buffer_size += total_send_size;
    }
    if (aggregate_buffer_size){
        aggregate_buf = (char*) ADIOI_Malloc(sizeof(char)*aggregate_buffer_size);
        local_buf = aggregate_buf + temp;
    }
    if (total_send_size){
        tmp_buf = local_buf;
        for (i = 0; i < nprocs; i++){
            if (send_size[i]) {
                memcpy(tmp_buf, send_buf[i], sizeof(char)*send_size[i]);
                tmp_buf += send_size[i];
            }
        }
    }
    /*Send messages to local proxy*/
    j = 0;
    /* For local proxy, it receives messages from all processes on the same node.*/
    if (myrank==local_ranks[0]){
        /* Contiguous storage for messages of local processes (in order to local process, then in order of target process rank per process)*/
        if (total_send_size){
            memcpy(aggregate_buf, local_buf, sizeof(char) * total_send_size);
        }
        ptr = aggregate_buf + total_send_size;
        for (i=1; i<nprocs_node; i++){
            if ( i == nprocs_node - 1 ){
                temp = node_message_size - s_lens[i*nprocs];
            } else{
                temp = s_lens[(i+1)*nprocs] - s_lens[i*nprocs];
            }
            if (temp) {
                MPI_Irecv(ptr, temp, MPI_BYTE, local_ranks[i], local_ranks[i] + local_ranks[0] + 100 * iter, comm, &intra_req[j++]);
            }
            ptr += temp;
        }
    }else{
        if (total_send_size){
            MPI_Issend(local_buf, total_send_size, MPI_BYTE, local_ranks[0], myrank + local_ranks[0] + 100 * iter, comm, &intra_req[j++]);
        }
    }
    if (j) {
        MPI_Waitall(j, intra_req, intra_sts);
    }
    /* End of intra-group gather*/
    #if DEBUG==1
    MPI_Barrier(comm);
    if (myrank==0){
        printf("rank %d starting inter-node gather\n",myrank);
    }
    #endif
    /*Proxy processses at different nodes exchange messages (all-to-all) */
    if (myrank==local_ranks[0]){
        j=0;
        /* Keep track of the intergroup send buffer*/
        ptr = s_buf2;
        /* Figure out the message and size to be sent to individual aggregator*/
        for (i=0; i<nrecvs; i++){
            temp2 = 0;
            // Iterate through all processes
            // These loops would prepare for all messages (arranged contiguously) to be sent to process v in order of local process ranks.
            for ( v = 0; v < nprocs; v++ ){
                // Check if this process belongs to the ith node.
                if ( process_node_list[v] == i){
                    for ( w = 0; w < nprocs_node; w++ ) {
                        // Recall that s_lens is exclusive prefix sum of send length.
                        // s_lens[w*nprocs + v] means the message size of the wth process send to process v (note it is in exclusive prefix-sum representation, so we can jump the data pointer to the location without extra computation).
                        temp = w*nprocs+v;
                        if ( temp < nprocs * nprocs_node -1 ){
                            if (s_lens[temp+1]-s_lens[temp]){
                                memcpy(ptr+temp2, aggregate_buf+s_lens[temp], s_lens[temp+1]-s_lens[temp]);
                                temp2 += (s_lens[temp+1]-s_lens[temp]);
                            }
                        } else {
                            if (node_message_size - s_lens[temp]){
                                memcpy(ptr+temp2, aggregate_buf+s_lens[temp], node_message_size - s_lens[temp]);
                                temp2 += (node_message_size - s_lens[temp]);
                            }
                        }
                    }
                }
            }
            ptr += temp2;
            /* Exchange receive size among receivers*/
            r_rank = global_receivers[i];
            // global_s_lens[i] is the total message size to be sent from this node to the ith node.
            global_s_lens[i] = temp2;
            //Let the ith node proxy know the message size to be sent from this node.
            if (r_rank != myrank){
                MPI_Issend(global_s_lens+i, 1, MPI_INT, r_rank, r_rank + myrank + 100 * iter, comm, &req[j++]);
                //Figure out the message to be received from the ith node proxy.
                MPI_Irecv(global_r_lens+i, 1, MPI_INT, r_rank, r_rank + myrank + 100 * iter, comm, &req[j++]);
            }else{
                global_r_lens[i] = temp2;
            }
        }
        /* End of intergroup message size exchange. global_s_lens is an array of size nrecvs (number of nodes) that stores the aggregated message size to be sent to different node from this node. global_r_lens is an array of size nrecvs that stores the message size to be received from all nodes.*/
        MPI_Waitall(j, req, sts);
        j=0;
        // Exchange aggregated messages among receivers
        ptr2=s_buf2;
        r_buf[0] = (char *) ADIOI_Malloc(node_recv_size*sizeof(char));
        for (i=0; i<nrecvs; i++){
            r_rank = global_receivers[i];
            if ( i > 0 ){
                r_buf[i] = r_buf[i-1] + global_r_lens[i-1];
            }
            if (myrank != r_rank){
                if (global_s_lens[i]){
                    MPI_Issend(ptr2, global_s_lens[i], MPI_BYTE, r_rank, r_rank + myrank + 100 * iter, comm, &req[j++]);
                }
                if (global_r_lens[i]){
                    MPI_Irecv(r_buf[i], global_r_lens[i], MPI_BYTE, r_rank, r_rank + myrank + 100 * iter, comm, &req[j++]);
                }
            }else{
                if (global_r_lens[i]){
                    memcpy(r_buf[i],ptr2,sizeof(char)*global_r_lens[i]);
                }
            }
            ptr2 += global_s_lens[i];
            //store the beginning of buffer received from every receiver.
            ptrs[i] = r_buf[i];
        }
        // wait for all irecv/isend to complete
        if (j){
            MPI_Waitall(j, req, sts);
        }
    }
    /* End of inter-node exchange of messages*/
    #if DEBUG==1
    MPI_Barrier(comm);
    if (myrank==0){
        printf("finished inter-node gather\n");
    }
    #endif
    /* Finally, the aggregators receives messages from the sender.*/
    // A process simply receive aggregated message from the proxy in order of seneder process.
    #if DEBUG==1
    MPI_Barrier(comm);
    if (myrank==0){
        printf("starting local message delivery\n");
    }
    #endif
    j=0;
    if (myrank==local_ranks[0]){
        // We must create a buffer that can be used to reorder messages. Messages received from individual node proxy process is ordered. However, the ranks are not necessarily ordered (depending on configuration). We have to pack the messages again to align with the request of individual local process.
        // Pack messages to be sent to a local process.
        if (total_recv_size){
            //ptr = local_buf;
            for (w=0; w<nprocs; w++){
                memcpy(recv_buf[w],ptrs[process_node_list[w]],sizeof(char)*recv_size[w]);
                //memcpy(ptr,ptrs[process_node_list[w]],sizeof(char)*recv_size[w]);
                // ptrs[process_node_list[w]] is in order, we can shift the pointer to the next location and access its content.
                ptrs[process_node_list[w]] += recv_size[w];
                //ptr += recv_size[w];
            }
            //memcpy(local_buf,aggregate_buf,sizeof(char)*total_recv_size);
        }
        ptr = aggregate_buf;
        for (i=1; i<nprocs_node; i++){
            // Figure out the total recv size of a local process.
            if ( i == nprocs_node -1 ){
                temp = node_recv_size - r_lens[i*nprocs];
            } else{
                temp = r_lens[(i+1)*nprocs] - r_lens[i*nprocs];
            }
            // Do something when the target process is an aggregator.
            if (temp){
                /*
                  ptrs is the pointer to the buffer received from remote proxy processes. The messages are ordered by ranks of local ranks, then messages for each local rank are ordered by the source ranks (at remote nodes)
                  ptrs[process_node_list[w]] means that we are trying to find the node that process w belongs to, so we can know which inter-node buffer we need to access.
                  r_lens[i*nprocs+w] is the exclusive prefix-sum receive size of processlocal_ranks[i] from process w (with respect to comm)
                */
                ptr2 = ptr;
                for (w=0; w<nprocs; w++){
                    if ( i == nprocs_node -1 && w == nprocs -1 ){
                        temp2 = node_recv_size - r_lens[i*nprocs+w];
                    } else{
                        temp2 = r_lens[i*nprocs+w+1] - r_lens[i*nprocs+w];
                    }
                    memcpy(ptr,ptrs[process_node_list[w]],sizeof(char)*temp2);
                    // ptrs[process_node_list[w]] is in order, we can shift the pointer to the next location and access its content.
                    ptrs[process_node_list[w]] += temp2;
                    ptr += temp2;
                }
                MPI_Issend(ptr2, temp, MPI_BYTE, local_ranks[i], local_ranks[i] + local_ranks[0] + 100 * iter, comm, &intra_req[j++]);
            }
        }
    } else{
        if (total_recv_size){
            MPI_Irecv(local_buf, total_recv_size, MPI_BYTE, local_ranks[0], myrank + local_ranks[0] + 100 * iter, comm, &intra_req[j++]);
        }
    }
    if (j) {
        MPI_Waitall(j, intra_req, intra_sts);
    }
    #if DEBUG==1
    MPI_Barrier(comm);
    if (myrank==0){
        printf("finished local message delivery\n");
    }
    #endif
    /* Aggregator finally copy the messages received from local proxy (in order) to the target recv buffer.*/

    if (myrank!=local_ranks[0]&&total_recv_size){
        ptr = local_buf;
        for (i=0; i<nprocs; i++){
            if (recv_size[i]){
                memcpy(recv_buf[i],ptr,sizeof(char)*recv_size[i]);
                ptr += recv_size[i];
            }
        }
    }

    if (myrank==local_ranks[0]){
        ADIOI_Free(global_s_lens);
        ADIOI_Free(s_lens); /*r_lens is freed together*/
        ADIOI_Free(ptrs);
        ADIOI_Free(r_buf[0]);
        ADIOI_Free(r_buf);
        if (node_recv_size || node_message_size){
            ADIOI_Free(s_buf2);
        }
    }
    if (aggregate_buffer_size){
        ADIOI_Free(aggregate_buf);
    }
    ADIOI_Free(intra_req);
    ADIOI_Free(intra_sts);
    #if DEBUG==1
    MPI_Barrier(comm);
    if (myrank==0){
        printf("finished collective write\n");
    }
    #endif
    return 0;
}

int collective_write_benchmark(int myrank, int nprocs, int *recv_size, int *send_size, char **recv_buf, char **send_buf, int iter, MPI_Comm comm){
    int i, j = 0;
    MPI_Request *req = (MPI_Request *) ADIOI_Malloc(2 * nprocs * sizeof(MPI_Request));
    MPI_Status *sts = (MPI_Status *) ADIOI_Malloc(2 * nprocs * sizeof(MPI_Status));
    for ( i = 0; i < nprocs; i++ ){
        if (send_size[i]) {
            //printf("sender %d to aggregator rank %d\n",myrank,i);
            MPI_Issend(send_buf[i], send_size[i], MPI_BYTE, i, i + myrank + 100 * iter, comm, &req[j++]);
        }
        if (recv_size[i]) {
            //printf("aggregator %d from process rank %d\n",myrank,i);
            MPI_Irecv(recv_buf[i], recv_size[i], MPI_BYTE, i, myrank + i + 100 * iter, comm, &req[j++]);
        }
    }
    MPI_Barrier(comm);
    MPI_Waitall(j, req, sts);
    ADIOI_Free(req);
    ADIOI_Free(sts);
    return 0;
}

int create_recv_type(int nprocs, char** recv_buf, int* recv_size, int* local_aggregators, int local_aggregator_size, int *process_aggregator_list, MPI_Datatype** new_types){
    int i, j, w;
    int *array_of_blocklengths = (int*) ADIOI_Malloc(nprocs*sizeof(int));
    MPI_Aint *array_of_displacements = (MPI_Aint*) ADIOI_Malloc(nprocs*sizeof(MPI_Aint));
    new_types[0] = (MPI_Datatype*) ADIOI_Malloc(local_aggregator_size*sizeof(MPI_Datatype));
    for ( i = 0; i < local_aggregator_size; i++ ){
        j = 0;
        for ( w = 0; w < nprocs; w++ ){
            if (local_aggregators[i] == process_aggregator_list[w] ){
                array_of_blocklengths[j] = recv_size[w];
                MPI_Address(recv_buf[w], array_of_displacements + j);
                j++;
            }
        }
        MPI_Type_create_hindexed(j, array_of_blocklengths, array_of_displacements, MPI_BYTE, new_types[0] + i);
        MPI_Type_commit(new_types[0]+i);
    }
    ADIOI_Free(array_of_blocklengths);
    ADIOI_Free(array_of_displacements);
    return 0;
}

int clean_recv_type(int local_aggregator_size, MPI_Datatype* new_types){
    int i;
    for (i = 0; i < local_aggregator_size; i++){
        MPI_Type_free(new_types + i);
    }
    ADIOI_Free(new_types);
    return 0;
}

/*
  This function reorder the global aggregators in a round robin fashion.  

  Input:
       1. process_node_list: mapping from process to the node index it belongs.
       2. cb_nodes: global aggregator size.
       3. nrecvs: number of physical nodes 
  Output:
       1. ranklist: global aggregators.
*/

int reorder_ranklist(int *process_node_list, int *ranklist, int cb_nodes, int nrecvs){
    int i, j;
    int **node_ranks = (int**) ADIOI_Malloc(sizeof(int*)*nrecvs);
    int *node_size = (int*) ADIOI_Calloc(nrecvs,sizeof(int));
    int *node_index = (int*) ADIOI_Calloc(nrecvs,sizeof(int));

    for ( i = 0; i < cb_nodes; i++ ){
        node_size[process_node_list[ranklist[i]]]++;
    }
    for ( i = 0; i < nrecvs; i++ ){
        node_ranks[i] = (int*) ADIOI_Malloc(sizeof(int)*node_size[i]);
    }
    for ( i = 0; i < cb_nodes; i++ ){
        j = process_node_list[ranklist[i]];
        node_ranks[j][node_index[j]] = ranklist[i];
        node_index[j]++;
    }
    memset( node_index, 0, sizeof(int) * nrecvs );
    j = 0;
    for ( i = 0; i < cb_nodes; i++ ){
        while ( node_index[j] == node_size[j] ) {
            j++;
            if ( j >= nrecvs ) {
                j=0;
            }
        }
        ranklist[i] = node_ranks[j][node_index[j]];
        node_index[j]++;
        j++;
        if ( j >= nrecvs ) {
            j=0;
        }
    }
    for ( i = 0; i < nrecvs; i++ ){
        ADIOI_Free(node_ranks[i]);
    }
    ADIOI_Free(node_ranks);
    ADIOI_Free(node_size);
    ADIOI_Free(node_index);
    return 0;
}

#if DEBUG==1
/*----< main() >-------------------------------------------------------------*/
int main(int argc, char** argv){
    int rank, nprocs, i, blocklen = 0, ntimes = 0, type = 0, rank_assignment = 0, co = 0;
    int* local_ranks, *global_receivers, *node_size, *process_node_list, *global_aggregators, *process_aggregator_list, *local_aggregators, *aggregator_local_ranks, is_aggregator, is_aggregator_new=0;
    int nprocs_node, nrecvs, global_aggregator_size, local_aggregator_size, nprocs_aggregator, iter = 1;
    int *recv_size, *send_size;
    char **recv_buf, **send_buf;
    MPI_Datatype* recv_types;

    /* command-line arguments */
    while ((i = getopt(argc, argv, "hp:b:n:t:r:c:")) != EOF){
        switch(i) {
            case 'p': nprocs_node = atoi(optarg);
                      break;
            case 'b': blocklen = atoi(optarg);
                      break;
            case 'n': ntimes = atoi(optarg);
                      break;
            case 't': type = atoi(optarg);
                      break;
            case 'r': rank_assignment = atoi(optarg);
                      break;
            case 'c': co = atoi(optarg);
                      break;
            case 'h':
            default:  if (rank==0) usage();
                      MPI_Finalize();
                      return 1;
        }
    }
    MPI_Comm comm = MPI_COMM_WORLD, intra_comm;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    //gather_node_information(rank, nprocs, &nprocs_node, &nrecvs, &node_size, &local_ranks, &global_receivers, &process_node_list, comm);
    static_node_assignment(rank, nprocs, rank_assignment, &nprocs_node, &nrecvs, &node_size, &local_ranks, &global_receivers, &process_node_list);
    if ( rank == 0 ){
        printf("blocklen = %d, nprocs_node = %d, rank_assignment = %d, type = %d, co = %d\n",blocklen, nprocs_node, rank_assignment, type, co);
    }
/*
    if (rank==2){
        printf("nrecvs=%d, nprocs_node=%d\n",nrecvs, nprocs_node);
        for (i=0; i < nrecvs; i++){
            printf("global_receivers[%d]=%d, node_size[%d]=%d\n",i,global_receivers[i],i,node_size[i]);
        }
        for (i=0; i < nprocs_node; i++){
            printf("local_ranks[%d]=%d\n",i,local_ranks[i]);
        }
        for (i=0; i < nprocs; i++){
            printf("process_node_list[%d]=%d\n",i,process_node_list[i]);
        }
    }
*/
    initialize_setting(rank, nprocs, local_ranks, global_receivers, nrecvs, blocklen, &recv_size, &send_size, &recv_buf, &send_buf, &global_aggregators, &global_aggregator_size, &is_aggregator, type);
    aggregator_meta_information(rank, process_node_list, nprocs, nrecvs, global_aggregator_size, global_aggregators, co, &is_aggregator_new, &local_aggregator_size, &local_aggregators, &nprocs_aggregator, &aggregator_local_ranks, &process_aggregator_list, 0);
/*
    if ( rank == 0 ){
        printf("total number of aggregators %d\n",local_aggregator_size);
        for ( i = 0; i < local_aggregator_size; i++ ){
            printf("local_aggregators[%d] = %d\n",i, local_aggregators[i]);
        }
        for ( i = 0; i < global_aggregator_size; i++ ){
            printf("global_aggregators[%d] = %d\n",i, global_aggregators[i]);
        }
        for ( i = 0; i < nprocs; i++) {
            printf("process_aggregator_list[%d]=%d, process_node_list[%d]=%d\n",i, process_aggregator_list[i], i, process_node_list[i]);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    printf("rank =%d, is_local_aggregator = %d, is_global_aggregator = %d\n",rank, is_aggregator_new, is_aggregator);
*/
    create_recv_type(nprocs, recv_buf, recv_size, local_aggregators, local_aggregator_size, process_aggregator_list, &recv_types);
    MPI_Comm_split(MPI_COMM_WORLD, process_aggregator_list[rank], rank, &intra_comm);
    /* MPI INFO for RMA*/
    MPI_Info win_info;
    MPI_Info_create(&win_info);
    MPI_Info_set(win_info, "alloc_shared_contig", "true");

/*
    reorder_ranklist(process_node_list, global_aggregators, global_aggregator_size, nrecvs);
    collective_write2(rank, nprocs, nprocs_aggregator, global_aggregator_size, local_aggregator_size, is_aggregator, is_aggregator_new, aggregator_local_ranks, global_aggregators, local_aggregators, process_aggregator_list, recv_size, send_size, recv_types, send_buf, iter, comm);
    clean_recv_type(local_aggregator_size, recv_types);
*/
    collective_write(rank, nprocs, nprocs_node, nrecvs, local_ranks, global_receivers, process_node_list, recv_size, send_size, recv_buf, send_buf, iter, comm);
    //collective_write_benchmark(rank, nprocs, recv_size, send_size, recv_buf, send_buf, 1, comm);
    test_correctness(rank, nprocs, recv_size, recv_buf);
    clean_up(nprocs, &recv_size, &send_size, &recv_buf, &send_buf);
    clean_aggregator_meta(is_aggregator_new, aggregator_local_ranks, local_aggregators, process_aggregator_list);
    MPI_Comm_free(&intra_comm);
    MPI_Finalize();
    return 0;
}
#endif

