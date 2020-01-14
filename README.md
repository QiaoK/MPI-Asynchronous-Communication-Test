The program [./test](mpi_test.c) runs the many-to-all and all-to-many communication among MPI processes.
The many group spreads out across all process ranks.

* Compile command:
  * Edit file [./Makefile](Makefile) to customize the C compiler and compile
    options.
  * Run command `make` to compile and generate the executable program named
    `test`.

* Run command:
  * Command-line options:
    ```
      % ./test -h
	Usage: ./test [OPTION]... [FILE]...
           [-h] Print help
           [-a] number of aggregators (in the context of ROMIO)
           [-p] number of processes per node (does not really matter)
           [-d] data size
           [-c] maximum communication size
           [-i] number of experiments (MPI barrier between experiments)
           [-k] number of iteration (run methods many times, there is no sync between individual runs)
           [-m] method
               0: All experiments
               1: All to many without ordering (all-to-many)
               2: Many to all without ordering (many-to-all)
               3: All to many with ordering (all-to-many)
               4: Many to all with ordering (many-to-all)
               5: Many to all with alltoallw (many-to-all)
               6: All to many sync (all-to-many sync)
               7: All to many half sync (all-to-many half sync)
               8: All to many with alltoallw (all-to-many benchmark)
               9: All to many pairwise (all-to-many pairwise)
               10: Many to all pairwise (many-to-all pairwise)
               11: Many to all half sync (many-to-all half sync)
              12: Many to all half sync2 (many-to-all half sync2)
    ```
* Example outputs on screen
  * Running both all-to-many and many-to-all for two times. The many group has 14 processes. The data size is 2KB. Maximum communication size 3.
  ```
    % mpiexec -n 32 ./test -a 14 -p 1 -d 2048 -c 3 -m 0 -i 2 -k 1
	total number of processes = 32, cb_nodes = 14, data size = 2048, comm_size = 3
	aggregators = 0, 3, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 
	| --------------------------------------
	| All-to-many rank 0 request post time = 0.001556
	| All-to-many rank 0 waitall time = 0.022929
	| All-to-many rank 0 total time = 0.024494
	| All-to-many max request post time = 0.011989
	| All-to-many max waitall time = 0.045943
	| All-to-many max total time = 0.055115
	| --------------------------------------
	| Many-to-all rank 0 request post time = 0.000075
	| Many-to-all rank 0 waitall time = 0.053334
	| Many-to-all rank 0 total time = 0.053339
	| Many-to-all max request post time = 0.000167
	| Many-to-all max waitall time = 0.080153
	| Many-to-all max total time = 0.080153
	| --------------------------------------
	| --------------------------------------
	| All-to-many rank 0 request post time = 0.000059
	| All-to-many rank 0 waitall time = 0.029737
	| All-to-many rank 0 total time = 0.029803
	| All-to-many max request post time = 0.000066
	| All-to-many max waitall time = 0.029737
	| All-to-many max total time = 0.029803
	| --------------------------------------
	| Many-to-all rank 0 request post time = 0.000049
	| Many-to-all rank 0 waitall time = 0.093742
	| Many-to-all rank 0 total time = 0.093747
	| Many-to-all max request post time = 0.000073
	| Many-to-all max waitall time = 0.093742
	| Many-to-all max total time = 0.093747
	| --------------------------------------
  ```

## Questions/Comments:
email: qiao.kang@eecs.northwestern.edu

Copyright (C) 2019, Northwestern University.

See [COPYRIGHT](COPYRIGHT) notice in top-level directory.
```
