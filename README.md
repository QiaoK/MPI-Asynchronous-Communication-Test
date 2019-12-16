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
		[-d] data size
		[-c] maximum communication size
		[-i] number of iteration
		[-m] method
			0: Both 1 and 2
			1: All processes to c receivers
			2: c processes to all processes
    ```
* Example outputs on screen
  * Running both all-to-many and many-to-all for two times. The many group has 14 processes. The data size is 2KB. Maximum communication size 3.
  ```
    % mpiexec -n 32 ./test -a 14 -d 2048 -c 3 -m 0 -i 2
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
