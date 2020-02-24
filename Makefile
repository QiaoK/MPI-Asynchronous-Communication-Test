CC=mpicc
CFLAGS= -Wall -Wextra -O2
LIBS = -lm
TEST_SENDRECV_OBJS = mpi_sendrecv_test.o
TEST_OBJS = mpi_test.o lustre_driver_test.o
test : $(TEST_OBJS)
	$(CC) -o $@ $(TEST_OBJS) $(LIBS)
pt2pt_test : $(TEST_SENDRECV_OBJS)
	$(CC) -o $@ $(TEST_SENDRECV_OBJS) $(LIBS)
%.o: %.c
	$(CC) $(CFLAGS) -c $<  
clean:
	rm -rf *.o
	rm -rf test
