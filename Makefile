CC=mpicc
CFLAGS= -Wall -Wextra -O2
LIBS = -lm
TEST_OBJS = mpi_test.o
test : $(TEST_OBJS)
	$(CC) -o $@ $(TEST_OBJS) $(LIBS)
%.o: %.c
	$(CC) $(CFLAGS) -c $<  
clean:
	rm -rf *.o
	rm -rf test
