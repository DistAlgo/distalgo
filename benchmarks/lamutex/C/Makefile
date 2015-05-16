
CC=gcc
CFLAGS=-O2 -std=c99 -fms-extensions
LDFLAGS=-lrt

all : lamport
	strip lamport

lamport : lamport.o pqueue.o

clean :
	rm -f *.o lamport

.PHONY : all clean
