TARGET = as_infer
LIBS = -L../hiredis/ -lhiredis -L/usr/local/lib64
SRC = infer.cc logger.cc structures.cc
CMD_SRC = cmdline.cc main.cc
CC = g++
CFLAGS = -g -Wall -std=c++0x 
PROD_CFLAGS = -g -Wall -std=c++0x -O2
TESTDIR = test

.PHONY: default all clean cmdline test lib

default: $(TARGET)
all: default

cmdline.cc: opts.ggo
	gengetopt -c cc < opts.ggo

OBJECTS = $(patsubst %.cc, %.o, $(SRC))
HEADERS = $(wildcard *.h)

%.o: %.cc $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

.PRECIOUS: $(TARGET) $(OBJECTS)

$(TARGET): $(CMD_SRC) $(OBJECTS)
	$(CC) $(CFLAGS) -c $(CMD_SRC)
	$(CC) $(CFLAGS) $(OBJECTS) $(patsubst %.cc, %.o, $(CMD_SRC)) -pg -Wall $(LIBS) -o $@

production: 
	$(MAKE) CFLAGS="$(PROD_CFLAGS)" $(TARGET)
	#$(CC) $(PROD_CFLAGS) -c $(CMD_SRC)
	#$(CC) $(PROD_CFLAGS) $(OBJECTS) $(patsubst %.cc, %.o, $(CMD_SRC)) -Wall $(LIBS) -o $(TARGET)

lib: $(OBJECTS)
	ar -rv libasinfer.a $(OBJECTS)

clean:
	-rm -f *.o
	-rm -f $(TARGET)              
	-rm -f libasinfer.a

test: lib
	$(MAKE) -C $(TESTDIR)
	test/test


