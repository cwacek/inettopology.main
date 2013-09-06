CC = g++
GTESTDIR = gtest
TESTS = $(wildcard test_*.cc)
OBJECTS = $(patsubst %.cc, %.o, $(wildcard *.cc))
CFLAGS = -I$(GTESTDIR)/include -std=c++0x
LIBS = $(GTESTDIR)/libgtest.a ../../hiredis/libhiredis.a -lpthread

%.o: %.cc $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

test: $(OBJECTS) 
	$(CC) -I$(GTESTDIR)/include $(OBJECTS) ../libasinfer.a $(LIBS) -o $@

clean:
	-rm -f *.o
	-rm -f $(TARGET)              

.PHONY: test
