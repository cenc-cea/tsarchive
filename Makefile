# Copyright (c) 2017 by TAOS Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=.
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver' -ltaos -lpthread -lm -lrt -L libmseed -lmseed -lsasl2 -L librdkafka/dist/lib -lrdkafka
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion -Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX -msse4.2 -Wno-unused-function -D_M_X64 -std=gnu99 -I libmseed -I librdkafka/dist/include

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./tsarchive.c -o $(ROOT)/tsarchive $(LFLAGS)
	gcc $(CFLAGS) ./create_table.c -o $(ROOT)/create_table $(LFLAGS)

clean:
	rm $(ROOT)/tsarchive $(ROOT)/create_table
	
	
