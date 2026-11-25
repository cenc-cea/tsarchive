# Copyright (c) 2017 by TAOS Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=.
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/taos/driver' -ltaos -lpthread -lm -lrt -L libmseed -lmseed -lsasl2 -L librdkafka/dist/lib -lrdkafka
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion -Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX -msse4.2 -Wno-unused-function -D_M_X64 -std=gnu99 -I libmseed -I librdkafka/dist/include
IOTDB_CFLAGS ?= $(shell pkg-config --cflags libcurl 2>/dev/null)
IOTDB_LIBS ?= $(shell pkg-config --libs libcurl 2>/dev/null)
IOTDB_MSEED_LIB ?= -Llibmseed -lmseed
IOTDB_KAFKA_LIB ?= $(shell pkg-config --libs rdkafka 2>/dev/null || echo -Llibrdkafka/dist/lib -lrdkafka)
ifneq ($(strip $(IOTDB_LIBS)),)
else
IOTDB_CFLAGS = $(shell curl-config --cflags 2>/dev/null)
IOTDB_LIBS = $(shell curl-config --libs 2>/dev/null)
endif

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./tsarchive.c -o $(ROOT)/tsarchive $(LFLAGS)
	gcc $(CFLAGS) ./create_table.c -o $(ROOT)/create_table $(LFLAGS)

iotdb:
	@test -n "$(IOTDB_LIBS)" || (echo "libcurl development files are required (pkg-config/curl-config was not found)"; exit 1)
	$(CC) $(CFLAGS) -DIOTDB_BACKEND $(IOTDB_CFLAGS) ./tsarchive.c ./iotdb_rest.c ./iotdb_path.c -o $(ROOT)/tsarchive_iotdb $(IOTDB_MSEED_LIB) $(IOTDB_KAFKA_LIB) $(IOTDB_LIBS) -lpthread -lm -lrt
	gcc $(CFLAGS) $(IOTDB_CFLAGS) ./create_table_iotdb.c ./iotdb_rest.c ./iotdb_path.c -o $(ROOT)/create_table_iotdb $(IOTDB_LIBS)

test-iotdb:
	gcc -std=gnu99 -Wall -Wextra -Werror -I. ./tests/test_iotdb_path.c ./iotdb_path.c -o $(ROOT)/test_iotdb_path
	./test_iotdb_path
	rm -f ./test_iotdb_path
	gcc -std=gnu99 -Wall -Wextra -Werror -I./tests/fake_curl -I. ./tests/test_iotdb_rest.c ./iotdb_rest.c ./tests/fake_curl/fake_curl.c -o $(ROOT)/test_iotdb_rest
	./test_iotdb_rest
	rm -f ./test_iotdb_rest

clean:
	rm -f $(ROOT)/tsarchive $(ROOT)/create_table $(ROOT)/tsarchive_iotdb $(ROOT)/create_table_iotdb $(ROOT)/test_iotdb_path
	
	
