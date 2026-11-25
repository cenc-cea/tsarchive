#ifndef TSARCHIVE_IOTDB_PATH_H
#define TSARCHIVE_IOTDB_PATH_H

#include <stddef.h>

int iotdb_format_database_path(char *output, size_t capacity, const char *database);
int iotdb_format_device_path(char *output, size_t capacity,
                             const char *database, const char *kind,
                             const char *network, const char *station,
                             const char *location, const char *channel);

#endif
