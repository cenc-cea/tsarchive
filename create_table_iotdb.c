#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "iotdb_path.h"
#include "iotdb_rest.h"

#define LINE_CAPACITY 1024
#define PATH_CAPACITY 1024

static int ensure_channel_schema(IotdbClient *client, const char *database,
                                 const char *network, const char *station,
                                 const char *location, const char *channel)
{
    char device[PATH_CAPACITY];
    char sql[PATH_CAPACITY + 1024];
    int written;

    if (!iotdb_format_device_path(device, sizeof(device), database, "waveform",
                                  network, station, location, channel)) {
        fprintf(stderr, "invalid channel identifier in table list\n");
        return -1;
    }
    written = snprintf(sql, sizeof(sql),
                       "CREATE TIMESERIES %s.value WITH DATATYPE=INT32, ENCODING=TS_2DIFF",
                       device);
    if (written < 0 || (size_t)written >= sizeof(sql) || iotdb_create_timeseries(client, sql) != 0) {
        fprintf(stderr, "failed to create waveform schema: %s\n", iotdb_client_error(client));
        return -1;
    }

    if (!iotdb_format_device_path(device, sizeof(device), database, "metadata",
                                  network, station, location, channel)) {
        fprintf(stderr, "invalid channel identifier in table list\n");
        return -1;
    }
    written = snprintf(sql, sizeof(sql),
                       "CREATE TIMESERIES %s.sequence_number WITH DATATYPE=INT32, ENCODING=RLE",
                       device);
    if (written < 0 || (size_t)written >= sizeof(sql) || iotdb_create_timeseries(client, sql) != 0)
        goto metadata_error;
    written = snprintf(sql, sizeof(sql),
                       "CREATE TIMESERIES %s.starttime WITH DATATYPE=INT64, ENCODING=TS_2DIFF",
                       device);
    if (written < 0 || (size_t)written >= sizeof(sql) || iotdb_create_timeseries(client, sql) != 0)
        goto metadata_error;
    written = snprintf(sql, sizeof(sql),
                       "CREATE TIMESERIES %s.endtime WITH DATATYPE=INT64, ENCODING=TS_2DIFF",
                       device);
    if (written < 0 || (size_t)written >= sizeof(sql) || iotdb_create_timeseries(client, sql) != 0)
        goto metadata_error;
    written = snprintf(sql, sizeof(sql),
                       "CREATE TIMESERIES %s.npts WITH DATATYPE=INT64, ENCODING=RLE",
                       device);
    if (written < 0 || (size_t)written >= sizeof(sql) || iotdb_create_timeseries(client, sql) != 0)
        goto metadata_error;
    written = snprintf(sql, sizeof(sql),
                       "CREATE TIMESERIES %s.samprate WITH DATATYPE=DOUBLE, ENCODING=GORILLA",
                       device);
    if (written < 0 || (size_t)written >= sizeof(sql) || iotdb_create_timeseries(client, sql) != 0)
        goto metadata_error;
    return 0;

metadata_error:
    fprintf(stderr, "failed to create metadata schema: %s\n", iotdb_client_error(client));
    return -1;
}

int main(int argc, char **argv)
{
    IotdbClient client;
    char database_path[256];
    char line[LINE_CAPACITY];
    char network[128];
    char station[128];
    char location[128];
    char channel[128];
    char sql[512];
    FILE *input;
    size_t line_number = 0;
    int timeout_ms = 10000;

    if (argc != 4) {
        fprintf(stderr, "Usage: %s iotdb_rest_url database tables_list.txt\n", argv[0]);
        return 2;
    }
    if (!iotdb_format_database_path(database_path, sizeof(database_path), argv[2])) {
        fprintf(stderr, "IoTDB database must be a simple path node such as tsarchive\n");
        return 2;
    }
    input = fopen(argv[3], "r");
    if (input == NULL) {
        perror("could not open channel list");
        return 1;
    }
    {
        const char *configured_timeout = getenv("IOTDB_TIMEOUT_MS");
        if (configured_timeout != NULL && configured_timeout[0] != '\0') {
            char *end;
            long parsed = strtol(configured_timeout, &end, 10);
            if (end == configured_timeout || *end != '\0' || parsed <= 0 || parsed > 600000) {
                fprintf(stderr, "IOTDB_TIMEOUT_MS must be between 1 and 600000\n");
                fclose(input);
                return 2;
            }
            timeout_ms = (int)parsed;
        }
    }
    if (iotdb_client_init(&client, argv[1], getenv("IOTDB_USERNAME"),
                          getenv("IOTDB_PASSWORD"), timeout_ms) != 0) {
        fprintf(stderr, "invalid IoTDB REST configuration; set IOTDB_USERNAME and IOTDB_PASSWORD\n");
        fclose(input);
        return 2;
    }
    snprintf(sql, sizeof(sql), "CREATE DATABASE %s", database_path);
    if (iotdb_create_database(&client, sql) != 0) {
        fprintf(stderr, "failed to initialize IoTDB database: %s\n", iotdb_client_error(&client));
        iotdb_client_close(&client);
        fclose(input);
        return 1;
    }

    while (fgets(line, sizeof(line), input) != NULL) {
        int fields;
        line_number++;
        if (line[0] == '\n' || line[0] == '\r' || line[0] == '\0' || line[0] == '#')
            continue;
        fields = sscanf(line, "%127[^.].%127[^.].%127[^.].%127s",
                        network, station, location, channel);
        if (fields != 4) {
            fprintf(stderr, "invalid channel entry on line %lu\n", (unsigned long)line_number);
            iotdb_client_close(&client);
            fclose(input);
            return 1;
        }
        if (strcmp(location, "-") == 0 || strcmp(location, "--") == 0)
            location[0] = '\0';
        if (ensure_channel_schema(&client, argv[2], network, station, location, channel) != 0) {
            fprintf(stderr, "failed to initialize channel on line %lu\n", (unsigned long)line_number);
            iotdb_client_close(&client);
            fclose(input);
            return 1;
        }
        printf("initialized %s.%s.%s.%s\n", network, station,
               location[0] == '\0' ? "-" : location, channel);
    }
    if (ferror(input)) {
        perror("failed while reading channel list");
        iotdb_client_close(&client);
        fclose(input);
        return 1;
    }
    fclose(input);
    iotdb_client_close(&client);
    return 0;
}
