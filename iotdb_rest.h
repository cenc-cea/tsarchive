#ifndef TSARCHIVE_IOTDB_REST_H
#define TSARCHIVE_IOTDB_REST_H

#include <curl/curl.h>

typedef struct {
    CURL *curl;
    char base_url[1024];
    const char *username;
    const char *password;
    long timeout_ms;
    char error[256];
} IotdbClient;

int iotdb_client_init(IotdbClient *client, const char *base_url,
                      const char *username, const char *password,
                      long timeout_ms);
int iotdb_execute_sql(IotdbClient *client, const char *sql);
int iotdb_create_database(IotdbClient *client, const char *sql);
int iotdb_create_timeseries(IotdbClient *client, const char *sql);
void iotdb_client_close(IotdbClient *client);
const char *iotdb_client_error(const IotdbClient *client);

#endif
