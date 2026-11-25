#include "iotdb_rest.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RESPONSE_CAPACITY 4096

typedef struct {
    char body[RESPONSE_CAPACITY];
    size_t length;
} ResponseBuffer;

static size_t append_response(char *data, size_t size, size_t count, void *opaque)
{
    ResponseBuffer *response = (ResponseBuffer *)opaque;
    size_t bytes = size * count;

    if (bytes > sizeof(response->body) - response->length - 1)
        return 0;
    memcpy(response->body + response->length, data, bytes);
    response->length += bytes;
    response->body[response->length] = '\0';
    return bytes;
}

static char *sql_json_body(const char *sql)
{
    size_t length = strlen(sql);
    char *body;
    char *out;
    const unsigned char *input;

    if (length > (SIZE_MAX - 12) / 6)
        return NULL;
    body = (char *)malloc(length * 6 + 12);
    if (body == NULL)
        return NULL;
    out = body;
    out += sprintf(out, "{\"sql\":\"");
    for (input = (const unsigned char *)sql; *input != '\0'; input++) {
        switch (*input) {
        case '"':
        case '\\':
            *out++ = '\\';
            *out++ = (char)*input;
            break;
        case '\b': *out++ = '\\'; *out++ = 'b'; break;
        case '\f': *out++ = '\\'; *out++ = 'f'; break;
        case '\n': *out++ = '\\'; *out++ = 'n'; break;
        case '\r': *out++ = '\\'; *out++ = 'r'; break;
        case '\t': *out++ = '\\'; *out++ = 't'; break;
        default:
            if (*input < 0x20) {
                sprintf(out, "\\u%04x", *input);
                out += 6;
            } else {
                *out++ = (char)*input;
            }
        }
    }
    *out++ = '"';
    *out++ = '}';
    *out = '\0';
    return body;
}

static int response_code(const char *body, long *code)
{
    const char *field = strstr(body, "\"code\"");
    const char *colon;
    char *end;

    if (field == NULL || (colon = strchr(field, ':')) == NULL)
        return 0;
    colon++;
    while (isspace((unsigned char)*colon))
        colon++;
    *code = strtol(colon, &end, 10);
    return end != colon;
}

int iotdb_client_init(IotdbClient *client, const char *base_url,
                      const char *username, const char *password,
                      long timeout_ms)
{
    size_t length;

    if (client == NULL || base_url == NULL || username == NULL || password == NULL ||
        username[0] == '\0' || password[0] == '\0' || timeout_ms <= 0) {
        return -1;
    }
    length = strlen(base_url);
    if (length == 0 || length >= sizeof(client->base_url) ||
        (strncmp(base_url, "http://", 7) != 0 && strncmp(base_url, "https://", 8) != 0)) {
        return -1;
    }
    if ((length == 7 && strncmp(base_url, "http://", 7) == 0) ||
        (length == 8 && strncmp(base_url, "https://", 8) == 0))
        return -1;

    memset(client, 0, sizeof(*client));
    while (length > 0 && base_url[length - 1] == '/')
        length--;
    memcpy(client->base_url, base_url, length);
    client->base_url[length] = '\0';
    client->username = username;
    client->password = password;
    client->timeout_ms = timeout_ms;
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
        snprintf(client->error, sizeof(client->error), "could not initialize libcurl");
        return -1;
    }
    client->curl = curl_easy_init();
    if (client->curl == NULL) {
        snprintf(client->error, sizeof(client->error), "could not create HTTP client");
        return -1;
    }
    curl_easy_setopt(client->curl, CURLOPT_CONNECTTIMEOUT_MS, timeout_ms);
    curl_easy_setopt(client->curl, CURLOPT_TIMEOUT_MS, timeout_ms);
    curl_easy_setopt(client->curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(client->curl, CURLOPT_TCP_KEEPALIVE, 1L);
    return 0;
}

static int execute(IotdbClient *client, const char *sql, long duplicate_code)
{
    char url[sizeof(client->base_url) + 32];
    char curl_error[CURL_ERROR_SIZE] = {0};
    char *body;
    long http_status = 0;
    long application_status = 0;
    int has_application_status;
    CURLcode result;
    struct curl_slist *headers = NULL;
    ResponseBuffer response;

    if (client == NULL || client->curl == NULL || sql == NULL || sql[0] == '\0')
        return -1;
    if (strlen(client->base_url) + sizeof("/rest/v1/nonQuery") > sizeof(url)) {
        snprintf(client->error, sizeof(client->error), "IoTDB REST URL is too long");
        return -1;
    }
    body = sql_json_body(sql);
    if (body == NULL) {
        snprintf(client->error, sizeof(client->error), "could not allocate SQL request body");
        return -1;
    }
    memset(&response, 0, sizeof(response));
    snprintf(url, sizeof(url), "%s/rest/v1/nonQuery", client->base_url);
    headers = curl_slist_append(headers, "Content-Type: application/json");
    if (headers == NULL) {
        free(body);
        snprintf(client->error, sizeof(client->error), "could not allocate HTTP headers");
        return -1;
    }

    curl_easy_reset(client->curl);
    curl_easy_setopt(client->curl, CURLOPT_URL, url);
    curl_easy_setopt(client->curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
    curl_easy_setopt(client->curl, CURLOPT_USERNAME, client->username);
    curl_easy_setopt(client->curl, CURLOPT_PASSWORD, client->password);
    curl_easy_setopt(client->curl, CURLOPT_POST, 1L);
    curl_easy_setopt(client->curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(client->curl, CURLOPT_POSTFIELDSIZE, (long)strlen(body));
    curl_easy_setopt(client->curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(client->curl, CURLOPT_WRITEFUNCTION, append_response);
    curl_easy_setopt(client->curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(client->curl, CURLOPT_ERRORBUFFER, curl_error);
    curl_easy_setopt(client->curl, CURLOPT_CONNECTTIMEOUT_MS, client->timeout_ms);
    curl_easy_setopt(client->curl, CURLOPT_TIMEOUT_MS, client->timeout_ms);
    curl_easy_setopt(client->curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(client->curl, CURLOPT_TCP_KEEPALIVE, 1L);

    result = curl_easy_perform(client->curl);
    if (result == CURLE_OK)
        curl_easy_getinfo(client->curl, CURLINFO_RESPONSE_CODE, &http_status);
    has_application_status = result == CURLE_OK && response_code(response.body, &application_status);
    if (result != CURLE_OK || http_status < 200 || http_status >= 300 ||
        !has_application_status || (application_status != 200 && application_status != duplicate_code)) {
        if (result != CURLE_OK) {
            snprintf(client->error, sizeof(client->error), "IoTDB REST transport failed: %.220s",
                     curl_error[0] == '\0' ? curl_easy_strerror(result) : curl_error);
        } else if (has_application_status) {
            snprintf(client->error, sizeof(client->error),
                     "IoTDB REST request failed: HTTP %ld, code %ld", http_status,
                     application_status);
        } else {
            snprintf(client->error, sizeof(client->error),
                     "IoTDB REST request failed: HTTP %ld, missing response code", http_status);
        }
        curl_slist_free_all(headers);
        free(body);
        return -1;
    }
    client->error[0] = '\0';
    curl_slist_free_all(headers);
    free(body);
    return 0;
}

int iotdb_execute_sql(IotdbClient *client, const char *sql)
{
    return execute(client, sql, -1);
}

int iotdb_create_database(IotdbClient *client, const char *sql)
{
    return execute(client, sql, 501); /* DATABASE_ALREADY_EXISTS */
}

int iotdb_create_timeseries(IotdbClient *client, const char *sql)
{
    return execute(client, sql, 503); /* PATH_ALREADY_EXIST */
}

void iotdb_client_close(IotdbClient *client)
{
    if (client != NULL && client->curl != NULL) {
        curl_easy_cleanup(client->curl);
        client->curl = NULL;
    }
}

const char *iotdb_client_error(const IotdbClient *client)
{
    return client == NULL || client->error[0] == '\0' ? "IoTDB request failed" : client->error;
}
