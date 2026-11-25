#include "curl/curl.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct CurlHandle {
    char url[1200];
    char username[256];
    char password[256];
    char request_body[4096];
    long request_body_length;
    long timeout_ms;
    curl_write_callback write_callback;
    void *write_data;
    char *error_buffer;
};

static long configured_http_status = 200;
static char configured_response[4096] = "{\"code\":200,\"message\":\"SUCCESS\"}";
static CURLcode configured_transport_result = CURLE_OK;
static char last_url[1200];
static char last_body[4096];
static char last_username[256];
static char last_password[256];
static long last_timeout;

int curl_global_init(long flags)
{
    (void)flags;
    return CURLE_OK;
}

CURL *curl_easy_init(void)
{
    return (CURL *)calloc(1, sizeof(CURL));
}

void curl_easy_cleanup(CURL *curl)
{
    free(curl);
}

void curl_easy_reset(CURL *curl)
{
    memset(curl, 0, sizeof(*curl));
}

CURLcode curl_easy_setopt(CURL *curl, CURLoption option, ...)
{
    va_list values;
    va_start(values, option);
    switch (option) {
    case CURLOPT_URL:
        snprintf(curl->url, sizeof(curl->url), "%s", va_arg(values, const char *));
        break;
    case CURLOPT_USERNAME:
        snprintf(curl->username, sizeof(curl->username), "%s", va_arg(values, const char *));
        break;
    case CURLOPT_PASSWORD:
        snprintf(curl->password, sizeof(curl->password), "%s", va_arg(values, const char *));
        break;
    case CURLOPT_POSTFIELDS:
        snprintf(curl->request_body, sizeof(curl->request_body), "%s", va_arg(values, const char *));
        break;
    case CURLOPT_POSTFIELDSIZE:
        curl->request_body_length = va_arg(values, long);
        break;
    case CURLOPT_TIMEOUT_MS:
        curl->timeout_ms = va_arg(values, long);
        break;
    case CURLOPT_HTTPAUTH:
    case CURLOPT_POST:
    case CURLOPT_CONNECTTIMEOUT_MS:
    case CURLOPT_NOSIGNAL:
    case CURLOPT_TCP_KEEPALIVE:
        (void)va_arg(values, long);
        break;
    case CURLOPT_HTTPHEADER:
        (void)va_arg(values, struct curl_slist *);
        break;
    case CURLOPT_WRITEFUNCTION:
        curl->write_callback = va_arg(values, curl_write_callback);
        break;
    case CURLOPT_WRITEDATA:
        curl->write_data = va_arg(values, void *);
        break;
    case CURLOPT_ERRORBUFFER:
        curl->error_buffer = va_arg(values, char *);
        break;
    default:
        (void)va_arg(values, void *);
        break;
    }
    va_end(values);
    return CURLE_OK;
}

CURLcode curl_easy_perform(CURL *curl)
{
    size_t response_length = strlen(configured_response);
    if (curl->request_body_length < 0 || (size_t)curl->request_body_length >= sizeof(last_body))
        return CURLE_FAILED_INIT;
    memcpy(last_body, curl->request_body, (size_t)curl->request_body_length);
    last_body[curl->request_body_length] = '\0';
    snprintf(last_url, sizeof(last_url), "%s", curl->url);
    snprintf(last_username, sizeof(last_username), "%s", curl->username);
    snprintf(last_password, sizeof(last_password), "%s", curl->password);
    last_timeout = curl->timeout_ms;
    if (configured_transport_result != CURLE_OK) {
        if (curl->error_buffer != NULL)
            snprintf(curl->error_buffer, CURL_ERROR_SIZE, "mock transport failure");
        return configured_transport_result;
    }
    if (curl->write_callback != NULL)
        curl->write_callback(configured_response, 1, response_length, curl->write_data);
    return CURLE_OK;
}

CURLcode curl_easy_getinfo(CURL *curl, CURLINFO info, ...)
{
    va_list values;
    (void)curl;
    va_start(values, info);
    if (info == CURLINFO_RESPONSE_CODE) {
        long *status = va_arg(values, long *);
        *status = configured_http_status;
    }
    va_end(values);
    return CURLE_OK;
}

const char *curl_easy_strerror(CURLcode code)
{
    (void)code;
    return "mock curl error";
}

struct curl_slist *curl_slist_append(struct curl_slist *list, const char *value)
{
    struct curl_slist *item = (struct curl_slist *)calloc(1, sizeof(*item));
    struct curl_slist *last;
    if (item == NULL)
        return NULL;
    item->data = (char *)malloc(strlen(value) + 1);
    if (item->data == NULL) {
        free(item);
        return NULL;
    }
    strcpy(item->data, value);
    if (list == NULL)
        return item;
    for (last = list; last->next != NULL; last = last->next) {}
    last->next = item;
    return list;
}

void curl_slist_free_all(struct curl_slist *list)
{
    while (list != NULL) {
        struct curl_slist *next = list->next;
        free(list->data);
        free(list);
        list = next;
    }
}

void test_curl_set_response(long http_status, const char *body, CURLcode transport_result)
{
    configured_http_status = http_status;
    configured_transport_result = transport_result;
    snprintf(configured_response, sizeof(configured_response), "%s", body);
}

const char *test_curl_url(void) { return last_url; }
const char *test_curl_request_body(void) { return last_body; }
const char *test_curl_username(void) { return last_username; }
const char *test_curl_password(void) { return last_password; }
long test_curl_timeout(void) { return last_timeout; }
