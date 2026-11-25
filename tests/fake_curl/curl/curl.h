#ifndef TSARCHIVE_TEST_CURL_H
#define TSARCHIVE_TEST_CURL_H

#include <stddef.h>

typedef struct CurlHandle CURL;
typedef int CURLcode;
typedef int CURLoption;
typedef int CURLINFO;
typedef size_t (*curl_write_callback)(char *data, size_t size, size_t count, void *opaque);

struct curl_slist {
    char *data;
    struct curl_slist *next;
};

#define CURLE_OK 0
#define CURLE_FAILED_INIT 2
#define CURL_GLOBAL_DEFAULT 3L
#define CURLAUTH_BASIC 1L
#define CURL_ERROR_SIZE 256
#define CURLINFO_RESPONSE_CODE 0x200002
#define CURLOPT_URL 10002
#define CURLOPT_HTTPAUTH 107
#define CURLOPT_USERNAME 10018
#define CURLOPT_PASSWORD 10005
#define CURLOPT_POST 47
#define CURLOPT_POSTFIELDS 10015
#define CURLOPT_POSTFIELDSIZE 60
#define CURLOPT_HTTPHEADER 10023
#define CURLOPT_WRITEFUNCTION 20011
#define CURLOPT_WRITEDATA 10001
#define CURLOPT_ERRORBUFFER 10010
#define CURLOPT_CONNECTTIMEOUT_MS 156
#define CURLOPT_TIMEOUT_MS 155
#define CURLOPT_NOSIGNAL 99
#define CURLOPT_TCP_KEEPALIVE 213

int curl_global_init(long flags);
CURL *curl_easy_init(void);
void curl_easy_cleanup(CURL *curl);
void curl_easy_reset(CURL *curl);
CURLcode curl_easy_setopt(CURL *curl, CURLoption option, ...);
CURLcode curl_easy_perform(CURL *curl);
CURLcode curl_easy_getinfo(CURL *curl, CURLINFO info, ...);
const char *curl_easy_strerror(CURLcode code);
struct curl_slist *curl_slist_append(struct curl_slist *list, const char *value);
void curl_slist_free_all(struct curl_slist *list);

void test_curl_set_response(long http_status, const char *body, CURLcode transport_result);
const char *test_curl_url(void);
const char *test_curl_request_body(void);
const char *test_curl_username(void);
const char *test_curl_password(void);
long test_curl_timeout(void);

#endif
