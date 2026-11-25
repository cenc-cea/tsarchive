#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../iotdb_rest.h"
#include "fake_curl/curl/curl.h"

int main(void)
{
    IotdbClient client;

    assert(iotdb_client_init(&client, "https://iotdb.example/", "user", "secret", 2500) == 0);
    test_curl_set_response(200, "{\"code\":200,\"message\":\"SUCCESS\"}", CURLE_OK);
    assert(iotdb_execute_sql(&client, "INSERT INTO root.db.d(timestamp, s) VALUES (1, \"a\\b\\n\")") == 0);
    assert(strcmp(test_curl_url(), "https://iotdb.example/rest/v1/nonQuery") == 0);
    assert(strcmp(test_curl_username(), "user") == 0);
    assert(strcmp(test_curl_password(), "secret") == 0);
    assert(test_curl_timeout() == 2500);
    assert(strcmp(test_curl_request_body(),
                  "{\"sql\":\"INSERT INTO root.db.d(timestamp, s) VALUES (1, \\\"a\\\\b\\\\n\\\")\"}") == 0);

    test_curl_set_response(200, "{\"code\":305,\"message\":\"service unavailable\"}", CURLE_OK);
    assert(iotdb_execute_sql(&client, "SHOW DATABASES") == -1);
    assert(strstr(iotdb_client_error(&client), "code 305") != NULL);
    assert(strstr(iotdb_client_error(&client), "secret") == NULL);

    test_curl_set_response(503, "{\"code\":305}", CURLE_OK);
    assert(iotdb_execute_sql(&client, "SHOW DATABASES") == -1);
    assert(strstr(iotdb_client_error(&client), "HTTP 503") != NULL);

    test_curl_set_response(200, "{}", CURLE_OK);
    assert(iotdb_execute_sql(&client, "SHOW DATABASES") == -1);
    assert(strstr(iotdb_client_error(&client), "missing response code") != NULL);

    test_curl_set_response(0, "", CURLE_FAILED_INIT);
    assert(iotdb_execute_sql(&client, "SHOW DATABASES") == -1);
    assert(strstr(iotdb_client_error(&client), "transport failed") != NULL);
    iotdb_client_close(&client);

    assert(iotdb_client_init(&client, "file:///tmp/db", "user", "secret", 2500) == -1);
    assert(iotdb_client_init(&client, "https://iotdb.example", "", "secret", 2500) == -1);
    puts("IoTDB REST tests passed");
    return 0;
}
