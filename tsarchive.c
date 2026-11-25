/*
 * Written by Yanlu Ma, 2019-Dec-20 at CENC, Beijing.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#ifdef IOTDB_BACKEND
#include <inttypes.h>
#include "iotdb_path.h"
#include "iotdb_rest.h"
#else
#include <taos.h>
#endif
#include "libmseed.h"
#include "librdkafka/rdkafka.h"

static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque);

#ifdef IOTDB_BACKEND
#define IOTDB_SAMPLE_BATCH 128

static int insert_iotdb_record(IotdbClient *client, const char *database,
                               const MSRecord *msr, int64_t ingesttime,
                               int64_t starttime, const int32_t *samples,
                               int64_t sample_count)
{
    char metadata_path[1024];
    char waveform_path[1024];
    char sql[16384];
    int64_t sample_index;
    int64_t endtime;
    int written;

    if (sample_count <= 0 || samples == NULL || msr->sampletype != 'i') {
        fprintf(stderr, "IoTDB sink requires non-empty INT32 miniSEED samples\n");
        return -1;
    }
    if (!isfinite(msr->samprate) || msr->samprate <= 0.0 || msr->samprate > 1000.0) {
        fprintf(stderr, "unsupported sample rate for IoTDB millisecond timestamps\n");
        return -1;
    }
    if (!iotdb_format_device_path(metadata_path, sizeof(metadata_path), database,
                                  "metadata", msr->network, msr->station,
                                  msr->location, msr->channel) ||
        !iotdb_format_device_path(waveform_path, sizeof(waveform_path), database,
                                  "waveform", msr->network, msr->station,
                                  msr->location, msr->channel)) {
        fprintf(stderr, "invalid or oversized miniSEED channel path\n");
        return -1;
    }

    endtime = starttime + (int64_t)llround((double)(sample_count - 1) * 1000.0 / msr->samprate);
    written = snprintf(sql, sizeof(sql),
                       "INSERT INTO %s(timestamp, sequence_number, starttime, endtime, npts, samprate) "
                       "VALUES (%" PRId64 ", %d, %" PRId64 ", %" PRId64 ", %" PRId64 ", %.9g)",
                       metadata_path, ingesttime, (int)msr->sequence_number,
                       starttime, endtime, sample_count, msr->samprate);
    if (written < 0 || (size_t)written >= sizeof(sql)) {
        fprintf(stderr, "IoTDB metadata statement exceeded its buffer\n");
        return -1;
    }
    if (iotdb_execute_sql(client, sql) != 0) {
        fprintf(stderr, "failed to insert IoTDB metadata: %s\n", iotdb_client_error(client));
        return -1;
    }

    for (sample_index = 0; sample_index < sample_count; sample_index += IOTDB_SAMPLE_BATCH) {
        int64_t batch_end = sample_index + IOTDB_SAMPLE_BATCH;
        int64_t index;
        int64_t previous_time = 0;
        char *cursor;
        size_t remaining;

        if (batch_end > sample_count)
            batch_end = sample_count;
        written = snprintf(sql, sizeof(sql), "INSERT INTO %s(timestamp, value) VALUES ", waveform_path);
        if (written < 0 || (size_t)written >= sizeof(sql)) {
            fprintf(stderr, "IoTDB waveform statement exceeded its buffer\n");
            return -1;
        }
        cursor = sql + written;
        remaining = sizeof(sql) - (size_t)written;
        for (index = sample_index; index < batch_end; index++) {
            int64_t sample_time = starttime + (int64_t)llround((double)index * 1000.0 / msr->samprate);
            int added = snprintf(cursor, remaining, "%s(%" PRId64 ", %d)",
                                 index == sample_index ? "" : ", ", sample_time, samples[index]);

            if (index > sample_index && sample_time <= previous_time) {
                fprintf(stderr, "sample timestamps collide at millisecond precision\n");
                return -1;
            }
            if (added < 0 || (size_t)added >= remaining) {
                fprintf(stderr, "IoTDB waveform statement exceeded its buffer\n");
                return -1;
            }
            cursor += added;
            remaining -= (size_t)added;
            previous_time = sample_time;
        }
        if (iotdb_execute_sql(client, sql) != 0) {
            fprintf(stderr, "failed to insert IoTDB waveform batch: %s\n", iotdb_client_error(client));
            return -1;
        }
    }
    return 0;
}

static int commit_iotdb_message(rd_kafka_t *rk, rd_kafka_message_t **message)
{
    rd_kafka_resp_err_t error = rd_kafka_commit_message(rk, *message, 0);

    rd_kafka_message_destroy(*message);
    *message = NULL;
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "failed to commit Kafka offset after IoTDB processing: %s\n",
                rd_kafka_err2str(error));
        return -1;
    }
    return 0;
}
#endif


int main(int argc, char *argv[]) 
{
    int i, n, opt, npart = 0, *part = NULL;
    char *cp, *brokers = NULL, *topic = NULL, *groupid = NULL;
    char *tsdb_server = NULL, *db_name = NULL, *stable_name = NULL;
    char cmd[32768];
    rd_kafka_t *rk;
    rd_kafka_conf_t *rkc;
    rd_kafka_topic_t *rkt;
    rd_kafka_topic_conf_t *rktc;
    const rd_kafka_metadata_t *rkm;
    rd_kafka_resp_err_t rkre;
    rd_kafka_topic_partition_list_t *rktpl;
    char rks[1024];
#ifdef IOTDB_BACKEND
    IotdbClient iotdb;
    char database_path[256];
#else
    TAOS *taos;
#endif

    if (argc < 2) {
        fprintf(stderr, "Usage: %s -k bootstrap_servers(host:port) -t topic "
                        "-p part_no1,part_no2,... -g groupid -s tsdb_server "
                        "-d db_name -S stable_name\n", argv[0]);
        exit(1);
    } 

#ifdef IOTDB_BACKEND
    while ((opt = getopt(argc, argv, "k:t:p:g:s:d:")) != -1) {
#else
    while ((opt = getopt(argc, argv, "k:t:p:g:s:d:S:")) != -1) {
#endif
        switch (opt) {
            case 'k':
                brokers = strdup(optarg);
                break;
            case 't':
                topic = strdup(optarg);
                break;
            case 'p':
                npart = 0;
                for (cp = optarg; *cp != '\0'; cp++) {
                    if (*cp == ',')
                        npart++;
                }
                if (*(cp - 1) != ',')
                    npart++;
                if (npart > 0) {
                    if ((part = malloc(npart * sizeof(*part))) == NULL) {
                        fprintf(stderr, "failed to allocate space for part!\n");
                        exit(1);
                    }
                    for (cp = optarg, i = 0; *cp != '\0'; cp++, i++) {
                        sscanf(cp, "%d", part + i);
                        while (*cp != ',' && *cp != '\0') cp++;
                        if (*cp == '\0') break;
                    }
                }
                break;
            case 'g':
                groupid = strdup(optarg);
                break;
            case 's':
                tsdb_server = strdup(optarg);
                break;
            case 'd':
                db_name = strdup(optarg);
                break;
#ifndef IOTDB_BACKEND
            case 'S':
		stable_name = strdup(optarg);
		break;
#endif
            default:
#ifdef IOTDB_BACKEND
                fprintf(stderr, "Usage: %s -k bootstrap_servers(host:port) -t topic "
                                "-p part_no1,part_no2,... -g groupid -s iotdb_rest_url "
                                "-d database_name\n", argv[0]);
#else
                fprintf(stderr, "Usage: %s -k bootstrap_servers(host:port) "
                                "-t topic -p part_no1,part_no2,... -g groupid "
                                "-s tsdb_server -d db_name -S stable_name\n", 
				 argv[0]);
#endif
                exit(1);
        }
    }

    if (brokers == NULL || brokers[0] == '\0') {
        fprintf(stderr, "the option -k is missing!\n");
        exit(1);
    }

    if (topic == NULL || topic[0] == '\0') {
        fprintf(stderr, "the option -t is missing!\n");
        exit(1);
    }

    if (npart <= 0 || part == NULL) {
        fprintf(stderr, "the option -p is missing!\n");
        exit(1);
    }

    if (groupid == NULL || groupid[0] == '\0') {
        fprintf(stderr, "the option -g is missing!\n");
        exit(1);
    }

    if (tsdb_server == NULL || tsdb_server[0] == '\0') {
        fprintf(stderr, "the option -s is missing!\n");
        exit(1);
    }

    if (db_name == NULL || db_name[0] == '\0') {
        fprintf(stderr, "the option -d is missing!\n");
        exit(1);
    }

#ifndef IOTDB_BACKEND
    if (stable_name == NULL || stable_name[0] == '\0') {
        fprintf(stderr, "the option -S is missing!\n");
        exit(1);
    }
#else
    if (!iotdb_format_database_path(database_path, sizeof(database_path), db_name)) {
        fprintf(stderr, "IoTDB database must be a simple path node such as tsarchive\n");
        exit(1);
    }
#endif

    rkc = rd_kafka_conf_new();

    if (rd_kafka_conf_set(rkc, "bootstrap.servers", brokers, rks,
                          sizeof(rks)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", rks);
        rd_kafka_conf_destroy(rkc);
        exit(1);
    }

    if (rd_kafka_conf_set(rkc, "group.id", groupid, rks,
                         sizeof(rks)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", rks);
        rd_kafka_conf_destroy(rkc);
        exit(1);
    }

    if (rd_kafka_conf_set(rkc, "auto.offset.reset", "latest", rks,
                          sizeof(rks)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", rks);
        rd_kafka_conf_destroy(rkc);
        exit(1);
    }

#ifdef IOTDB_BACKEND
    if (rd_kafka_conf_set(rkc, "enable.auto.commit", "false", rks,
                          sizeof(rks)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(rkc, "enable.auto.offset.store", "false", rks,
                          sizeof(rks)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", rks);
        rd_kafka_conf_destroy(rkc);
        exit(1);
    }
#endif

    rd_kafka_conf_set_rebalance_cb(rkc, rebalance_cb);

    rd_kafka_conf_set(rkc, "enable.partition.eof", "true", NULL, 0);

    rktc = rd_kafka_topic_conf_new();

    rd_kafka_conf_set_default_topic_conf(rkc, rktc);
    rktc = NULL; /* The consumer configuration takes ownership. */

    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, rkc, rks, sizeof(rks)))) {
        fprintf(stderr, "failed to create a new Kafka consumer: %s\n", rks);
        rd_kafka_conf_destroy(rkc);
        exit(1);
    }
    rkc = NULL;

    rd_kafka_poll_set_consumer(rk);

    rkt = rd_kafka_topic_new(rk, topic, NULL);
    if (rkt == NULL) {
        fprintf(stderr, "failed to open Kafka topic %s\n", topic);
        exit(1);
    }

    rkre = rd_kafka_metadata(rk, 0, rkt, &rkm, 5000);
    if (rkre != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "failed to acquire the metadata!\n");
        exit(1);
    }

    if (rkm->topic_cnt != 1 || rkm->topics[0].err) {
        fprintf(stderr, "topic %s has no usable metadata\n", topic);
        rd_kafka_metadata_destroy(rkm);
        exit(1);
    }
    n = rkm->topics[0].partition_cnt;
    rd_kafka_metadata_destroy(rkm);

    rktpl = rd_kafka_topic_partition_list_new(npart);
    for (i = 0; i < npart; i++)
        if (part[i] < n)
            rd_kafka_topic_partition_list_add(rktpl, topic, part[i]);

    if ((rkre = rd_kafka_assign(rk, rktpl))) {
        fprintf(stderr, "failed to assign partitions: %s\n",
                         rd_kafka_err2str(rkre));
        exit(1);
    }

#ifdef IOTDB_BACKEND
    {
        const char *username = getenv("IOTDB_USERNAME");
        const char *password = getenv("IOTDB_PASSWORD");
        const char *precision = getenv("IOTDB_TIMESTAMP_PRECISION");
        const char *timeout_text = getenv("IOTDB_TIMEOUT_MS");
        char *timeout_end = NULL;
        long timeout_ms = 10000L;

        if (timeout_text != NULL && timeout_text[0] != '\0') {
            errno = 0;
            timeout_ms = strtol(timeout_text, &timeout_end, 10);
            if (errno == ERANGE || timeout_end == timeout_text || *timeout_end != '\0' ||
                timeout_ms <= 0 || timeout_ms > 600000) {
                fprintf(stderr, "IOTDB_TIMEOUT_MS must be between 1 and 600000 milliseconds\n");
                exit(1);
            }
        }
        if (precision != NULL && strcmp(precision, "ms") != 0) {
            fprintf(stderr, "IoTDB timestamp_precision must be ms for this sink\n");
            exit(1);
        }
        if (iotdb_client_init(&iotdb, tsdb_server, username, password, timeout_ms) != 0) {
            fprintf(stderr, "invalid IoTDB REST configuration; set IOTDB_USERNAME and IOTDB_PASSWORD\n");
            exit(1);
        }
        snprintf(cmd, sizeof(cmd), "CREATE DATABASE %s", database_path);
        if (iotdb_create_database(&iotdb, cmd) != 0) {
            fprintf(stderr, "failed to initialize IoTDB database: %s\n", iotdb_client_error(&iotdb));
            iotdb_client_close(&iotdb);
            exit(1);
        }
    }
#else
    // init TAOS
    taos_init();

    taos = taos_connect(tsdb_server, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        fprintf(stderr, "failed to connet to server: %s\n", tsdb_server);
        exit(1);
    }
   
    // use database
    sprintf(cmd, "use %s;", db_name);
    if (taos_query(taos, cmd) != 0) {
        fprintf(stderr, "failed to use database, reason: %s\n", 
                         taos_errstr(taos));
        exit(1);
    }
#endif

    while (1) {
        rd_kafka_message_t *rkmessage = NULL;
        MSRecord *msr = NULL;
        int32_t *idata;
        int64_t npts, starttime, ingesttime;
        time_t tnow, tb;
        struct timeval systemTime;

        tnow = time(NULL);
        rkmessage = rd_kafka_consumer_poll(rk, 1000);
        gettimeofday(&systemTime, NULL);
        ingesttime = systemTime.tv_sec * 1000L + 
                     (int64_t) round(systemTime.tv_usec * 0.001);
        if (!rkmessage)
            continue;

        if (rkmessage->err) {
            rd_kafka_message_destroy(rkmessage);
            continue;
        }

        if (rkmessage->len != 512 && rkmessage->len != 256) {
#ifdef IOTDB_BACKEND
            fprintf(stderr, "discarding unsupported Kafka record length %zu\n", rkmessage->len);
            if (commit_iotdb_message(rk, &rkmessage) != 0) {
                iotdb_client_close(&iotdb);
                exit(1);
            }
#else
            rd_kafka_message_destroy(rkmessage);
#endif
            continue;
        }

        if (msr_parse((char *)rkmessage->payload, (int)rkmessage->len,
                      &msr, 0, 1, 0) != MS_NOERROR || msr == NULL) {
            fprintf(stderr, "failed to parse miniSEED record\n");
            msr_free(&msr);
#ifdef IOTDB_BACKEND
            if (commit_iotdb_message(rk, &rkmessage) != 0) {
                iotdb_client_close(&iotdb);
                exit(1);
            }
#else
            rd_kafka_message_destroy(rkmessage);
#endif
            continue;
        }
        
        tb = MS_HPTIME2EPOCH(msr->starttime);
        if (tb < tnow - 3600 || tb > tnow + 3600) {
            msr_free(&msr);
#ifdef IOTDB_BACKEND
            if (commit_iotdb_message(rk, &rkmessage) != 0) {
                iotdb_client_close(&iotdb);
                exit(1);
            }
#else
            rd_kafka_message_destroy(rkmessage);
#endif
            continue;
        }

        npts = msr->numsamples;
        if (npts <= 0) {
            msr_free(&msr);
#ifdef IOTDB_BACKEND
            if (commit_iotdb_message(rk, &rkmessage) != 0) {
                iotdb_client_close(&iotdb);
                exit(1);
            }
#else
            rd_kafka_message_destroy(rkmessage);
#endif
            continue;
        }
        
        starttime = (int64_t) round(msr->starttime * 0.001);
        idata = (int32_t *)msr->datasamples;
        n = (int) round(1000.0 / msr->samprate);

#ifdef IOTDB_BACKEND
        if (insert_iotdb_record(&iotdb, db_name, msr, ingesttime, starttime,
                                idata, npts) != 0) {
            msr_free(&msr);
            rd_kafka_message_destroy(rkmessage);
            iotdb_client_close(&iotdb);
            exit(1);
        }
        msr_free(&msr);
        if (commit_iotdb_message(rk, &rkmessage) != 0) {
            iotdb_client_close(&iotdb);
            exit(1);
        }
#else
        sprintf(cmd, "insert into %s_%s_%s_%s_md using %s_md tags ('%s', "
                     "'%s', '%s', '%s') values (%ld, %d, %ld, %ld, %ld, %f);",
                     msr->network, msr->station, msr->location, msr->channel, 
                     stable_name, msr->network, msr->station, msr->location, 
                     msr->channel, ingesttime, msr->sequence_number, starttime, 
                     starttime + (npts - 1) * n, npts, msr->samprate);
        cp = strchr(cmd, ';');
        *(cp + 1) = '\0';
        if (taos_query(taos, cmd)) {
            fprintf(stderr, "failed to insert row [%s], reason:%s\n",
                             cmd, taos_errstr(taos));
            exit(1);
        }

        sprintf(cmd, "insert into %s_%s_%s_%s using %s tags ('%s', '%s', '%s',"
		     " '%s') values ", msr->network, msr->station, 
		     msr->location, msr->channel, stable_name, msr->network,
		     msr->station, msr->location, msr->channel);
        cp = strchr(cmd, ')');
        cp += 9;
        for (i = 0; i < npts; i++) {
            sprintf(cp, " (%ld, %d) ", starttime + i * n, idata[i]);
            cp = strchr(cp, ')');
            cp += 2;
        }
        *(cp - 1) = ';';
        *cp = '\0';
        if (taos_query(taos, cmd)) {
            fprintf(stderr, "failed to insert row [%s], reason:%s\n",
                             cmd, taos_errstr(taos));
            exit(1);
        }
        msr_free(&msr);
#endif
    }

#ifdef IOTDB_BACKEND
    iotdb_client_close(&iotdb);
#else
    taos_close(taos);
#endif
    return 0;
}


static int wait_eof = 0;
static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {
    switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                rd_kafka_assign(rk, partitions);
                wait_eof += partitions->cnt;
                break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                rd_kafka_assign(rk, NULL);
                wait_eof = 0;
                break;

        default:
                rd_kafka_assign(rk, NULL);
                break;
    }
}
