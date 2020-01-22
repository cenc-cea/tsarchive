/*
 * Written by Yanlu Ma, 2019-Dec-20 at CENC, Beijing.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include <taos.h>
#include "libmseed.h"
#include "librdkafka/rdkafka.h"

static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque);


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
    TAOS *taos;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s -k bootstrap_servers(host:port) -t topic "
                        "-p part_no1,part_no2,... -g groupid -s tsdb_server "
                        "-d db_name -S stable_name\n", argv[0]);
        exit(1);
    } 

    while ((opt = getopt(argc, argv, "k:t:p:g:s:d:S:")) != -1) {   
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
	    case 'S':
		stable_name = strdup(optarg);
		break;
            default:
                fprintf(stderr, "Usage: %s -k bootstrap_servers(host:port) "
                                "-t topic -p part_no1,part_no2,... -g groupid "
                                "-s tsdb_server -d db_name -S stable_name\n", 
				 argv[0]);
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

    if (stable_name == NULL || stable_name[0] == '\0') {
        fprintf(stderr, "the option -S is missing!\n");
        exit(1);
    }

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

    rd_kafka_conf_set_rebalance_cb(rkc, rebalance_cb);

    rd_kafka_conf_set(rkc, "enable.partition.eof", "true", NULL, 0);

    rktc = rd_kafka_topic_conf_new();

    rd_kafka_conf_set_default_topic_conf(rkc, rktc);

    if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, rkc, rks, sizeof(rks)))) {
        fprintf(stderr, "failed to create a new Kafka consumer: %s\n", rks);
        rd_kafka_conf_destroy(rkc);
        exit(1);
    }
    rkc = NULL;

    rd_kafka_poll_set_consumer(rk);

    rkt = rd_kafka_topic_new(rk, topic, rktc);
    rktc = NULL;

    rkre = rd_kafka_metadata(rk, 0, rkt, &rkm, 5000);
    if (rkre != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "failed to acquire the metadata!\n");
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

        if (rkmessage->err || (rkmessage->len != 512 && 
                               rkmessage->len != 256 )) {
            rd_kafka_message_destroy(rkmessage);
            continue;
        }

        msr_parse((char *)rkmessage->payload, rkmessage->len, &msr, 0, 1, 0);
        rd_kafka_message_destroy(rkmessage);        
        
        tb = MS_HPTIME2EPOCH(msr->starttime);
        if (tb < tnow - 3600 || tb > tnow + 3600)
            continue;

        npts = msr->numsamples;
        if (npts <= 0)
            continue;
        
        starttime = (int64_t) round(msr->starttime * 0.001);
        idata = (int32_t *)msr->datasamples;
        n = (int) round(1000.0 / msr->samprate);

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
    }

    taos_close(taos);
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
