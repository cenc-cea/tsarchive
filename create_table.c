/*
 * Written by Yanlu Ma, 2019-Dec-20 at CENC, Beijing.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <math.h>
#include <taos.h>


int main(int argc, char *argv[]) 
{
    TAOS *taos;
    int i;
    size_t nt, nl;
    char server_ip[64], db_name[64], stable_name[64], line[1024];
    char **tbl_name, cmd[1024], net[8], sta[8], loc[8], cha[8];
    FILE *fin;

    if (argc != 5) {
        fprintf(stderr, "Usage: %s server_ip db_name stable_name tables_"
			"list.txt\n", argv[0]);
        exit(0);
    }

    // init TAOS
    taos_init();

    strncpy(server_ip, argv[1], 64);
    strncpy(db_name, argv[2], 64);
    strncpy(stable_name, argv[3], 64);
 
    if ((fin = fopen(argv[4], "r")) == NULL) {
        fprintf(stderr, "failed to open %s!\n", argv[3]);
        exit(1);
    }

    nt = 0;
    while (fgets(line, 1024, fin) != NULL) 
        nt++;

    if ((tbl_name = malloc(nt * sizeof(*tbl_name))) == NULL) {
        fprintf(stderr, "allocation failed for tbl_name!\n");
        exit(1);
    }

    rewind(fin);
    for (i = 0; i < nt; i++) {
        fgets(line, 1024, fin);
        nl = strlen(line) + 1;
        if ((tbl_name[i] = malloc(nl * sizeof(char))) == NULL) {
            fprintf(stderr, "allocation failed for tbl_name[%d]!\n", i);
            exit(1);
        }
        sscanf(line, "%s", tbl_name[i]);
    }

    fclose(fin);

    taos = taos_connect(server_ip, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
        fprintf(stderr, "failed to connet to server: %s\n", server_ip);
        exit(1);
    }
   
    // drop database
    sprintf(cmd, "drop database if exists %s;", db_name);
    if (taos_query(taos, cmd) != 0) {
        fprintf(stderr, "failed to drop database, reason: %s\n", 
                         taos_errstr(taos));
        exit(1);
    }
  
    // create database
    sprintf(cmd, "create database if not exists %s;", db_name);
    if (taos_query(taos, cmd) != 0) {
        fprintf(stderr, "failed to create database, reason: %s\n", 
                         taos_errstr(taos));
        exit(1);
    }
  
    // use database
    sprintf(cmd, "use %s;", db_name);
    if (taos_query(taos, cmd) != 0) {
        fprintf(stderr, "failed to use database, reason: %s\n", 
                         taos_errstr(taos));
        exit(1);
    }

    // create supertable
    sprintf(cmd, "create table if not exists %s (ts timestamp, v int) "
		 "tags (network binary(8), station binary(8), "
		 "location binary(8), channel binary(8));", 
		  stable_name);
    if (taos_query(taos, cmd) != 0) {
        fprintf(stderr, "failed to use database, reason: %s\n",
                         taos_errstr(taos));
        exit(1);
    }
    
    sprintf(cmd, "create table if not exists %s_md (ts timestamp, "
                 "sn int, starttime timestamp, endtime timestamp, npts int, "
                 "samprate float) tags (network binary(8), station "
                 "binary(8), location binary(8), channel binary(8));",
                  stable_name);
    if (taos_query(taos, cmd) != 0) {
        fprintf(stderr, "failed to use database, reason: %s\n",
                         taos_errstr(taos));
        exit(1);
    }

    // create tables
    for (i = 0; i < nt; i++) {
        printf("create table %s ...\n", tbl_name[i]);
        fflush(stdout);
	sscanf(tbl_name[i], "%[^.].%[^.].%[^.].%s", net, sta, loc, cha);
        if (loc[0] == '-')
            loc[0] = '\0';
        sprintf(cmd, "create table if not exists %s_%s_%s_%s using %s "
                     "tags ('%s', '%s', '%s', '%s');", net, sta, loc, cha, 
                      stable_name, net, sta, loc, cha);
        if (taos_query(taos, cmd) != 0) {
            fprintf(stderr, "failed to create table, reason: %s\n", 
                             taos_errstr(taos));
            exit(1);
        }
    }

    for (i = 0; i < nt; i++) {
        printf("create table %s_md ...\n", tbl_name[i]);
        fflush(stdout);
        sscanf(tbl_name[i], "%[^.].%[^.].%[^.].%s", net, sta, loc, cha);
        if (loc[0] == '-')
            loc[0] = '\0';
        sprintf(cmd, "create table if not exists %s_%s_%s_%s_md using %s_md "
                     "tags ('%s', '%s', '%s', '%s');", net, sta, loc, cha, 
                      stable_name, net, sta, loc, cha);
        if (taos_query(taos, cmd) != 0) {
            fprintf(stderr, "failed to create table, reason: %s\n",
                             taos_errstr(taos));
            exit(1);
        }
    }

    taos_close(taos);
    return 0;
}
