# tsarchive
Consume miniSEED records from a Kafka topic and archive the waveform samples and
record metadata in a time-series database. The original executables
remain the default build. An optional IoTDB 2.x Tree sink is available with
`make iotdb`.

## IoTDB 2.x Tree sink

Install the build dependencies first: GCC, Make, libcurl development headers,
libmseed 2.x and librdkafka. Rebuild the bundled miniSEED library for your platform with `make -B -C libmseed static`, or pass `IOTDB_MSEED_LIB` to override it. The REST service must be
enabled on the IoTDB server. Configure credentials through the environment:

```sh
export IOTDB_USERNAME=root
export IOTDB_PASSWORD='replace-through-your-secret-manager'
export IOTDB_TIMESTAMP_PRECISION=ms
export IOTDB_TIMEOUT_MS=10000
make iotdb
```

`IOTDB_TIMESTAMP_PRECISION` may be omitted; if provided, it must be `ms`. The
sample writer rejects rates above 1000 samples/second because this version uses
millisecond timestamps and cannot represent distinct higher-rate samples. The
current sink accepts non-empty INT32 miniSEED records, matching the original
writer's sample representation.

The IoTDB consumer accepts the same Kafka options as the original consumer,
except `-S`, which does not apply to Tree storage:

```sh
./tsarchive_iotdb -k broker:9092 -t waveform -p 0,1 -g tsarchive -s \
  https://iotdb.example:18080 -d tsarchive
```

Tree schema creation accepts the IoTDB status for an existing database or timeseries. The consumer then
writes waveform values and record metadata on demand. It uses IoTDB REST v1
`/rest/v1/nonQuery` with Basic authentication and bounded connection/read
timeouts. Use HTTPS in production.

The channel initializer accepts the same `gsn_tables_list.txt` format and only
adds schema; it does not drop or clear an existing database:

```sh
./create_table_iotdb https://iotdb.example:18080 tsarchive gsn_tables_list.txt
```

For a channel such as `US.ABCD.00.BHZ`, the Tree paths are
`root.tsarchive.waveform.US.ABCD.n_00.BHZ.value` and
`root.tsarchive.metadata.US.ABCD.n_00.BHZ.*`. Empty or `-` locations map
to the escaped `no_x5F_location` node. Non-alphanumeric channel identifier bytes are
escaped as `_xHH_` so SQL/path syntax cannot be injected. Each channel's
metadata contains `sequence_number`, `starttime`, `endtime`, `npts`, and
`samprate`; record receipt time remains the metadata row timestamp.

Run the dependency-free identifier regression test with `make test-iotdb`.
