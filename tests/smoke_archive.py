"""Real Kafka-to-IoTDB smoke. Requires kcat, IOTDB_URL/USERNAME/PASSWORD,
KAFKA_BOOTSTRAP and an unused topic KAFKA_TOPIC with one partition.
"""
import base64
import datetime
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import time
import urllib.request
import uuid

repo = Path(__file__).resolve().parents[1]
base = os.environ['IOTDB_URL'].rstrip('/')
auth = base64.b64encode((os.environ['IOTDB_USERNAME'] + ':' + os.environ['IOTDB_PASSWORD']).encode()).decode()
db = 'seed_' + uuid.uuid4().hex
broker, topic = os.environ['KAFKA_BOOTSTRAP'], os.environ['KAFKA_TOPIC']


def sql(statement, query=True):
    request = urllib.request.Request(base + '/rest/v2/' + ('query' if query else 'nonQuery'),
        data=json.dumps({'sql': statement}).encode(), headers={'Authorization': 'Basic ' + auth, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=12) as response:
        result = json.load(response)
    assert result.get('code', 200) == 200, result
    return result


def miniseed(now):
    # miniSEED 2 fixed header + blockette 1000 + three big-endian INT32 samples.
    record = bytearray(256)
    record[:20] = b'000001D SMOKE00BHZUS'
    struct.pack_into('>HHBBBBHHhhBBBBiHH', record, 20, now.year, now.timetuple().tm_yday,
        now.hour, now.minute, now.second, 0, 0, 3, 10, 1, 0, 0, 0, 1, 0, 64, 48)
    struct.pack_into('>HHBBBB', record, 48, 1000, 0, 3, 1, 8, 0)
    struct.pack_into('>iii', record, 64, 60, -2, 12)
    return bytes(record)


with tempfile.TemporaryDirectory() as temp:
    channels = Path(temp) / 'channels'
    channels.write_text('US.SMOKE.00.BHZ\n')
    consumer = None
    try:
        for _ in range(2):
            subprocess.run([str(repo / 'create_table_iotdb'), base, db, str(channels)], check=True, timeout=40)
        consumer = subprocess.Popen([str(repo / 'tsarchive_iotdb'), '-k', broker, '-t', topic, '-p', '0',
            '-g', db, '-s', base, '-d', db])
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        record = miniseed(now)
        timestamp = int(now.timestamp()) * 1000
        waveform = 'root.' + db + '.waveform.US.SMOKE.n_00.BHZ'
        metadata = 'root.' + db + '.metadata.US.SMOKE.n_00.BHZ'
        for attempt in range(25):
            assert consumer.poll() is None, 'consumer stopped early'
            # The consumer starts at the live end of a fresh topic. Send a single
            # binary record (without -l, kcat sends a whole file as one record).
            if attempt in (2, 7):
                message = Path(temp) / 'record.mseed'
                message.write_bytes(record)
                subprocess.run(['kcat', '-P', '-b', broker, '-t', topic, '-p', '0', str(message)], check=True, timeout=12)
            rows = sql(f'SELECT value FROM {waveform} LIMIT 4')
            if rows.get('timestamps'):
                break
            time.sleep(.4)
        assert rows['timestamps'] == [timestamp, timestamp + 100, timestamp + 200], rows
        assert rows['values'] == [[60, -2, 12]], rows
        meta = sql(f'SELECT sequence_number,starttime,endtime,npts,samprate FROM {metadata} LIMIT 1')
        assert [col[0] for col in meta['values']] == [1, timestamp, timestamp + 200, 3, 10.0], meta
    finally:
        if consumer is not None:
            consumer.terminate()
            consumer.wait(timeout=5)
        present = sql(f'SHOW DATABASES root.{db}')
        if any('root.' + db in column for column in present.get('values', [])):
            sql(f'DELETE DATABASE root.{db}', False)
    print('PASS archive: repeated schema init, Kafka -> miniSEED -> 3 waveform points + 5 metadata values, cleanup')
