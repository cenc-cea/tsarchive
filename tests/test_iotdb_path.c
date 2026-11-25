#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../iotdb_path.h"

int main(void)
{
    char path[256];

    assert(iotdb_format_database_path(path, sizeof(path), "tsarchive"));
    assert(strcmp(path, "root.tsarchive") == 0);
    assert(!iotdb_format_database_path(path, sizeof(path), "bad.name"));
    assert(!iotdb_format_database_path(path, sizeof(path), "9database"));

    assert(iotdb_format_device_path(path, sizeof(path), "tsarchive", "waveform",
                                    "US", "ABCD", "00", "BHZ"));
    assert(strcmp(path, "root.tsarchive.waveform.US.ABCD.n_00.BHZ") == 0);
    assert(iotdb_format_device_path(path, sizeof(path), "tsarchive", "metadata",
                                    "US", "ABCD", "-", "BHZ"));
    assert(strcmp(path, "root.tsarchive.metadata.US.ABCD.no_x5F_location.BHZ") == 0);
    assert(iotdb_format_device_path(path, sizeof(path), "tsarchive", "waveform",
                                    "1US", "A-B", "", "BHZ"));
    assert(strcmp(path, "root.tsarchive.waveform.n_1US.A_x2D_B.no_x5F_location.BHZ") == 0);

    assert(!iotdb_format_device_path(path, sizeof(path), "tsarchive", "unknown",
                                     "US", "ABCD", "00", "BHZ"));
    assert(!iotdb_format_device_path(path, sizeof(path), "tsarchive;drop database root.x",
                                     "waveform", "US", "ABCD", "00", "BHZ"));
    assert(!iotdb_format_device_path(path, 8, "tsarchive", "waveform",
                                     "US", "ABCD", "00", "BHZ"));

    puts("IoTDB path tests passed");
    return 0;
}
