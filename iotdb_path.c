#include "iotdb_path.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>

static int valid_database(const char *database)
{
    const unsigned char *current = (const unsigned char *)database;

    if (current == NULL || !((*current >= 'A' && *current <= 'Z') ||
        (*current >= 'a' && *current <= 'z') || *current == '_'))
        return 0;
    for (current++; *current != '\0'; current++) {
        if (!((*current >= 'A' && *current <= 'Z') ||
              (*current >= 'a' && *current <= 'z') ||
              (*current >= '0' && *current <= '9') || *current == '_'))
            return 0;
    }
    return 1;
}

int iotdb_format_database_path(char *output, size_t capacity, const char *database)
{
    int written;

    if (output == NULL || capacity == 0 || !valid_database(database))
        return 0;
    written = snprintf(output, capacity, "root.%s", database);
    return written > 0 && (size_t)written < capacity;
}

static int safe_node(char *output, size_t capacity, const char *input, int optional)
{
    size_t index;
    size_t length = 0;
    size_t input_length;
    int has_character = 0;

    if (capacity == 0 || input == NULL)
        return 0;
    if (optional && (input[0] == '\0' || strcmp(input, "-") == 0 || strcmp(input, "--") == 0))
        input = "no_location";
    input_length = strlen(input);
    for (index = 0; index < input_length; index++) {
        unsigned char value = (unsigned char)input[index];
        if ((value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') ||
            (value >= '0' && value <= '9')) {
            if (length + 1 >= capacity)
                return 0;
            output[length++] = (char)value;
            has_character = 1;
        } else {
            if (length + 5 >= capacity)
                return 0;
            snprintf(output + length, capacity - length, "_x%02X_", value);
            length += 5;
        }
    }
    output[length] = '\0';
    if (length > 0 && output[0] >= '0' && output[0] <= '9') {
        if (length + 3 > capacity)
            return 0;
        memmove(output + 2, output, length + 1);
        output[0] = 'n';
        output[1] = '_';
    }
    return has_character;
}

int iotdb_format_device_path(char *output, size_t capacity,
                             const char *database, const char *kind,
                             const char *network, const char *station,
                             const char *location, const char *channel)
{
    char safe_network[128];
    char safe_station[128];
    char safe_location[128];
    char safe_channel[128];
    int written;

    if (output == NULL || capacity == 0 || kind == NULL || !valid_database(database) ||
        (strcmp(kind, "waveform") != 0 && strcmp(kind, "metadata") != 0) ||
        !safe_node(safe_network, sizeof(safe_network), network, 0) ||
        !safe_node(safe_station, sizeof(safe_station), station, 0) ||
        !safe_node(safe_location, sizeof(safe_location), location, 1) ||
        !safe_node(safe_channel, sizeof(safe_channel), channel, 0))
        return 0;

    written = snprintf(output, capacity, "root.%s.%s.%s.%s.%s.%s", database,
                       kind, safe_network, safe_station, safe_location, safe_channel);
    return written > 0 && (size_t)written < capacity;
}
