#ifndef _DEFS_H
#define _DEFS_H
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <clocale>
#include <cstdint>
#include <vector>
#include <queue>
#include <ctime>
#include <cassert>
#include <limits>
#include "spfc.hpp"

#define LONG_LAT_SCALE_FACTOR 1000000

// formats for parsing input strings
static const char *date_fmt = "%d-%d-%d";         // MM-DD-YYY
static const char *time_fmt = "%d:%d";            // HH:MM
static const char *key_fmt = "%lu-%lu-%lu";       // arr[0]-arr[1]-arr[2] 

//value formates
static const char *value_fmt = "%lld %lu %lu";      // eventid start_time end_time 
static const char *descr_fmt = "%s";


// key field name
static const char *key_field_name = "key";

// value field names
static const char *value_field_name = "value";
static const char *descr_field_name = "descr";

// other node field names
static const char *count_field_name = "count";
static const char *max_field_name = "max";
static const char *color_field_name = "red";
static const char *left_field_name = "left";
static const char *right_field_name = "right";

static const struct lconv *locale;

static const char *linkfmtstr = "%s-node-%lu";

static uint64_t next_link_n = 100;

static uint64_t next_link(){
	next_link_n += 1;
	return next_link_n;
}

/* time interval */
typedef struct interval_t {
	time_t start;
	time_t end;
} Interval;




#endif /* _DEFS_H */
