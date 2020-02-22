# Reventis

A Redis module for indexing events by location and time for fast efficient
range query.  Spatial coordinates are given in longitude and latitude with
up to 6 digits of precision. Time is specified by strings - e.g. HH:MM[:SS]
The seconds precision is optional.

## Features

* Store all events in a single Redis native data structure. Use multiple
  data structures with different keys.  

* Efficient range query.  Get all results within a specified range.

* Add/remove categories on individual events.  Include only results
  in a range query that fall in certain specified categories.  
  
* Purge all that events that fall before a specific time stamp.

* Delete a block of events in a certain range.

* Basic object tracking (since v0.3.0) with the ability to update coordinates
  for specific object ids,  query for all objects that fall in a specific location
  and time limits, or query for history of an object.   

## Installation Instructions

To build and install the module:   

```
make .
make all
make install
```

Run `make test` when you have a redis server up and running.  

Installs to /var/local/lib directory.

To load the module into redis, just type in the redis-cli:

```
module load /var/local/lib/reventis.so
module unload reventis
module list

```

Or, put this in your redis.conf configuration file:

```
loadmodule /var/local/lib/reventis.so
```

# Module Commands

You can use the following commands to interact with the index.  All mostly self explanatory.
The 'purge' commmand purges all events before a specified time. The 'print' command will print
the index entries to the redis-server.log file.  Useful for debugging purposes.  The 'size' command
gives the number of events in the index. Depth gives you the balanced tree height. 

```
reventis.insert key longitude latitude date-start time-start date-end time-end title-string
reventis.query key  <longitude range> <latitude range> <time-range>
reventis.lookup key event-id
reventis.delete key event-id
reventis.purge key date time
reventis.delblk key <longitude-range> <latitude-range> <time-range>
reventis.print key
reventis.clear key
reventis.size key
reventis.depth key
```

v0.2 introduces categories.  For each event indexed, you can add/remove categories.  Just map your
categories to integer values - from 1 up to 64.

```
reventis.addcategory key event-id category-id ... [ category-id ... [category-id]]
reventis.remcategory key event-id category-id ... [ category-id ... [category-id]]
```

Then, query like this.  Just specify any variable number of categories on the end:

```
reventis.query key <longitude range> <latitude range> <time-range> <category-id> ...
```

The object trackign api includes these functions:

```
reventis.update <key> <longitude> <latitude> <date> <time> <object_id> <description>
reventis.track <key> <longitude-range> <latitude-range> <time-range> <object_id>
reventis.trackall <key> <longitude-range> <latitude-range> <time-range>
reventis.hist <key> <object_id>
```

The first argument is always the name of the index - e.g. "myevents".  All inserted events belong to
a particular index structure. Multiple structures are possible.  

Longitude values must be in the range -180.0 ot 180.0, while acceptable latitudes are -90.0 to 90.0.
Time values are in 24 hour time in the form HH:MM:SS. 
Date values are in the form MM:DD:YYYY

The longitude/latitude and time range arguments must include lower and upper bounds.

# Test

Use the loadevents program to load a file of events.  You can also use gendata to add
a specific number of randomly generated events to the index.

```
./loadevents myevents data.csv
./gendata key <int>
```

The `testreventis` program will test the basic functions of the module. It will run with
ctest, but make sure you have a redis-server running before you invoke it.  


