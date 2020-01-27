# Reventis

A Redis module for indexing events by location and time, capable of efficient range query.
Indexes by longitude, latitude and time - e.g. -72.338000  41.571000 12-01-2019 12:00.

## Installation Instructions

To build and install the module:   

```
make .
make all
make install
make test
```

to /var/local/lib directory.

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

You can use the following commands to interat with the index.  All mostly self explanatory.
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

The first argument is always the name of the index - e.g. "myevents".  All inserted events belong to
a particular index structure. Multiple structures are possible.  

Longitude values must be in the range -180.0 ot 180.0, while acceptable latitudes are -90.0 to 90.0.
Time values are in 24 hour time in the form HH:MM.
Date values are in the form MM:DD:YYYY

The longitude/latitude and time range arguments must include lower and upper bounds.

# Test

Use the loadevents program to load a file of events.  You can also use gendata to add
a specific number of randomly generated events to the index.

```
./loadevents myevents data.csv
./gendata key <int>
```

The testquery program will check for clusters in the data set.


