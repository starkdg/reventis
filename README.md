# Reventis

A Redis module for indexing events by location and time, capable of efficient range query.

## Installation Instructions

To build and install the module:   

```
make .
make all
make install
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
gives the number of events in the index.


```
reventis.insert myevents longitude latitude date-start time-start date-end time-end title-string
reventis.query myevents  <longitude range> <latitude range> <time-range>
reventis.lookup myevents longitude latitude start-time end-time
reventis.delete myevents longitude latitude date-start time-start event-id
reventis.purge myevents date time
reventis.delblk myevents <longitude-range> <latitude-range> <time-range>
reventis.print myevents
reventis.clear myevents
reventis.size myevents

```

The first argument is always the name of the index - e.g. "myevents".  All inserted events belong to
a particular index structure. Multiple structures are possible.  

Longitude values must be in the range -180.0 ot 180.0, while acceptable latitudes are -90.0 to 90.0.
Time values are in 24 hour time in the form HH:MM.  

The longitude/latitude and time range arguments must include lower and upper bounds.

# Test

Use the loadevents program to load a file of events, like so:

```
./loadevents myevents data.csv
```

Then you can issue redis commands:

```
reventis.size myevents
==> 373

reventis.query myevents -73.514076 -71.793121 41.187130 42.036561 0-01-2019 12:00 01-01-2019 12:00

==> 31 results


reventis.query myevents -106.513485 -93.712109 27.336248 36.416949	01-01-2019 12:00 01-01-2019 12:00

==> 11 results

reventis.query myevents -72.257400 -72.248800 41.806490 41.809360 12-04-2019 9:00 12-04-2019 18:00

==> 6 results

reventis.query myevents -72.218815 -72.217520 41.719640 41.720690 10-09-2019 12:00 12-09-2019 18:00

==> 4 results

reventis.query myevents -72.338000 -72.330000 41.571000 41.576000 12-01-2019 12:00 12-01-2019 16:00
==> 3 results

reventis.purge myevents 07-01-2019 00:00

reventis.clear myevents


```




   


