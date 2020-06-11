#ifndef _CLIENT_UTIL_H
#define _CLIENT_UTIL_H

#include <cstdlib>
#include <string>
#include <stdexcept>
#include "hiredis.h"

using namespace std;

/* read events from file and submit to redis-server under key */
int LoadEvents(redisContext *c, const string &key, const string &file);


long long AddEvent(redisContext *c, const string &key, const double x, const double y,
				   const string &startdate, const string &starttime,
				   const string &enddate, const string &endtime, const string &descr);



void AddCategory(redisContext *c, const string &key, const long long id, int n, ...);

void RemoveCategory(redisContext *c, const string &key, const long long id, int n, ...);


/* read objects from file and submit to redis-server */
int LoadObjects(redisContext *c, const string &key, const string &file);



int GetSize(redisContext *c, const string &key);



void FlushAll(redisContext *c, const string &key);


int Query(redisContext *c, const string &key, const double x1, const double x2,
		  const double y1, const double y2, const string &startdate, const string &starttime,
		  const string &enddate, const string &endtime, int n, ...);


int ObjectHistory(redisContext *c, const string &key, const long long object_id);


int ObjectHistory2(redisContext *c, const string &key, const long long object_id,
				   const string &d1, const string &t1, const string &d2, const string &t2);


int TrackAll(redisContext *c, const string &key,
			 const double x1, const double x2,
			 const double y1, const double y2,
			 const string &startdate, const string &starttime,
			 const string &enddate, const string &endtime);


int PurgeAllBefore(redisContext *c, const string &key, const string &date, const string &time);

int DeleteBlock(redisContext *c, const string &key,
				const double x1, const double x2, const double y1, const double y2,
				const string &startdate, const string &starttime,
				const string &enddate, const string &endtime);


int DeleteObject(redisContext *c, const string &key, const long long object_id);


void DeleteEvent(redisContext *c, const string &key, const long long event_id);


#endif /* _CLIENT_UTIL_H */
