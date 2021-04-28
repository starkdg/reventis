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
				   const string &startdatetime, const string &enddatetime, const string &descr);



void AddCategory(redisContext *c, const string &key, const long long id, int n, ...);

void RemoveCategory(redisContext *c, const string &key, const long long id, int n, ...);


/* read objects from file and submit to redis-server */
int LoadObjects(redisContext *c, const string &key, const string &file);



int GetSize(redisContext *c, const string &key);



void DeleteKey(redisContext *c, const string &key);


int Query(redisContext *c, const string &key, const double x1, const double x2,
		  const double y1, const double y2, const string &startdatetime, 
		  const string &enddatetime, int n, ...);

int QueryByRadius(redisContext *c, const string &key,
				  const double x, const double y, const double radius,
				  const string &startdatetime, const string &enddatetime, int n, ...);

int ObjectHistory(redisContext *c, const string &key, const long long object_id);


int ObjectHistory2(redisContext *c, const string &key, const long long object_id,
				   const string &t1, const string &td2);


int TrackAll(redisContext *c, const string &key,
			 const double x1, const double x2,
			 const double y1, const double y2,
			 const string &startdatetime, const string &enddatetime);


int PurgeAllBefore(redisContext *c, const string &key, const string &datetime);

int DeleteBlock(redisContext *c, const string &key,
				const double x1, const double x2, const double y1, const double y2,
				const string &startdatetime, const string &enddatetime);


int DeleteObject(redisContext *c, const string &key, const long long object_id);


void DeleteEvent(redisContext *c, const string &key, const long long event_id);


#endif /* _CLIENT_UTIL_H */
