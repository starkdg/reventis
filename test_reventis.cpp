#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <ctime>
#include <cstdarg>
#include <algorithm>
#include <cassert>
#include "hiredis.h"

using namespace std;

typedef struct entry_t {
	long long object_id;
	double longitude;
	double latitude;
	string date;
	string starttime;
	string endtime;
	string descr;
} Entry;


int parse_event(string line, Entry &entry){
	string word;
	stringstream ss(line);
	getline(ss, word, ',');
	entry.latitude = atof(word.c_str());
	getline(ss, word, ',');
	entry.longitude = atof(word.c_str());
	getline(ss, entry.date, ',');
	getline(ss, entry.starttime, ',');
	getline(ss, entry.endtime, ',');
	getline(ss, entry.descr);
	entry.object_id = 0;
	return 0;
}

int parse_object(string line, Entry &entry){
	string word;
	stringstream ss(line);
	getline(ss, word, ',');
	entry.object_id = atoi(word.c_str());
	getline(ss, word, ',');
	entry.longitude = atof(word.c_str());
	getline(ss, word, ',');
	entry.latitude = atof(word.c_str());
	getline(ss, entry.date, ',');
	getline(ss, entry.starttime, ',');
	getline(ss, entry.descr);
	return 0;
}

/* read events from file and submit to redis-server */
int LoadEvents(redisContext *c, const string &key, const string &file){
	ifstream input(file);
	redisReply *reply;
	
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_event(line, e);
		reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s %s %s",
							 key.c_str(), e.longitude, e.latitude,
										  e.date.c_str(), e.starttime.c_str(),
										  e.date.c_str(), e.endtime.c_str(), e.descr.c_str());

		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
			cout << reply->integer << " " << e.descr << " " << e.date << " " << e.starttime << endl;
		}  else {
			freeReplyObject(reply);
			return count;
		}
		freeReplyObject(reply);
	}

	cout << count << " events loaded from " << file << endl;
	return count;
}

/* read objects from file and submit to redis-server */
int LoadObjects(redisContext *c, const string &key, const string &file){
	ifstream input(file);
	redisReply *reply;

	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_object(line, e);
		reply = (redisReply*)redisCommand(c, "reventis.update %s %f %f %s %s %ld %s",
										  key.c_str(), e.longitude, e.latitude,
										  e.date.c_str(), e.starttime.c_str(), e.object_id, e.descr.c_str());
		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
			cout << reply->integer << " " << e.descr << endl;
		} else {
			freeReplyObject(reply);
			return count;
		}
		freeReplyObject(reply);
	}
	cout << count << " objects loaded from " << file << endl;
	return count;
}

/* clear redis server */
int FlushAllEvents(redisContext *c, const string &key){
	int rc = 0;
	redisReply *reply = (redisReply*)redisCommand(c, "del %s", key.c_str());
	if (reply == NULL) return -1;
	freeReplyObject(reply);

	reply = (redisReply*)redisCommand(c, "del event:id");
	if (reply == NULL) return -1;
	
	freeReplyObject(reply);
	return rc;
}

/* query redis-server for number of events, objects added under key*/
long long GetNumberEvents(redisContext *c, const string &key){
	long long sz = -1;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.size %s", key.c_str());
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		sz = reply->integer;
	}
	freeReplyObject(reply);
	return sz;
}

/* add integer category for a specific id */
int AddCategoryForId(redisContext *c, const string &key, const long long id, int n, ...){
	int rc = 0;
	char cmd[256];
	snprintf(cmd, 256, "reventis.addcategory %s %lld ", key.c_str(), id);
	va_list ap;
	va_start(ap, n);
	for (int i=0;i<n;i++){
		string catidstr = to_string(va_arg(ap, int)) + " ";
		strncat(cmd, catidstr.c_str(), catidstr.size());
	}
	va_end(ap);

	redisReply *reply = (redisReply*)redisCommand(c, cmd);
	if (reply == NULL) rc = -1;
	freeReplyObject(reply);
	return rc;
}

/* remove integer category for specific id */
int RemoveCategoryForId(redisContext *c, const string &key, const long long id, int n, ...){
	int rc = 0;
	char cmd[256];
	snprintf(cmd, 256, "reventis.remcategory %s %lld ", key.c_str(), id);
	va_list ap;
	va_start(ap, n);
	for (int i=0;i<n;i++){
		string catidstr = to_string(va_arg(ap, int)) + " ";
		strncat(cmd, catidstr.c_str(), catidstr.size());
	}
	va_end(ap);

	redisReply *reply = (redisReply*)redisCommand(c, cmd);
	if (reply == NULL) rc = -1;
	freeReplyObject(reply);
	return rc;
}

/* process a redis-server reply and print results to stdout */
void ProcessReply(redisContext *c, redisReply *reply, int &count){
	if (reply && reply->type == REDIS_REPLY_ARRAY){
		for (int i=0;i<reply->elements;i++){
			ProcessReply(c, reply->element[i], count);
		}
	} else if (reply && reply->type == REDIS_REPLY_STRING){
		cout << "    (" << ++count << ") " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << reply->str << endl;
	} 
	return;
}

/* query for events given a longitude, latititude, time range */
/* and optionally filter for indicated categories */
/* [x1, x2] longitude range */
/* [y1, y2] latitude range  */
/* start/end dates - e.g. MM-DD-YYYY */
/* start/end times - e.g. HH:MM[:SS] */
/* n category integers [1,64] */
int Query(redisContext *c, const string &key,
		  const double x1, const double x2,
		  const double y1, const double y2,
		  const string &startdate, const string &starttime,
		  const string &enddate, const string &endtime, int n, ...){

	char cmd[256];
	snprintf(cmd, 256, "reventis.query %s %f %f %f %f %s %s %s %s ",
			 key.c_str(), x1, x2, y1, y2,
			 startdate.c_str(), starttime.c_str(),
			 enddate.c_str(), endtime.c_str());

	va_list ap;
	va_start(ap, n);
	for (int i=0;i<n;i++){
		string catidstr = to_string(va_arg(ap, int)) + " ";
		strncat(cmd, catidstr.c_str(), catidstr.size());
	}
	va_end(ap);

	cout << "cmd: " << cmd << endl;
	redisReply *reply = (redisReply*)redisCommand(c, cmd);

	int count = 0;
	ProcessReply(c, reply, count);
	freeReplyObject(reply);
	cout << count << " events queried" << endl;
	return count;
}

/* query for object ids in specified range */
int QueryByObjectId(redisContext *c, const string &key,
					const double x1, const double x2,
					const double y1, const double y2,
					const string &startdate, const string &starttime,
					const string &enddate, const string &endtime,
					long long object_id){

	cout << "query object_id = " << object_id << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.track %s %f %f %f %f %s %s %s %s %lld",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str(),
												  object_id);
	int count = 0;
	ProcessReply(c, reply, count);
	freeReplyObject(reply);
	cout << count << " objects queried" << endl;
	return count;
}

int LookupObjectHistory(redisContext *c, const string &key, long long object_id){
	cout << "Looking up object history of obj_id = " << object_id << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.hist %s %lld", key.c_str(), object_id);

	int count = 0;
	ProcessReply(c, reply, count);
	freeReplyObject(reply);
	cout << "object_id = " << object_id << " has " << count << " updates" << endl;
	return count;
}

int TrackAllObjects(redisContext *c, const string &key,
					const double x1, const double x2, const double y1, const double y2,
					const string &startdate, const string &starttime,
					const string &enddate, const string &endtime){

	char scratch[256];
	snprintf(scratch, 256, "Fetch all objects in longitude = %f to %f, latitude = %f to %f, %s %s to %s %s",
			 x1, x2, y1, y2, startdate.c_str(), starttime.c_str(), enddate.c_str(), endtime.c_str());

	cout << scratch << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.trackall %s %f %f %f %f %s %s %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str());
	int count = 0;
	ProcessReply(c, reply, count);
	freeReplyObject(reply);
	cout << " (" << count << " objects returned)" << endl << endl;
	return count;
}

int TrackObjectId(redisContext *c, const string &key,
				  const double x1, const double x2, const double y1, const double y2,
				  const string &startdate, const string &starttime,
				  const string &enddate, const string &endtime, long long obj_id){

	char scratch[256];
	snprintf(scratch, 256, "Fetch obj = %lld in longitude = %f to %f, latitude = %f to %f, %s %s to %s %s",
			 obj_id, x1, x2, y1, y2, startdate.c_str(), starttime.c_str(), enddate.c_str(), enddate.c_str());
	cout << scratch << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.track %s %f %f %f %f %s %s %s %s %lld",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str(), obj_id);
	int count = 0;
	ProcessReply(c, reply, count);
	freeReplyObject(reply);
	cout << " (" << count << " objects returned)" << endl << endl;
	return count;
}

int Purge(redisContext *c, const string &key, const string &date, const string &time){
	int count = 0;
	cout << "purge all before " << date << " " << time << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.purge %s %s %s",
												  key.c_str(), date.c_str(), time.c_str());
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		count = reply->integer;
		cout << "deleted " << count << " entries" << endl;
	} else {
		cout << "error purging" << endl;
	}
	
	freeReplyObject(reply);
	return count;
}

int DeleteBlock(redisContext *c, const string &key, const double x1, const double x2, const double y1, const double y2,
				const string &startdate, const string &starttime, const string &enddate, const string &endtime){
	int count = 0;
	cout << "delete block" << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delblk %s %f %f %f %f %s %s %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str());
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		count = reply->integer;
		cout << "deleted " << count << " entries" << endl;
	} else {
		cout << "error deleting block" << endl;
	}
	freeReplyObject(reply);
	return count;
}

int DeleteObject(redisContext *c, const string &key, long long object_id){
	int count = 0;
	cout << "delete object_id = " << object_id << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delobj %s %lld", key.c_str(), object_id);
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		count = reply->integer;
		cout << "deleted " << count << " entries" << endl;
	} else {
		cout << "error deleting object" << endl;
	}
	freeReplyObject(reply);
	return count;
}

int main(int argc, char **argv){
	const string key = "myevents";
	const string event_file = "data.csv";
	const string object_file = "objects.csv";
	const string addr = "localhost";
	const int port = 6379;

	cout << "Open connection to " << addr << ":" << port << endl;
	redisContext *c = redisConnect(addr.c_str(), port);
	if (c == NULL || c->err){
		if (c){
			cout << "Error: " << c->errstr << endl;
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
	}

	int rc = FlushAllEvents(c, key);
	assert(rc == 0);

	int n = LoadEvents(c, key, event_file);
	assert(n == 373);

	n = GetNumberEvents(c, key);
	assert(n == 373);
	
	AddCategoryForId(c, key, 11, 1, 1);
	AddCategoryForId(c, key, 12, 2, 1, 2);
	AddCategoryForId(c, key, 13, 3, 1, 2, 3);

	AddCategoryForId(c, key, 317, 2, 10, 20);
	AddCategoryForId(c, key, 318, 2, 20, 60);
	AddCategoryForId(c, key, 319, 2, 30, 60);
	AddCategoryForId(c, key, 320, 3, 40, 50, 64);

	AddCategoryForId(c, key, 304, 1, 1);
	AddCategoryForId(c, key, 305, 3, 10, 11, 12);
	AddCategoryForId(c, key, 306, 2, 10, 25);
	AddCategoryForId(c, key, 307, 3, 10, 20, 64);
	AddCategoryForId(c, key, 308, 2, 10, 20);
	AddCategoryForId(c, key, 309, 2, 10, 50);

	AddCategoryForId(c, key, 354, 1, 1);
	AddCategoryForId(c, key, 355, 2, 2, 21);
	AddCategoryForId(c, key, 356, 1, 3);
	AddCategoryForId(c, key, 357, 1, 4);
	AddCategoryForId(c, key, 359, 3, 5, 3, 21);
	AddCategoryForId(c, key, 360, 1, 6);
	AddCategoryForId(c, key, 361, 1, 7);
	AddCategoryForId(c, key, 362, 1, 8);
	AddCategoryForId(c, key, 363, 1, 9);
	AddCategoryForId(c, key, 364, 1, 10);
	AddCategoryForId(c, key, 365, 5, 11, 10, 20, 24, 25);

	n = LoadObjects(c, key, object_file);
	assert(n == 63);

	n = GetNumberEvents(c, key);
	assert(n == 373 + 63);

	/* query for clusters of data */
	double x1 = -72.338000;
	double x2 = -72.330000;
	double y1 = 41.571000;
	double y2 = 41.576000;
	string date = "12-01-2019";
	string enddate;
	string starttime = "12:00";
	string endtime = "16:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 0);
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 1);
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 2);
	assert(n == 2);
	
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 3);
	assert(n == 1);

	x1 = -72.218815;
	x2 = -72.217520;
	y1 = 41.719640;
	y2 = 41.720690;
	date = "10-09-2019";
	starttime = "12:00";
	endtime = "18:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 0);
	assert(n == 4);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 20, 10);
	assert(n == 2);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 60, 40);
	assert(n == 3);
	
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 10);
	assert(n == 1);

	x1 = -72.257400;
	x2 = -72.248800;
	y1 = 41.806490;
	y2 = 41.809360;
	date = "12-04-2019";
	starttime = "9:00";
	endtime = "18:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 0);
	assert(n == 6);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 3, 1, 12, 64);
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 50, 1);
	assert(n == 2);
		
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 20, 1);
	assert(n == 3);

	x1 = -106.513485;
	x2 = -93.712109;
	y1 = 27.336248;
	y2 = 36.416949;
	date = "01-01-2019";
	starttime = "12:00";
	enddate = "01-01-2020";
	endtime = "12:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 0);
	assert(n == 11);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 4, 1, 9, 10, 20);
	assert(n == 4);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 2, 5, 4);
	assert(n == 2);

	RemoveCategoryForId(c, key, 361, 1, 7);
	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 1, 7);
	assert(n == 0);


	/* look up object histories for various object_id's*/
	n = LookupObjectHistory(c, key, 150);
	assert(n == 12);

	n = LookupObjectHistory(c, key, 10);
	assert(n == 17);

	n = LookupObjectHistory(c, key, 20);
	assert(n == 7);

	n = LookupObjectHistory(c, key, 30);
	assert(n == 5);

	n = LookupObjectHistory(c, key, 40);
	assert(n == 6);

	n = LookupObjectHistory(c, key, 60);
	assert(n == 3);

	n = LookupObjectHistory(c, key, 70);
	assert(n == 3);

	n = LookupObjectHistory(c, key, 80);
	assert(n == 5);

	/* test function to track all objects in a certain range */
	x1 = -121.466200;
	x2 = -121.429000;
	y1 = 38.672800;
	y2 = 38.676850;
	date = "07-21-2019";
	starttime = "17:10";
	endtime = "18:35";
	n = TrackAllObjects(c, key, x1, x2, y1, y2, date, starttime, date, endtime);
	assert(n == 3);

	x1 = -72.255100;
	x2 = -72.240000;
	y1 = 41.804500;
	y2 = 41.813150;
	date = "11-05-2019";
	starttime = "13:00";
	endtime = "16:35";
	n = TrackAllObjects(c, key, x1, x2, y1, y2, date, starttime, date, endtime);
	assert(n == 6);


	x1 = -72.658900;
	x2 = -72.540200;
	y1 = 41.807600;
	y2 = 41.855700;
	date = "09-14-2019";
	starttime = "13:02";
	endtime = "13:35";
	n = TrackAllObjects(c, key, x1, x2, y1, y2, date, starttime, date, endtime);
	assert(n == 6);

	/* fetch individual objects in a specified range */
	long long obj_id = 10;
	x1 = -74.012500;
	x2 = -74.002000;
	y1 = 40.702800;
	y2 = 40.720000;
	date = "08-02-2019";
	starttime = "08:30";
	endtime = "09:01";
	n = TrackObjectId(c, key, x1, x2, y1, y2, date, starttime, date, endtime, obj_id);
	assert(n == 3);

	x1 = -97.739100;
	x2 = -96.334300;
	y1 = 30.280400;
	y2 = 30.616300;
	date = "01-01-2019";
	starttime = "12:00";
	enddate = "01-30-2020";
	endtime = "12:00";
	n = DeleteBlock(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime);
	assert(n == 11);

	n = GetNumberEvents(c, key);
	assert(n == 425);
	
	date = "05-01-2019";
	starttime = "00:00";
	n = Purge(c, key, date, starttime);
	assert(n == 10);

	n = GetNumberEvents(c, key);
	assert(n == 415);
	
	n = DeleteObject(c, key, 150);
	assert(n == 12);

	n = GetNumberEvents(c, key);
	assert(n == 403);

	n = DeleteObject(c, key, 10);
	assert(n == 17);

	n = GetNumberEvents(c, key);
	assert(n == 386);

	rc = FlushAllEvents(c, key);
	assert(rc == 0);
	
	redisFree(c);
	return 0;
}
