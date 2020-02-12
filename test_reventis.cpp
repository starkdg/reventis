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
	double longitude;
	double latitude;
	string date;
	string starttime;
	string endtime;
	string descr;
} Entry;


void trim(string &s){
	s.erase(remove(s.begin(), s.end(), ' '), s.end());
}

int parse_entry(string line, Entry &entry){
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
	return 0;
}

int LoadEvents(redisContext *c, const string &key, const string &file){
	ifstream input(file);
	redisReply *reply;
	
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_entry(line, e);
		reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s %s %s",
							 key.c_str(), e.longitude, e.latitude,
										  e.date.c_str(), e.starttime.c_str(),
										  e.date.c_str(), e.endtime.c_str(), e.descr.c_str());

		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
			cout << reply->integer << " " << e.descr << endl;
		}  else {
			freeReplyObject(reply);
			return count;
		}
		freeReplyObject(reply);
	}

	return count;
}

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

long long GetNumberEvents(redisContext *c, const string &key){
	long long sz = -1;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.size %s", key.c_str());
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		sz = reply->integer;
	}
	freeReplyObject(reply);
	return sz;
}

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
	return count;
}

int main(int argc, char **argv){
	const string key = "myevents";
	const string file = "data.csv";
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

	cout << "Load events from " << file << endl;
	int n = LoadEvents(c, key, file);
	cout << n << " events loaded associated with key, " << key << endl;
	assert(n == 373);

	cout << "Add Categories." << endl;
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

	cout << "----------------------------------------------------------" << endl;
	
	double x1 = -72.338000;
	double x2 = -72.330000;
	double y1 = 41.571000;
	double y2 = 41.576000;
	string date = "12-01-2019";
	string starttime = "12:00";
	string endtime = "16:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 0);
	cout << n << " results." << endl;
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 1);
	cout << n << " results." << endl;
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 2);
	cout << n << " results." << endl;
	assert(n == 2);
	
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 3);
	cout << n << " results." << endl;
	assert(n == 1);


	cout << "-----------------------------------------------------------" << endl;
	
	/* second cluster */
	x1 = -72.218815;
	x2 = -72.217520;
	y1 = 41.719640;
	y2 = 41.720690;
	date = "10-09-2019";
	starttime = "12:00";
	endtime = "18:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 0);
	cout << n << " results." << endl;
	assert(n == 4);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 20, 10);
	cout << n << " results." << endl;
	assert(n == 2);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 60, 40);
	cout << n << " results." << endl;
	assert(n == 3);
	
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 1, 10);
	cout << n << " results." << endl;
	assert(n == 1);

	cout << "----------------------------------------------------------" << endl;
		
	x1 = -72.257400;
	x2 = -72.248800;
	y1 = 41.806490;
	y2 = 41.809360;
	date = "12-04-2019";
	starttime = "9:00";
	endtime = "18:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 0);
	cout << n << " results." << endl;
	assert(n == 6);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 3, 1, 12, 64);
	cout << n << " results." << endl;
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 50, 1);
	cout << n << " results." << endl;
	assert(n == 2);
		
	n = Query(c, key, x1, x2, y1, y2, date, starttime, date, endtime, 2, 20, 1);
	cout << n << " results." << endl;
	assert(n == 3);

	cout << "-----------------------------------------------------------" << endl;
	
	x1 = -106.513485;
	x2 = -93.712109;
	y1 = 27.336248;
	y2 = 36.416949;
	date = "01-01-2019";
	starttime = "12:00";
	string enddate = "01-01-2020";
	endtime = "12:00";
	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 0);
	cout << n << " results." << endl;
	assert(n == 11);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 4, 1, 9, 10, 20);
	cout << n << " results." << endl;
	assert(n == 4);

	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 2, 5, 4);
	cout << n << " results." << endl;
	assert(n == 2);

	RemoveCategoryForId(c, key, 361, 1, 7);
	n = Query(c, key, x1, x2, y1, y2, date, starttime, enddate, endtime, 1, 7);
	cout << n << " results." << endl;
	assert(n == 0);
	
	rc = FlushAllEvents(c, key);
	assert(rc == 0);
	redisFree(c);
	return 0;
}
