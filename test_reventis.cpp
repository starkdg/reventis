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

/* read events from file and submit to redis-server under key */
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
		}  else {
			freeReplyObject(reply);
			return count;
		}
		freeReplyObject(reply);
	}

	cout << "Loaded " << count << " events from " << file << endl;
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
		} else if (reply && reply->type == REDIS_REPLY_ERROR){
			cout << "error: " << reply->str << endl;
			return count;
		} else {
			freeReplyObject(reply);
			return count;
		}
		freeReplyObject(reply);
	}
	cout << "Loaded " << count << " objects from " << file << endl;
	return count;
}

int GetSize(redisContext *c, const string &key){
	int n = 0;;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.size %s", key.c_str());
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		n = reply->integer;
	}
	freeReplyObject(reply);
	return n;
}

int FlushAllEvents(redisContext *c, const string &key){

	cout << "submit => del " << key << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "del %s", key.c_str());
	if (reply == NULL) {
		cout << "Could not delete key" << endl;
		return -1;
	}
	freeReplyObject(reply);

	cout << "submit => del event:id" << endl;
	reply = (redisReply*)redisCommand(c, "del event:id");
	if (reply == NULL) {
		cout << "Could not delete event:id" << endl;
		return -1;
	}

	freeReplyObject(reply);
	return 0;
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
	if (reply && (reply->type == REDIS_REPLY_STATUS || reply->type == REDIS_REPLY_STRING)){
		rc = 0;
	}  else {
		cout << "ERR - unable to add category" << endl;
		rc = -1;
	}
	
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
	if (reply && (reply->type == REDIS_REPLY_STATUS || reply->type == REDIS_REPLY_STRING)){
		rc = 0;
	} else {
		cout << "ERR - unable to add category" << endl;
		rc = -1;
	}
	freeReplyObject(reply);
	return rc;
}

void ProcessSubReplyArray(redisContext *c, redisReply *reply){
	for (int i=0;i < (int)reply->elements;i++){
		int type = reply->element[i]->type;
		if (type == REDIS_REPLY_STRING || type == REDIS_REPLY_STATUS || type == REDIS_REPLY_ERROR){
			cout << reply->element[i]->str << " ";
		} else if (type == REDIS_REPLY_INTEGER){
			cout << reply->element[i]->integer << " ";
		} else if (type == REDIS_REPLY_NIL){
			cout << " NIL ";
		}
	}
	cout << endl;
}

void ProcessReply(redisContext *c, redisReply *reply, int &count){
	cout << "  Reply:" << endl;
	if (reply && reply->type == REDIS_REPLY_ARRAY){
		for (int i=0;i < reply->elements;i++){
			int type = reply->element[i]->type;
			if (type == REDIS_REPLY_ARRAY){
				cout << "    (" << i+1 << ")  ";
				ProcessSubReplyArray(c, reply->element[i]);
			} else if (type == REDIS_REPLY_STRING || type == REDIS_REPLY_STATUS){
				cout << reply->element[i]->str << endl;
			} else if (type == REDIS_REPLY_INTEGER){
				cout << reply->element[i]->integer << endl;
			} else if (type == REDIS_REPLY_ERROR){
				cout << reply->element[i]->str << endl;
			} else if (type == REDIS_REPLY_NIL){
				cout << "NIL" << endl;
			} else {
				cout << "ERROR" << endl;
			}
			count++;
		}
		return;
	} else if (reply && reply->type == REDIS_REPLY_STRING){
		cout << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_INTEGER){
		cout << reply->integer << endl;
	} else if (reply && reply->type == REDIS_REPLY_STATUS){
		cout << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_NIL){
		cout << "NIL" << endl;
	} else {
		cout << "ERROR" << endl;
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

	cout << "send => " << cmd << endl << endl;
	redisReply *reply = (redisReply*)redisCommand(c, cmd);

	int count = 0;
	ProcessReply(c, reply, count);
	cout << "  (" << count << " results)" << endl << endl;
	freeReplyObject(reply);
	return count;
}

int ObjectHistory(redisContext *c, const string &key, const long long object_id){

	cout << "send => reventis.hist " << key << " " << object_id << endl << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.hist %s %lld",
												  key.c_str(), object_id);

	int count = 0;
	ProcessReply(c, reply, count);
	cout <<  "  (" << count << " results)" << endl << endl;
	freeReplyObject(reply);
	return count;
}

int ObjectHistoryForTimePeriod(redisContext *c, const string &key, const long long object_id,
				  const string &d1, const string &t1,
				  const string &d2, const string &t2){

	cout << "send => reventis.hist " << key << " " << object_id
		 << " " << d1 << " " << t1 << " " << d2 << " " << t2 << endl << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.hist %s %lld %s %s %s %s",
												  key.c_str(), object_id,
												  d1.c_str(), t1.c_str(),
												  d2.c_str(), t2.c_str());
	int count = 0;
	ProcessReply(c, reply, count);
	cout <<  "  (" << count << " results)" << endl << endl;
	freeReplyObject(reply);
	return count;
}

int TrackAll(redisContext *c, const string &key,
			 const double x1, const double x2,
			 const double y1, const double y2,
			 const string &startdate, const string &starttime,
			 const string &enddate, const string &endtime){
	
	cout << "send => reventis.trackall " << key << " " << x1 << " " << x2
		 << " " << y1 << " " << y2 << " " << startdate << " " << starttime 
		 << " " << enddate << " " << endtime << endl << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.trackall %s %f %f %f %f %s %s %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str());
	int count = 0;
	ProcessReply(c, reply, count);
	cout << "  (" << count << " results)" << endl << endl;
	freeReplyObject(reply);
	return count;
}

int PurgeAllBefore(redisContext *c, const string &key, const string &date, const string &time){

	cout << "send => reventis.purge " << key << " " << date << " " << time << endl << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.purge %s %s %s",
												  key.c_str(), date.c_str(), time.c_str());
	int count;
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		count = reply->integer;
		cout << "  (" << reply->integer << " results)" << endl << endl;
	} else {
		count = 0;
		cout << "ERR - unable to purge" << endl << endl;
	}

	freeReplyObject(reply);
	return count;
}

int DeleteBlock(redisContext *c, const string &key,
				const double x1, const double x2, const double y1, const double y2,
				const string &startdate, const string &starttime,
				const string &enddate, const string &endtime){

	cout << "send => reventis.delblk " << key << " " << x1 << " " << x2
		 << " " << y1 << " " << y2 << " " << startdate << " " << starttime
		 << " " << enddate << " " << endtime << endl << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delblk %s %f %f %f %f %s %s %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str());
	int count;
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		count = reply->integer;
		cout << "  (" << reply->integer << " results)" << endl << endl;
	} else {
		count = 0;
		cout << "ERR - unable to delete block" << endl << endl;
	}
	freeReplyObject(reply);
	return count;
}

int DeleteObject(redisContext *c, const string &key, const long long object_id){

	cout << "send => reventis.delobj " << key << " " << object_id << endl << endl;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delobj %s %lld",
												  key.c_str(), object_id);
	int count = 0;
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		count = reply->integer;
		cout << "  (" << reply->integer << " results)" << endl << endl;
	} else {
		count = 0;
		cout << "ERR - unable to delete object " << object_id << endl;
	}
	cout << endl << endl;
	freeReplyObject(reply);
	return count;
}


void test_add_events(redisContext *c, const string &key){
	const string file = "data.csv";
	const string obj_file = "objects.csv";
	
	int n_original = GetSize(c, key);
	
	int n_events = LoadEvents(c, key, file);
	assert(n_events == 373);

	int n_objects = LoadObjects(c, key, obj_file);
	assert(n_objects == 63);

	int n_total = GetSize(c, key);
	assert(n_total == n_events + n_objects + n_original);
}

void test_add_categories(redisContext *c, const string &key){
	int rc;
	rc = AddCategoryForId(c, key, 11, 1, 1);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 12, 2, 1, 2);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 13, 3, 1, 2, 3);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 317, 2, 10, 20);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 318, 2, 20, 60);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 319, 2, 30, 60);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 320, 3, 40, 50, 64);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 304, 1, 1);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 305, 3, 10, 11, 12);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 306, 2, 10, 25);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 307, 3, 10, 20, 64);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 308, 2, 10, 20);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 309, 2, 10, 50);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 354, 1, 1);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 355, 2, 2, 21);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 356, 1, 3);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 357, 1, 4);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 359, 3, 5, 3, 21);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 360, 1, 6);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 361, 1, 7);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 362, 1, 8);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 363, 1, 9);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 364, 1, 10);
	assert(rc == 0);
	rc = AddCategoryForId(c, key, 365, 5, 11, 10, 20, 24, 25);
	assert(rc == 0);
}

void test_queries(redisContext *c, const string &key){
	string datestr, enddatestr, starttime, endtime;
	
	/* query first cluster */
	double x1 = -72.338000;
	double x2 = -72.330000;
	double y1 = 41.571000;
	double y2 = 41.576000;
	datestr = "12-01-2019";
	starttime = "12:00";
	endtime = "16:00";
	int n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 0);
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 1, 1);
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 1, 2);
	assert(n == 2);
	
	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 1, 3);
	assert(n == 1);

	/* second cluster */
	x1 = -72.218815;
	x2 = -72.217520;
	y1 = 41.719640;
	y2 = 41.720690;
	datestr = "10-09-2019";
	starttime = "12:00";
	endtime = "18:00";
	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 0);
	assert(n == 4);

	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 2, 20, 10);
	assert(n == 2);

	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 2, 60, 40);
	assert(n == 3);
	
	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 1, 10);
	assert(n == 1);

	/* third cluster */
	x1 = -72.257400;
	x2 = -72.248800;
	y1 = 41.806490;
	y2 = 41.809360;
	datestr = "12-04-2019";
	starttime = "9:00";
	endtime = "18:00";
	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 0);
	assert(n == 6);

	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 3, 1, 12, 64);
	assert(n == 3);

	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 2, 50, 1);
	assert(n == 2);
		
	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, datestr, endtime, 2, 20, 1);
	assert(n == 3);

	/* fourth cluster */
	x1 = -106.513485;
	x2 = -93.712109;
	y1 = 27.336248;
	y2 = 36.416949;
	datestr = "01-01-2019";
	starttime = "12:00";
	enddatestr = "01-01-2020";
	endtime = "12:00";
	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, enddatestr, endtime, 0);
	assert(n == 11);

	/* do queries filtered for categories */
	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, enddatestr, endtime, 4, 1, 9, 10, 20);
	assert(n == 4);

	n = Query(c, key, x1, x2, y1, y2, datestr, starttime, enddatestr, endtime, 2, 5, 4);
	assert(n == 2);

}

void test_queries2(redisContext *c, const string &key){
	double x1 = -106.513485, x2 = -93.712109;
	double y1 = 27.336248, y2 = 36.416949;
	string datestr = "01-01-2019";
	string starttime = "12:00";
	string enddatestr = "01-01-2020";
	string endtime = "12:00";
	
	RemoveCategoryForId(c, key, 361, 1, 7);
	int n = Query(c, key, x1, x2, y1, y2, datestr, starttime, enddatestr, endtime, 1, 7);
	assert(n == 0);
}

void test_object_history(redisContext *c, const string &key){

	int n = ObjectHistory(c, key, 150);
	assert(n == 12);

	n = ObjectHistory(c, key, 10);
	assert(n == 17);

	n = ObjectHistory(c, key, 20);
	assert(n == 7);

	n = ObjectHistory(c, key, 30);
	assert(n == 5);

	n = ObjectHistory(c, key, 40);
	assert(n == 6);

	n = ObjectHistory(c, key, 60);
	assert(n == 3);

	n = ObjectHistory(c, key, 70);
	assert(n == 3);

	n = ObjectHistory(c, key, 80);
	assert(n == 5);
}

void test_partial_object_history(redisContext *c, const string &key){

	/* partial object histories */
	string datestr = "05-21-2019", enddatestr;
	string starttime = "12:00";
	string endtime = "15:30";
	int n = ObjectHistoryForTimePeriod(c, key, 150,  datestr, starttime, datestr, endtime);
	assert(n == 3);

	datestr = "08-02-2019";
	starttime = "09:00";
	endtime = "10:15";
	n = ObjectHistoryForTimePeriod(c, key, 10, datestr, starttime, datestr, endtime);
	assert(n == 6);

	datestr = "08-30-2019";
	starttime = "13:00";
	enddatestr = "09-01-2019";
	endtime = "08:00";
	n = ObjectHistoryForTimePeriod(c, key, 80, datestr, starttime, enddatestr, endtime);
	assert(n == 1);

}

void test_delete_objects(redisContext *c, const string &key){
	int n = DeleteObject(c, key, 150);
	assert(n == 12);

	n = DeleteObject(c, key, 70);
	assert(n == 3);
}

void test_purge(redisContext *c, const string &key){
	string datestr = "07-04-2019";
	string starttime = "12:00";
	int n = PurgeAllBefore(c, key, datestr, starttime);
	assert(n == 25);
}

void test_trackall(redisContext *c, const string &key){
	double x1 = -74.00000, x2 = -73.990001;
	double y1 = 40.710000, y2 = 40.730000;
	string datestr = "08-02-2019";
	string starttimestr = "09:00";
	string endtimestr = "17:30";
	int n = TrackAll(c, key, x1, x2, y1, y2, datestr, starttimestr, datestr, endtimestr);
	assert(n == 1);
}

int main(int argc, char **argv){
	const string key = "myevents";
	const string file = "data.csv";
	const string obj_file = "objects.csv";
	const string addr = "localhost";
	const int port = 6379;

	string startdate, enddate, date;
	string starttime, endtime;
	
	cout << "Open connection to " << addr << ":" << port << endl;
	redisContext *c = redisConnect(addr.c_str(), port);
	if (c == NULL || c->err){
		if (c){
			cout << "Error: " << c->errstr << endl;
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
	}

	cout << "Flush" << endl;
	int rc = FlushAllEvents(c, key);
	assert(rc == 0);
	
	cout << "Load Events, Objects" << endl;
	test_add_events(c, key);

	cout << "Add Categories to events" << endl;
	test_add_categories(c, key);

	cout << "Test Queries" << endl;
	test_queries(c, key);

	test_queries2(c, key);

	cout << "TrackALL" << endl;
	test_trackall(c, key);

	cout << "Object Histories" << endl;
	test_object_history(c, key);

	test_partial_object_history(c, key);

	cout << "Delete objects" << endl;
	test_delete_objects(c, key);

	cout << "Purge" << endl;
	test_purge(c, key);
	
	int n = GetSize(c, key);
	assert(n == 396);
	
	redisFree(c);
	return 0;
}
