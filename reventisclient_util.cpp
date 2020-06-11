#include "reventisclient_util.hpp"
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdarg>
#include <cstring>

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

static int parse_event(string line, Entry &entry){
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
	
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_event(line, e);
		redisReply *reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s %s %s",
							 key.c_str(), e.longitude, e.latitude,
										  e.date.c_str(), e.starttime.c_str(),
										  e.date.c_str(), e.endtime.c_str(), e.descr.c_str());

		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
		}  else {
			throw runtime_error("bad reply");
		}
		freeReplyObject(reply);
	}

	return count;
}

static int parse_object(string line, Entry &entry){
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
		} else {
			throw runtime_error("bad reply");
		}
		freeReplyObject(reply);
	}

	return count;
}



long long AddEvent(redisContext *c, const string &key,
				   const double x, const double y,
				   const string &startdate, const string &starttime,
				   const string &enddate, const string &endtime, const string &descr){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s %s %s",
												  key.c_str(), x, y,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str(), descr.c_str());
    long long id = 0;
	if (reply && reply->type == REDIS_REPLY_INTEGER){
	    id = reply->integer;
	} else {
		throw runtime_error("bad reply");
	}
	freeReplyObject(reply);
	return id;
}

void AddCategory(redisContext *c, const string &key, const long long id, int n, ...){
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
	if (reply == NULL || reply->type != REDIS_REPLY_STATUS){
		throw runtime_error("bad reply");
	}

	freeReplyObject(reply);
}

void RemoveCategory(redisContext *c, const string &key, const long long id, int n, ...){
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
	if (reply == NULL || reply->type != REDIS_REPLY_STATUS){
		throw runtime_error("bad reply");
	}

	freeReplyObject(reply);
}

int GetSize(redisContext *c, const string &key){
	int n = 0;;
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.size %s", key.c_str());
	if (reply && reply->type == REDIS_REPLY_INTEGER){
		n = reply->integer;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		n = 0;
	} else {
		throw runtime_error("bad reply");
	}
	freeReplyObject(reply);
	return n;
}


void FlushAll(redisContext *c, const string &key){
	redisReply *reply = (redisReply*)redisCommand(c, "flushall");
	if (reply == NULL || reply->type != REDIS_REPLY_STATUS){
		throw runtime_error("bad reply");
	}
	freeReplyObject(reply);
}


static void ProcessSubReply(redisReply *reply){
	if (reply && reply->type == REDIS_REPLY_ARRAY){
		for (int i=0;i < reply->elements;i++){
			ProcessSubReply(reply->element[i]);
		}
	} else if (reply && reply->type == REDIS_REPLY_STRING){
		cout << "  " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_INTEGER){
		cout << "  " << reply->integer << endl;
	} else if (reply && reply->type == REDIS_REPLY_STATUS){
		cout << "  " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << "  " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_NIL){
		cout << "  " << "NIL" << endl;
	}
}


static int ProcessArrayReply(redisReply *reply){
	int count = 0;
	if (reply && reply->type == REDIS_REPLY_ARRAY){
		for (int i=0;i<reply->elements;i++){
			cout << "(" << i+1 << ")--------" << endl;
			ProcessSubReply(reply->element[i]);
			count++;
		}
		cout << "------------------" << endl;
	} else {
		throw runtime_error("bad reply");
	}
	return count;
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

	redisReply *reply = (redisReply*)redisCommand(c, cmd);
	int count = ProcessArrayReply(reply);
	cout << "( " << count << " results)" << endl;
	freeReplyObject(reply);
	return count;
}


int ObjectHistory(redisContext *c, const string &key, const long long object_id){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.hist %s %lld",
												  key.c_str(), object_id);
	int count = ProcessArrayReply(reply);
	cout << "( " << count << " objects)" << endl;
	freeReplyObject(reply);
	return count;
}

int ObjectHistory2(redisContext *c, const string &key, const long long object_id,
				  const string &d1, const string &t1, const string &d2, const string &t2){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.hist %s %lld %s %s %s %s",
												  key.c_str(), object_id,
												  d1.c_str(), t1.c_str(),
												  d2.c_str(), t2.c_str());
	int count = ProcessArrayReply(reply);
	cout <<  "  (" << count << " objects)" << endl;
	freeReplyObject(reply);
	return count;
}

int TrackAll(redisContext *c, const string &key,
			 const double x1, const double x2,
			 const double y1, const double y2,
			 const string &startdate, const string &starttime,
			 const string &enddate, const string &endtime){
	
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.trackall %s %f %f %f %f %s %s %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str());
	int count = ProcessArrayReply(reply);
	cout << "  (" << count << " objects)" << endl;
	freeReplyObject(reply);
	return count;
}


int PurgeAllBefore(redisContext *c, const string &key, const string &date, const string &time){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.purge %s %s %s",
												  key.c_str(), date.c_str(), time.c_str());
	int count = -1;
	if (reply == NULL || reply->type != REDIS_REPLY_INTEGER){
		throw runtime_error("bad reply");
	}
	count = reply->integer;
	freeReplyObject(reply);
	return count;
}

int DeleteBlock(redisContext *c, const string &key,
				const double x1, const double x2, const double y1, const double y2,
				const string &startdate, const string &starttime,
				const string &enddate, const string &endtime){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delblk %s %f %f %f %f %s %s %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdate.c_str(), starttime.c_str(),
												  enddate.c_str(), endtime.c_str());
	int count;
	if (reply == NULL || reply->type != REDIS_REPLY_INTEGER){
		throw runtime_error("bad reply");
	} 
	count = reply->integer;
	freeReplyObject(reply);
	return count;
}

int DeleteObject(redisContext *c, const string &key, const long long object_id){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delobj %s %lld",
												  key.c_str(), object_id);
	int count = 0;
	if (reply == NULL || reply->type != REDIS_REPLY_INTEGER){
		throw runtime_error("bad reply");
	}
	count = reply->integer;
	freeReplyObject(reply);
	return count;
}

void DeleteEvent(redisContext *c, const string &key, const long long event_id){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.del %s %lld",
												  key.c_str(), event_id);
	if (reply == NULL || reply->type == REDIS_REPLY_ERROR){
		throw runtime_error(reply->str);
	}
}






