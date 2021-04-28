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
	string startdatetime;
	string enddatetime;
	string descr;
} Entry;

static int parse_event(string line, Entry &entry){
	string word;
	stringstream ss(line);
	getline(ss, word, ',');
	entry.latitude = atof(word.c_str());
	getline(ss, word, ',');
	entry.longitude = atof(word.c_str());

	string datestr, t1str, t2str;
	getline(ss, datestr, ',');
	getline(ss, t1str, ',');
	getline(ss, t2str, ',');

	entry.startdatetime = datestr + "T" + t1str;
	entry.enddatetime = datestr + "T" + t2str;
	
	getline(ss, entry.descr);
	return 0;
}

int LoadEvents(redisContext *c, const string &key, const string &file){
	ifstream input(file);
	
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_event(line, e);
		redisReply *reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s",
							 key.c_str(), e.longitude, e.latitude,
							 e.startdatetime.c_str(), e.enddatetime.c_str(), e.descr.c_str());

		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
		}  else if (reply && reply->type == REDIS_REPLY_ERROR){
			throw runtime_error(reply->str);
		}  else {
			throw runtime_error("bad reply - no context");
		}
		freeReplyObject(reply);
	}

	return count;
}

static int parse_object(string line, Entry &entry){
	string word;
	stringstream ss(line);
	getline(ss, word, ',');
	entry.object_id = atoll(word.c_str());
	getline(ss, word, ',');
	entry.longitude = atof(word.c_str());
	getline(ss, word, ',');
	entry.latitude = atof(word.c_str());

	string dt;
	getline(ss, dt, ',');
	entry.startdatetime = dt;
	entry.enddatetime = dt;

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
		reply = (redisReply*)redisCommand(c, "reventis.update %s %f %f %s %lld %s",
										  key.c_str(), e.longitude, e.latitude,
										  e.startdatetime.c_str(),
										  e.object_id,
										  e.descr.c_str());
		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
		} else if (reply && reply->type == REDIS_REPLY_ERROR){
			throw runtime_error(reply->str);

		} else {
			throw runtime_error("bad reply");
		}
		freeReplyObject(reply);
	}

	return count;
}



long long AddEvent(redisContext *c, const string &key,
				   const double x, const double y,
				   const string &startdatetime, const string &enddatetime,
				   const string &descr){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s",
												  key.c_str(), x, y,
												  startdatetime.c_str(), enddatetime.c_str(),
												  descr.c_str());
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


void DeleteKey(redisContext *c, const string &key){
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delkey %s", key.c_str());
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
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << "Error - " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_STRING){
		cout << "Str - " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_STATUS){
		cout << "Status - " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_INTEGER){
		cout <<  reply->integer << endl;
	} else {
		throw runtime_error("bad reply");
	}
	return count;
}


int Query(redisContext *c, const string &key,
		  const double x1, const double x2,
		  const double y1, const double y2,
		  const string &startdatetime, 
		  const string &enddatetime, int n, ...){

	char cmd[256];
	snprintf(cmd, 256, "reventis.query %s %f %f %f %f %s %s",
			 key.c_str(), x1, x2, y1, y2,
			 startdatetime.c_str(), enddatetime.c_str());

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


int QueryByRadius(redisContext *c, const string &key,
				  const double x, const double y, const double radius,
				  const string &startdatetime, 
				  const string &enddatetime, int n, ...){
	char cmd[256];
	snprintf(cmd, 256, "reventis.queryradius %s %f %f %f %s %s %s",
			 key.c_str(), x, y, radius, "km", startdatetime.c_str(), enddatetime.c_str());
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
				  const string &t1, const string &t2){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.hist %s %lld %s %s",
												  key.c_str(), object_id,
												  t1.c_str(), t2.c_str());
												 
	int count = ProcessArrayReply(reply);
	cout <<  "  (" << count << " objects)" << endl;
	freeReplyObject(reply);
	return count;
}

int TrackAll(redisContext *c, const string &key,
			 const double x1, const double x2,
			 const double y1, const double y2,
			 const string &startdatetime, const string &enddatetime){
	
	redisReply *reply = (redisReply*)redisCommand(c, "reventis.trackall %s %f %f %f %f %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdatetime.c_str(), enddatetime.c_str());
	int count = ProcessArrayReply(reply);
	cout << "  (" << count << " objects)" << endl;
	freeReplyObject(reply);
	return count;
}


int PurgeAllBefore(redisContext *c, const string &key, const string &datetime){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.purge %s %s",
												  key.c_str(), datetime.c_str());
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
				const string &startdatetime, const string &enddatetime){

	redisReply *reply = (redisReply*)redisCommand(c, "reventis.delblk %s %f %f %f %f %s %s",
												  key.c_str(), x1, x2, y1, y2,
												  startdatetime.c_str(), enddatetime.c_str());
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






