#include <cstdlib>
#include <iostream>
#include <string>
#include <cstring>
#include "hiredis.h"

using namespace std;

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


int main(int argc, char **argv){
	if (argc < 10){
		cout << "not enough args" << endl;
		cout << "./queryevents key longitude1 longitude2 latitude1 latitude2 start-datetime end-datetime" << endl;
		exit(0);
	}

	redisContext *c = redisConnect("localhost", 6379);
	if (c == NULL || c->err){
		if (c){
			cout << "Error: " << c->errstr << endl;
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
		exit(EXIT_FAILURE);
	}
	
	const string key = argv[1];
	const double x1 = atof(argv[2]);
	const double x2 = atof(argv[3]);
	const double y1 = atof(argv[4]);
	const double y2 = atof(argv[5]);
	const string startdate = argv[6];
	const string starttime = argv[7];
	const string enddate = argv[8];
	const string endtime = argv[9];

	int n = Query(c, key, x1, x2, y1, y2, startdate, starttime, enddate, endtime, 0);
	cout << "items found: " << n << endl;
	cout << "done." << endl;

	redisFree(c);
	return 0;
}
