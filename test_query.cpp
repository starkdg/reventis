#include <cstdlib>
#include <iostream>
#include <string>
#include <cassert>
#include "hiredis.h"

using namespace std;

void process_reply(redisContext *c, redisReply *reply, int &count){
	
	if (reply && reply->type == REDIS_REPLY_ARRAY){
		for (int i=0;i<reply->elements;i++){
			process_reply(c, reply->element[i], count);
		}
	} else if (reply && reply->type == REDIS_REPLY_STRING){
		count++;
		cout << "(" << count << ") " << reply->str << endl;
	} else if (reply && reply->type == REDIS_REPLY_ERROR){
		cout << reply->str << endl;
	} else {
		cout << "Error: " << c->errstr << endl;
	}

	return;
}


int main(int argc, char **argv){

	string cmdstr = "reventis.query";
	string key = "myevents";
	double x1, y1, x2, y2;
	string startdatetime, enddatetime;
	int count = 0;

	string cmdlinestr = cmdstr + " %s %f %f %f %f %s %s";
	
	redisContext *c = redisConnect("localhost", 6379);
	if (c == NULL || c->err){
		if (c) cout << "Error: " << c->errstr << endl;
		else cout << "Unable to allocate redis context" << endl;
	}

	redisReply *reply;


	x1 = -72.338000;
	x2 = -72.330000;
	y1 = 41.571000;
	y2 = 41.576000;
	startdatetime = "2019-12-01T12:00";
	enddatetime = "2019-12-01T16:00";

	cout << "query: " << cmdstr << " " << key << " " << x1 << " " << x2 << " " << y1 << " " << y2 << " "
		 << startdatetime << " " << enddatetime << " " << endl;
	
	reply = (redisReply*)redisCommand(c, cmdlinestr.c_str(), key.c_str(), x1, x2, y1, y2,
									  startdatetime.c_str(), enddatetimestr.c_str());
	count = 0;
	process_reply(c, reply, count);
	freeReplyObject(reply);

	cout << "Total = " << count << endl << endl;
	
	x1 = -72.257400;
	x2 = -72.248800;
	y1 = 41.806490;
	y2 = 41.809360;
	startdatetime = "2019-12-04T09:00";
	enddatetime = "2019-12-04T18:00";

	cout << "query: " << cmdstr << " " << key << " " << x1 << " " << x2 << " " << y1 << " " << y2
		 << startdatetime << " " <<  enddatetime << endl;

	reply = (redisReply*)redisCommand(c, cmdlinestr.c_str(), key.c_str(), x1, x2, y1, y2,
									  startdatetime.c_str(), enddatetime.c_str());
	count = 0;
	process_reply(c, reply, count);
	freeReplyObject(reply);

	cout << "Total = " << count << endl << endl;
	
	x1 = -72.218815;
	x2 = -72.217520;
	y1 = 41.719640;
	y2 = 41.720690;
	startdatetime = "2019-10-09T12:00";
	enddatetime = "2019-10-09T18:00";

	cout << "query: " << cmdstr << " " << key << " " << x1 << " " << x2 << " " << y1 << " " << y2
		 << startdatetime << " " << enddatetime << endl;
	
	reply = (redisReply*)redisCommand(c, cmdlinestr.c_str(), key.c_str(), x1, x2, y1, y2,
									  startdatetime.c_str(), enddatetime.c_str());
	count = 0;
	process_reply(c, reply, count);
	freeReplyObject(reply);

	cout << "Total = " << count << endl << endl;
	
	x1 = -73.514076;
	x2 = -71.793121;
	y1 = 41.187130;
	y2 = 42.036561;
	startdatetime = "2019-01-01T12:00";
	enddatetime = "2020-01-01T12:00";
	
	cout << "query: " << cmdstr << " " << key << " " << x1 << " " << x2 << " " << y1 << " " << y2
		 << startdatetime << " " << enddatetime << endl;

	
	reply = (redisReply*)redisCommand(c, cmdlinestr.c_str(), key.c_str(), x1, x2, y1, y2,
									  startdatetime.c_str(), enddatetime.c_str());
	count = 0;
	process_reply(c, reply, count);
	freeReplyObject(reply);

	cout << "Total = " << count << endl << endl;
	
	redisFree(c);
	return 0;
}

