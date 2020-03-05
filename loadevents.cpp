#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <cassert>
#include <unistd.h>
#include "hiredis.h"

using namespace std;

static random_device rd;
static mt19937 gen(rd());
static uniform_real_distribution<> distr1(-180.0, 180.0);
static uniform_real_distribution<> distr2(-90.0, 90.0);
static uniform_int_distribution<> month_distr(0, 11);
static uniform_int_distribution<> day_distr(1, 31);
static uniform_int_distribution<> year_distr(116, 124);
static uniform_int_distribution<> hour_distr(0, 23);
static uniform_int_distribution<> minute_distr(0, 59);
static uniform_int_distribution<unsigned int> dur_distr(60*15, 3600*5);
static uniform_int_distribution<> id_distr(1000000, 20000000);

int genlonglat(double &x, double &y){
	x = distr1(gen);
	y = distr2(gen);
	return 0;
}

int gentimes(string &startdate, string &starttime, string &enddate, string &endtime){

	const char *date_fmt = "%m-%d-%Y";
	const char *time_fmt = "%H:%M";
	char buffer[64];
	
	time_t t1, t2;

	tm start;
	start.tm_sec = 0;
	start.tm_min = minute_distr(gen);
	start.tm_hour = hour_distr(gen);

	start.tm_mday = day_distr(gen);
	start.tm_mon = month_distr(gen);
    start.tm_year = year_distr(gen);
	
	start.tm_isdst = 0;
	t1 = mktime(&start);

	unsigned int dur = dur_distr(gen);
	t2 = t1 + (time_t)dur;

	strftime(buffer, 64, date_fmt, localtime(&t1));
	startdate = string(buffer);

	strftime(buffer, 64, time_fmt, localtime(&t1));
	starttime = string(buffer);

	strftime(buffer, 64, date_fmt, localtime(&t2));
	enddate = string(buffer);

	strftime(buffer, 64, time_fmt, localtime(&t2));
	endtime = string(buffer);

	if (t2 < t1) return -1;
	
	return 0;
}

int GenerateEvents(redisContext *c, const string &key, const int n){
	char cmd[128];
	int count = 0;
	redisReply *reply = NULL;

	while (count < n){
		double x,  y;
		genlonglat(x, y);

		string d1, t1, d2, t2;
		if (gentimes(d1, t1, d2, t2) < 0)
			continue;

		long long tag_num = id_distr(gen);
		
		string descr = "\"Event ID # " + to_string(tag_num) + "\"";

		snprintf(cmd, 128, "reventis.insert %s %f %f %s %s %s %s %s",
				 key.c_str(), x, y, d1.c_str(), t1.c_str(),
				 d2.c_str(), t2.c_str(), descr.c_str());

		reply = (redisReply*)redisCommand(c, cmd);

		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
		} else if (reply && reply->type == REDIS_REPLY_STRING){
			continue;
		} else if (reply && reply->type == REDIS_REPLY_ERROR){
			continue;
		} else if (reply == NULL){
			cout << "Error no reply";
			freeReplyObject(reply);
			break;
		}
		freeReplyObject(reply);
	}
	return count;
}


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


int LoadEvents(redisContext *c, const string &key, const string &file){
	ifstream input(file);
	redisReply *reply;
	
	char cmd[128];
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_event(line, e);

		snprintf(cmd, 128, "reventis.insert %s %f %f %s %s %s %s %s",
				 key.c_str(), e.longitude, e.latitude, e.date.c_str(), e.starttime.c_str(),
				 e.date.c_str(), e.endtime.c_str(), e.descr.c_str());

		cout << "send => " << cmd << endl;
		reply = (redisReply*)redisCommand(c, cmd);

		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
			cout << "reply => event id = " << reply->integer << endl;
		}  else if (reply && reply->type == REDIS_REPLY_ERROR){
			cout << "ERR - error submitting entry: " << reply->str << endl;
			freeReplyObject(reply);
			return count;
		} else if (reply == NULL){
			cout << "ERR - no reply" << endl;
			freeReplyObject(reply);
			return count;
		} 
		freeReplyObject(reply);
	}

	return count;
}

int LoadObjects(redisContext *c, const string &key, const string &file){
	ifstream input(file);
	redisReply *reply;

	char cmd[128];
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_object(line, e);

		snprintf(cmd, 128, "reventis.update %s %f %f %s %s %ld %s",
				 key.c_str(), e.longitude, e.latitutde,
				 e.date.c_str(), e.starttime.c_str(),
				 e.object_id, e.descr.c_str());

		cout << "send => " << cmd << endl;
		reply = (redisReply*)redisCommand(c, cmd);
		if (reply && reply->type == REDIS_REPLY_INTEGER){
			count++;
			cout << "reply => event id = " << reply->integer << endl;
		} else if (reply && reply->type == REDIS_REPLY_ERROR){
			cout << "error: " << reply->str << endl;
			return count;
		} else if (reply == NULL){
			cout << "ERR - no reply" << endl;
			freeReplyObject(reply);
			return count;
		}
		freeReplyObject(reply);
	}
	cout << "Loaded " << count << " objects from " << file << endl;
	return count;
}


int main(int argc, char **argv){
	if (argc < 4){
		cout << "not enough args" << endl;
		cout << "./load key file [events|objects|gen] [n]" << endl;
		cout << "file - file containing events or objects" << endl;
		cout << "opt  - events - file contains events" << endl;
		cout << "       objects - file contains objects" << endl;
		cout << "       gen     - randomly generate n events" << endl;
		cout << "n    - number of events to randomly generate (optional)" << endl;
		exit(0);
	}

	const string key = argv[1];
	const string file = argv[2];
	const string type = argv[3];
	const int n_generated = (argc >= 5) ? atoi(argv[4] : 100);

	cout << "Open connection to " << addr << ":" << port << endl;
	redisContext *c = redisConnect(addr.c_str(), port);
	if (c == NULL || c->err){
		if (c){
			cout << "Error: " << c->errstr << endl;
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
	}

	cout << "Submit using key = " << key << endl << endl;
	
	if (!type.compare("events")){
		cout << "Submit events from " << file << endl;
		int n_events = LoadEvents(c, key, file);
		cout << n_events << " successfully submitted" << endl;
	} else if (!type.compare("objects")){
		cout << "Submit objects from " << file << endl;
		int n_objects = LoadObjects(c, key, file);
		cout << n_objects << " successfully submitted" << endl;
	} else if (!type.compare("gen")){
		cout << "Submit "  <<  n_generated << " randomly generated events" << endl;
		int n_random = GenerateEvents(c, key);
		cout << n_random << " successfully submitted" << endl;
	} else {
		cout << "Unrecognized option: " << type << endl;
	}
	
	cout << "Done." << endl;
		
	redisFree(c);
	return 0;
}
