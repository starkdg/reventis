#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <ctime>
#include <algorithm>
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
	trim(entry.descr);
	trim(entry.date);
	trim(entry.starttime);
	trim(entry.endtime);
	return 0;
}

int main(int argc, char **argv){
	if (argc < 3){
		cout << "not enough args" << endl;
		cout << "./load <tree> <file>" << endl;
		exit(0);
	}

	const string tree = argv[1];
	const string file = argv[2];

	cout << "Add file, " << file << " to tree, " << tree << endl;

	ifstream input(file);

	redisContext *c = redisConnect("localhost", 6379);
	if (c == NULL || c->err){
		if (c){
			cout << "Error: " << c->errstr << endl;
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
	}

	redisReply *reply;
	
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_entry(line, e);

		cout << "(" << count << ") " << e.descr << " " << e.date << " "
			 << e.starttime << " " << " to " << e.date << " " << e.endtime
			 << " " << e.longitude << "/" << e.latitude << endl;
		reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s %s %s",
							 tree.c_str(), e.longitude, e.latitude,
										  e.date.c_str(), e.starttime.c_str(),
										  e.date.c_str(), e.endtime.c_str(), e.descr.c_str());

		if (reply && reply->type == REDIS_REPLY_INTEGER){
			cout << "eventid: " << reply->integer << endl;
		} else if (reply && reply->type == REDIS_REPLY_STRING){
			cout << reply->str << endl;
		} else {
			cout << "Error: " << c->errstr << endl;
			break;
		}

		freeReplyObject(reply);
		count++;
	}

	redisFree(c);
	return 0;
}
