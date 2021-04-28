#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <random>
#include <cassert>
#include <unistd.h>
#include <boost/program_options.hpp>
#include "hiredis.h"

using namespace std;
namespace po = boost::program_options;

struct Args {
	string cmd, server, key, file;
	int port, n;
}; 


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

Args ParseOptions(int argc, char **argv){
	Args args;
	po::options_description descr("Reventis Client Options");
	try {
		descr.add_options()
			("help,h", "produce help message")
			("key,k", po::value<string>(&args.key)->required(), "redis key string")
			("cmd,c", po::value<string>(&args.cmd)->required(), "command: events, objects, gen")
			("server,s", po::value<string>(&args.server)->default_value("localhost"),
			 "reids server hostname or unix socket path - e.g. localhost or 127.0.0.1")
			 ("port,p", po::value<int>(&args.port)->default_value(6379), "redis server port")
			("file,f", po::value<string>(&args.file)->default_value(""), "events file or objects file")
			("num,n", po::value<int>(&args.n)->default_value(1000), "number of random events to add");

		po::positional_options_description pd;
		pd.add("cmd", 1);

		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(descr).positional(pd).run(), vm);

		if (vm.count("help") || args.cmd == "help"){
			cout << descr << endl;
			exit(0);
		}
		po::notify(vm);
		
	} catch (const po::error &ex){
		cout << descr << endl;
		exit(0);
	}

	return args;
}


int genlonglat(double &x, double &y){
	x = distr1(gen);
	y = distr2(gen);
	return 0;
}

int gentimes(string &startdatetime, string &enddatetime){

	const char *datetime_fmt = "%Y-%m-%dT%H:%M:00";
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

	strftime(buffer, 64, datetime_fmt, gmtime(&t1));
	startdatetime = string(buffer);

	strftime(buffer, 64, datetime_fmt, gmtime(&t2));
	enddatetime = string(buffer);

	if (t2 < t1) return -1;
	
	return 0;
}

int GenerateEvents(redisContext *c, const string &key, const int n){
	int count = 0;
	redisReply *reply = NULL;
	const char *cmd_fmt = "reventis.insert %s %f %f %s %s %s";
	
	while (count < n){
		double x,  y;
		genlonglat(x, y);

		string dt1, dt2;
		if (gentimes(dt1, dt2) < 0)
			continue;

		long long tag_num = id_distr(gen);
		
		string descr = "\"Event ID # " + to_string(tag_num) + "\"";

		reply = (redisReply*)redisCommand(c, cmd_fmt,
										  key.c_str(),
										  x, y,
										  dt1.c_str(), dt2.c_str(),
										  descr.c_str());

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
	string startdatetime;
	string enddatetime;
	string descr;
} Entry;

int parse_event(string line, Entry &entry){
	string word;
	stringstream ss(line);
	getline(ss, word, ',');
	entry.latitude = atof(word.c_str());
	getline(ss, word, ',');
	entry.longitude = atof(word.c_str());

	string datestr, startstr, endstr;
	getline(ss, datestr, ',');
	getline(ss, startstr, ',');
	getline(ss, endstr, ',');
	entry.startdatetime = datestr + "T" + startstr;
	entry.enddatetime = datestr + "T" + endstr;
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
	getline(ss, word, ',');
	entry.startdatetime = word;
	entry.enddatetime = word;
	getline(ss, word, ',');
	entry.descr = word;
	return 0;
}

int LoadEvents(redisContext *c, const string &key, const string &file){
	ifstream input(file);
	redisReply *reply;
	const char *cmd_fmt = "reventis.insert %s %f %f %s %s %s";

	char cmd[128];
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_event(line, e);

		snprintf(cmd, 128, cmd_fmt, key.c_str(), e.longitude, e.latitude,
				 e.startdatetime.c_str(), e.enddatetime.c_str(), e.descr.c_str());

		cout << "send => " << cmd << endl;
		reply = (redisReply*)redisCommand(c, cmd_fmt, key.c_str(),
										  e.longitude, e.latitude,
										  e.startdatetime.c_str(), e.enddatetime.c_str(),
										  e.descr.c_str());

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
	const char *cmd_fmt = "reventis.update %s %f %f %s %ld %s";
	
	char cmd[128];
	int count = 0;
	for (string line; getline(input, line);){
		Entry e;
		parse_object(line, e);

		snprintf(cmd, 128, cmd_fmt, key.c_str(), e.longitude, e.latitude,
				 e.startdatetime.c_str(), e.object_id, e.descr.c_str());

		cout << "send => " << cmd << endl;
		reply = (redisReply*)redisCommand(c, cmd_fmt, key.c_str(), e.longitude, e.latitude,
										  e.startdatetime.c_str(), e.object_id, e.descr.c_str());
		
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

void print_header(){
	cout << endl << "------------Reventis Client------------" << endl << endl;
}


int main(int argc, char **argv){
	print_header();

	Args args = ParseOptions(argc, argv);
	
	cout << "Open connection to " << args.server << ":" << args.port << endl;
	redisContext *c = redisConnect(args.server.c_str(), args.port);
	if (c == NULL || c->err){
		if (c){
			cout << "Error: " << c->errstr << endl;
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
		exit(0);
	}

	cout << "Submit using key = " << args.key << endl << endl;
	
	if (args.cmd.compare("events") == 0){
		cout << "Submit events from " << args.file << endl;
		int n_events = LoadEvents(c, args.key, args.file);
		cout << endl << n_events << " events successfully submitted" << endl;
	} else if (args.cmd.compare("objects") == 0){
		cout << "Submit objects from " << args.file << endl;
		int n_objects = LoadObjects(c, args.key, args.file);
		cout << endl << n_objects << " objects successfully submitted" << endl;
	} else if (args.cmd.compare("gen") == 0){
		cout << "Submit "  <<  args.n << " randomly generated events" << endl;
		int n_random = GenerateEvents(c, args.key, args.n);
		cout << endl << n_random << " events successfully submitted" << endl;
	} else {
		cout << endl << "Unrecognized option: " << args.cmd << endl;
	}
	
	cout << "Done." << endl;
		
	redisFree(c);
	return 0;
}
