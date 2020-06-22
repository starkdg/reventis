#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <string>
#include <cstring>
#include <ctime>
#include <cassert>
#include <boost/program_options.hpp>
#include <hiredis.h>

using namespace std;
namespace po = boost::program_options;

struct Args {
	string server, key, file;
	int port;
};

typedef struct entry_t {
	long long id;

	string actors;

	// event attribute fields
	int isroot;
	string eventcode;
	int eventrootcode;
	int quadclass;
	double gscale;
	int n_mentions;
	int n_sources;
	int n_articles;
	double avg_tone;

	// action event
	int geotype;
	string action;
	double longitude, latitude;

	time_t timestamp;
	string source_url;
	
} Entry;

Args ParseOptions(int argc, char **argv){
	Args args;
	po::options_description descr("Reventis Load GDELT Data");
	try {
		descr.add_options()
			("help,h", "produce help message")
			("key,k", po::value<string>(&args.key)->required(), "redis key")
			("server,s", po::value<string>(&args.server)->default_value("localhost"), "redis server address")
			("port,p", po::value<int>(&args.port)->default_value(6379), "redis server port")
			("file,f", po::value<string>(&args.file)->default_value(""), "GDELT events file");

		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(descr).run(), vm);

		if (vm.count("help")){
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

void skip_tabs(stringstream &ss, const int n){
	for (int i=0;i<n;i++){
		string word;
		getline(ss, word, '\t');
	}
}

int parse_gdeltv2_event(string &line, Entry &e){
	string word;
	stringstream ss(line);

	getline(ss, word, '\t');    // global event id
	e.id = atoll(word.c_str());

	skip_tabs(ss, 5);

	getline(ss, word, '\t');
	e.actors += "Actor1: " + word + " ";

	skip_tabs(ss, 9);
	getline(ss, word, '\t');
	e.actors += "Actor2: " + word + " ";

	skip_tabs(ss, 8);

	getline(ss, word, '\t');
	e.isroot = atoi(word.c_str());
	if (!e.isroot) return -1;
	
	getline(ss, word, '\t');
	e.eventcode = word;

	skip_tabs(ss, 1);

	getline(ss, word, '\t');
	e.eventrootcode = atoi(word.c_str());

	getline(ss, word, '\t');
	e.quadclass = atoi(word.c_str());

	getline(ss, word, '\t');
	e.gscale = atof(word.c_str());

	getline(ss, word, '\t');
	e.n_mentions = atoi(word.c_str());

	getline(ss, word, '\t');
	e.n_sources = atoi(word.c_str());

	getline(ss, word, '\t');
	e.n_articles = atoi(word.c_str());

	getline(ss, word, '\t');
	e.avg_tone = atof(word.c_str());

	skip_tabs(ss, 16);

	getline(ss, word, '\t');
	e.geotype = atoi(word.c_str());
	if (e.geotype < 3 || e.geotype > 4) return -2;

	getline(ss, word, '\t');
	e.action += "Action: " + word;

	skip_tabs(ss, 3);

	getline(ss, word, '\t');
	e.latitude = atof(word.c_str());

	getline(ss, word, '\t');
	e.longitude = atof(word.c_str());

	skip_tabs(ss, 1);

	getline(ss, word, '\t');
	stringstream datestr(word);

	tm dt = {};
	datestr >> get_time(&dt, "%Y%m%d%H%M%S");
	e.timestamp = mktime(&dt);
	
	getline(ss, word, '\t');
	e.source_url = word;
	
	return 0;
}

int LoadGdelEvents(redisContext *c, const string &key, const string &file){
	ifstream input(file);

	redisReply *reply;
	const char *cmd_fmt = "reventis.insertwithcat %s %f %f %s %s %s %s %s %d";

	char cmd[512];
	char datestr[32];
	char timestr[32];
	long long count = 0;
	for (string line; getline(input, line);){
		Entry e;
		if (parse_gdeltv2_event(line, e) == 0){
			strftime(datestr, 32, "%m-%d-%Y", gmtime(&e.timestamp));
			strftime(timestr, 32, "%H:%M:%S", gmtime(&e.timestamp));

			string descr = "id = " + to_string(e.id) + " event = " + e.eventcode
				+ " action = " + e.action + " gscale = " +  to_string(e.gscale) +
				" tone = " + to_string(e.avg_tone) + " " + e.source_url;
			
			snprintf(cmd, 512, cmd_fmt, key.c_str(), e.longitude, e.latitude,
					 datestr, timestr, datestr, timestr, descr.c_str(), e.eventrootcode);
			
			cout << "send => " << cmd << endl;
			reply = (redisReply*)redisCommand(c, cmd_fmt, key.c_str(), e.longitude, e.latitude,
											  datestr, timestr, datestr, timestr, descr.c_str(), e.eventrootcode);
			if (reply && reply->type == REDIS_REPLY_INTEGER){
				count++;
				cout << "reply => event id = " << reply->integer << endl;
			} else if (reply && reply->type == REDIS_REPLY_ERROR){
				cout << "ERR - error submitting entry: " << reply->str << endl;
				freeReplyObject(reply);
				return count;
			} else if (reply == NULL){
				cout << "ERR - no reply" << endl;
				return count;
			}
			freeReplyObject(reply);
		}
	
	}
	return count;
}

void print_header(){
	cout << endl << "-------------Reventis Load GDELT Data--------------" << endl << endl;
}

int main(int argc, char **argv){
	print_header();
	Args args = ParseOptions(argc, argv);
	
	cout << "Open connection to " << args.server << ":" << args.port << endl;
	
	redisContext *c = redisConnect(args.server.c_str(), args.port);
	if (c == NULL || c->err){
		if (c) {
			cout << "Error: " << c->errstr << endl;
			redisFree(c);
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
		exit(EXIT_FAILURE);
	}

	cout << "Submit using key = " << args.key << endl << endl;
	cout << "Submit events from " << args.file << endl;
	
	int n = LoadGdelEvents(c, args.key, args.file);
	cout << endl << n  << " events successfully submited" << endl;

	cout << "Done." << endl;

	redisFree(c);
	return n;
}
