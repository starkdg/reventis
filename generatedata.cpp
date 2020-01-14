#include <cstdlib>
#include <random>
#include <iostream>
#include <iomanip>
#include <limits>
#include <ctime>
#include <string>
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

static uniform_int_distribution<> dur_distr(60*15, 3600*5);

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
	t2 = t1 + dur_distr(gen);


	strftime(buffer, 64, date_fmt, localtime(&t1));
	startdate = string(buffer);


	strftime(buffer, 64, time_fmt, localtime(&t1));
	starttime = string(buffer);

	strftime(buffer, 64, date_fmt, localtime(&t2));
	enddate = string(buffer);

	strftime(buffer, 64, time_fmt, localtime(&t2));
	endtime = string(buffer);
	
	return 0;
}

int main(int argc, char **argv){
	if (argc < 3){
		cout << "not enough args" << endl;
		cout << "./gen key count" << endl;
		exit(0);
	}

	const string key = argv[1];
	const int n = atoi(argv[2]);

	redisContext *c = redisConnect("localhost", 6379);
	if (c == NULL || c->err){
		if (c){
			cout << "Error: " << c->errstr << endl;
		} else {
			cout << "Unable to allocate redis context" << endl;
		}
	}

	cout << "Add " << n << " random entries to " << key << endl << endl;

	redisReply *reply = NULL;
	for (int i=0;i<n;i++){
		double x,  y;
		genlonglat(x, y);


		string s1, t1, s2, t2;
		gentimes(s1, t1, s2, t2);

		long long idnum = id_distr(gen);

		string descr = "\"Event ID # " + to_string(idnum) + "\"";
		
		cout << key << "    " << x << "    " << y << "    "
			 << s1 << "    "  <<  t1 << "    "
			 << s2 << "    " << t2 << "    " 
			 << descr  << endl;


		reply = (redisReply*)redisCommand(c, "reventis.insert %s %f %f %s %s %s %s %s",
										  key.c_str(), x, y,
										  s1.c_str(), t1.c_str(), s2.c_str(), t2.c_str(),
										  descr.c_str());
		if (reply && reply->type == REDIS_REPLY_INTEGER){
			cout << "eventid: " << reply->integer << endl;
		} else if (reply && reply->type == REDIS_REPLY_STRING){
			cout << reply->str << endl;
		} else {
			cout << "Error: " << c->errstr << endl;
			break;
		}
	}

	freeReplyObject(reply);
	cout << "done" << endl;
	return 0;
}
