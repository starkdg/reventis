#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <ctime>
#include <cassert>
#include "reventisclient_util.hpp"
#include "hiredis.h"

using namespace std;


void TestAddEvents(redisContext *c, const string &key){
	const string event_file = "data.csv";
	
	int n_original = GetSize(c, key);

	int n_events = LoadEvents(c, key, event_file);
	assert(n_events == 373);

	int n_total = GetSize(c, key);
	assert(n_total == n_events + n_original);
}

void TestAddObjects(redisContext *c, const string &key){
	const string obj_file = "objects.csv";
	
	int n_original = GetSize(c, key);
	
	int n_objects = LoadObjects(c, key, obj_file);
	assert(n_objects == 63);

	int n_total = GetSize(c, key);
	assert(n_total == n_objects + n_original);
}

void TestQuery(redisContext *c, const string &key){
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

}

void TestQueryByRadius(redisContext *c, const string &key){
	double x = -72.334000, y =  41.5735, radius = 2.0;
	string startdatestr = "12-01-2019";
	string starttimestr = "12:00";
	string enddatestr = "12-01-2019";
	string endtimestr = "16:00";
	int n = QueryByRadius(c, key, x, y, radius, startdatestr, starttimestr, enddatestr, endtimestr, 0);
	assert(n == 3);

	x = -72.218167;
	y = 41.720165;
	startdatestr = "10-09-2019";
	enddatestr = "10-09-2019";
	starttimestr = "12:00";
	endtimestr = "18:00";
	radius = 2.0;
	n = QueryByRadius(c, key, x, y, radius, startdatestr, starttimestr, enddatestr, endtimestr, 0);
	assert(n == 4);

	x = -72.253100;
	y = 41.807925;
	startdatestr = "12-04-2019";
	enddatestr = "12-04-2019";
	starttimestr = "9:00";
	endtimestr = "18:00";
	radius = 10.0;
	n = QueryByRadius(c, key, x, y, radius, startdatestr, starttimestr, enddatestr, endtimestr, 0);
	assert(n == 6);

	x = -100.112797;
	y = 31.876599;
	startdatestr = "01-01-2019";
	enddatestr = "01-01-2020";
	starttimestr = "12:00";
	endtimestr = "12:00";
	radius = 400;
	n = QueryByRadius(c, key, x, y, radius, startdatestr, starttimestr, enddatestr, endtimestr, 0);
	assert(n == 11);
}

void TestAddEventsCategoriesAndQuery(redisContext *c, const string &key){
	double x = -97.797586;
	double y = 30.318388;
	string datestr = "05-25-2019";
	string startstr = "13:00";
	string endstr  =  "13:30";
	string descr = "class 10";
	long long id1 = AddEvent(c, key, x, y, datestr, startstr, datestr, endstr, descr);

	AddCategory(c, key, id1, 1, 10);


	x = -97.715113;
	y = 30.333895;
	datestr = "05-01-2019";
	startstr = "14:30";
	endstr = "14:45";
	descr = "class 10, 20";

	long long id2 = AddEvent(c, key, x, y, datestr, startstr, datestr, endstr, descr);

	AddCategory(c, key, id2, 2, 10, 20);


	x = -97.816367;
	y = 30.206953;
	datestr = "04-01-2019";
	startstr = "23:00";
	endstr = "23:05";
	descr = "class 10, 20, 30";

	long long id3 = AddEvent(c, key, x, y, datestr, startstr, datestr, endstr, descr);

	AddCategory(c, key, id3, 3, 10, 20, 30);


	double x1 = -106.513485;
	double x2 = -93.712109;
	double y1 = 27.336248;
	double y2 = 36.416949;
	string startdatestr = "03-25-2019";
	string starttimestr = "12:00";
	string enddatestr = "06-01-2019";
	string endtimestr = "12:00";
	
	int n;
	n = Query(c, key, x1, x2, y1, y2, startdatestr, starttimestr, enddatestr, endtimestr, 1, 10);
	assert(n == 3);
	
	n = Query(c, key, x1, x2, y1, y2, startdatestr, starttimestr, enddatestr, endtimestr, 1, 20);
	assert(n == 2);

	n = Query(c, key, x1, x2, y1, y2, startdatestr, starttimestr, enddatestr, endtimestr, 1, 30);
	assert(n == 1);

	RemoveCategory(c, key, id3, 3, 10, 20, 30);

	n = Query(c, key, x1, x2,  y1, y2, startdatestr, starttimestr, enddatestr, endtimestr, 1, 10);
	assert(n == 2);

	cout << "delete id1 " << id1 << endl;
	DeleteEvent(c, key, id1);

	cout << "delete id2 " << id2 << endl;
	DeleteEvent(c, key, id2);

	cout << "delete id3 " << id3 << endl;
	DeleteEvent(c, key, id3);

	cout << "query" << endl;
	n = Query(c, key, x1, x2, y1, y2, startdatestr, starttimestr, enddatestr, endtimestr, 3, 10, 20, 30);
	assert(n == 0);
}

void TestPurge(redisContext *c, const string &key){
	string datestr = "07-04-2019";
	string starttime = "12:00";
	int n = PurgeAllBefore(c, key, datestr, starttime);
	cout << "purged " << n << endl;
	assert(n == 25);
}

void TestObjectHistory(redisContext *c, const string &key){

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

void TestPartialObjectHistory(redisContext *c, const string &key){

	/* partial object histories */
	string datestr = "05-21-2019", enddatestr;
	string starttime = "12:00";
	string endtime = "15:30";
	int n = ObjectHistory2(c, key, 150,  datestr, starttime, datestr, endtime);
	assert(n == 3);

	datestr = "08-02-2019";
	starttime = "09:00";
	endtime = "10:15";
	n = ObjectHistory2(c, key, 10, datestr, starttime, datestr, endtime);
	assert(n == 6);

	datestr = "08-30-2019";
	starttime = "13:00";
	enddatestr = "09-01-2019";
	endtime = "08:00";
	n = ObjectHistory2(c, key, 80, datestr, starttime, enddatestr, endtime);
	assert(n == 1);
}

void TestDeleteObjects(redisContext *c, const string &key){
	int n = DeleteObject(c, key, 150);
	assert(n == 12);
	n = DeleteObject(c, key, 70);
	assert(n == 3);
}


void TestTrackAll(redisContext *c, const string &key){
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
	const string addr = "localhost";
	const int port = 6379;

	cout << "Open connection to " << addr << ":" << port << endl;
	redisContext *c = redisConnect(addr.c_str(), port);
	assert(c != NULL);

	cout << "Test Add Events " << endl;
	TestAddEvents(c, key);

	cout << "Test Add Objects " << endl;
	TestAddObjects(c, key);

	cout << "Test Query" << endl;
	TestQuery(c, key);

	cout << "Test Query By Radius" << endl;
	TestQueryByRadius(c, key);

	cout << "Test Add Events With Categories" << endl;
	TestAddEventsCategoriesAndQuery(c, key);

	cout << "Test Object History" << endl;
	TestObjectHistory(c, key);
	
	cout << "Test Partial Object History" << endl;
	TestPartialObjectHistory(c, key);

	cout << "Test Delete Objects " << endl;
	TestDeleteObjects(c, key);

	cout << "Test Track All" << endl;
	TestTrackAll(c, key);
	
	cout << "Test Purge" << endl;
	TestPurge(c, key);

	cout << "Delete Key" << endl;
	DeleteKey(c, key);
	
	
	cout << "Done" << endl;
	redisFree(c);

	return 0;
}
