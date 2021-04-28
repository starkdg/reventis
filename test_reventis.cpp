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
	string startdatetime, enddatetime;
	
	/* query first cluster */
	double x1 = -72.338000;
	double x2 = -72.330000;
	double y1 = 41.571000;
	double y2 = 41.576000;
	startdatetime = "2019-12-01T12:00";
	enddatetime = "2019-12-01T16:00";
	int n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 0);
	assert(n == 3);

	/* second cluster */
	x1 = -72.218815;
	x2 = -72.217520;
	y1 = 41.719640;
	y2 = 41.720690;
	startdatetime = "2019-10-09T12:00";
	enddatetime = "2019-10-09T18:00";
	n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 0);
	assert(n == 4);

	/* third cluster */
	x1 = -72.257400;
	x2 = -72.248800;
	y1 = 41.806490;
	y2 = 41.809360;
	startdatetime = "2019-12-04T9:00";
	enddatetime = "2019-12-04T18:00";
	n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 0);
	assert(n == 6);

	/* fourth cluster */
	x1 = -106.513485;
	x2 = -93.712109;
	y1 = 27.336248;
	y2 = 36.416949;
	startdatetime = "2019-01-01T12:00";
	enddatetime = "2020-01-01T12:00";
	n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 0);
	assert(n == 11);

}

void TestQueryByRadius(redisContext *c, const string &key){
	double x = -72.334000, y =  41.5735, radius = 2.0;
	string startdatetime = "2019-12-01T12:00";
	string enddatetime = "2019-12-01T16:00";
	int n = QueryByRadius(c, key, x, y, radius, startdatetime, enddatetime, 0);
	assert(n == 3);

	x = -72.218167;
	y = 41.720165;
	startdatetime = "2019-10-09T12:00";
	enddatetime = "2019-10-09T18:00";
	radius = 2.0;
	n = QueryByRadius(c, key, x, y, radius, startdatetime, enddatetime, 0);
	assert(n == 4);

	x = -72.253100;
	y = 41.807925;
	startdatetime = "2019-12-04T9:00";
	enddatetime = "2019-12-04T18:00";
	radius = 10.0;
	n = QueryByRadius(c, key, x, y, radius, startdatetime, enddatetime, 0);
	assert(n == 6);

	x = -100.112797;
	y = 31.876599;
	startdatetime = "2019-01-01T12:00";
	enddatetime = "2020-01-01T12:00";
	radius = 400;
	n = QueryByRadius(c, key, x, y, radius, startdatetime, enddatetime, 0);
	assert(n == 11);
}

void TestAddEventsCategoriesAndQuery(redisContext *c, const string &key){
	double x = -97.797586;
	double y = 30.318388;
	string startdatetime = "2019-05-25T13:00";
	string enddatetime = "2019-05-25T13:30";
	string descr = "class 10";
	long long id1 = AddEvent(c, key, x, y, startdatetime, enddatetime, descr);

	AddCategory(c, key, id1, 1, 10);


	x = -97.715113;
	y = 30.333895;
	startdatetime = "2019-05-01T14:30";
	enddatetime = "2019-05-01T14:45";
	descr = "class 10, 20";

	long long id2 = AddEvent(c, key, x, y, startdatetime, enddatetime, descr);

	AddCategory(c, key, id2, 2, 10, 20);


	x = -97.816367;
	y = 30.206953;
	startdatetime = "2019-04-01T23:00";
	enddatetime = "2019-04-01T23:05";
	descr = "class 10, 20, 30";

	long long id3 = AddEvent(c, key, x, y, startdatetime, enddatetime, descr);

	AddCategory(c, key, id3, 3, 10, 20, 30);


	double x1 = -106.513485;
	double x2 = -93.712109;
	double y1 = 27.336248;
	double y2 = 36.416949;

	startdatetime = "2019-03-25T12:00";
	enddatetime = "2019-06-01T12:00";
	
	int n;
	n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 1, 10);
	assert(n == 3);
	
	n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 1, 20);
	assert(n == 2);

	n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 1, 30);
	assert(n == 1);

	RemoveCategory(c, key, id3, 3, 10, 20, 30);

	n = Query(c, key, x1, x2,  y1, y2, startdatetime, enddatetime, 1, 10);
	assert(n == 2);

	cout << "delete id1 " << id1 << endl;
	DeleteEvent(c, key, id1);

	cout << "delete id2 " << id2 << endl;
	DeleteEvent(c, key, id2);

	cout << "delete id3 " << id3 << endl;
	DeleteEvent(c, key, id3);

	cout << "query" << endl;
	n = Query(c, key, x1, x2, y1, y2, startdatetime, enddatetime, 3, 10, 20, 30);
	assert(n == 0);
}

void TestPurge(redisContext *c, const string &key){
	string datetimestr = "2019-07-04T12:00";
	int n = PurgeAllBefore(c, key, datetimestr);
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
	string startdatetime = "2019-05-21T12:00";
	string enddatetime = "2019-05-21T15:30";
	int n = ObjectHistory2(c, key, 150,  startdatetime, enddatetime);
	assert(n == 3);

	startdatetime = "2019-08-02T09:00";
	enddatetime = "2019-08-02T10:15";
	n = ObjectHistory2(c, key, 10, startdatetime, enddatetime);
	assert(n == 6);

	startdatetime = "2019-08-30T13:00";
	enddatetime = "2019-01-01T08:00";
	n = ObjectHistory2(c, key, 80, startdatetime, enddatetime);
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
	string startdatetime = "2019-08-02T09:00";
	string enddatetime = "2019-08-02T17:30";
	int n = TrackAll(c, key, x1, x2, y1, y2, startdatetime, enddatetime);
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
