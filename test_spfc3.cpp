#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <cassert>
#include <ctime>
#include <string>
#include "spfc.hpp"

using namespace std;

void printf_point(const Point t){
	for (int i=0;i<DIMS;i++) printf("%u ", t.arr[i]);
	printf("\n");
}

void print_region(const Region r){
	for (int i=0;i<DIMS;i++){
		printf("dim[%d] %u to %u\n", i, r.lower[i], r.upper[i]);
	}
}


int parsedatetime(const string &datestr, const string &timestr, time_t &epoch){
	tm datetime;
	datetime.tm_sec = 0;
	datetime.tm_isdst = -1;
	if (sscanf(datestr.c_str(), "%d-%d-%d", &datetime.tm_mon, &datetime.tm_mday, &datetime.tm_year) != 3)
		return -1;

	if (sscanf(timestr.c_str(), "%d:%d", &datetime.tm_hour, &datetime.tm_min) != 2)
		return -1;
	datetime.tm_mon -= 1;
	datetime.tm_year = (datetime.tm_year >= 2000) ? datetime.tm_year%100 + 100 : datetime.tm_year%100;

	printf("month %d, day %d, year %d\n", datetime.tm_mon, datetime.tm_mday, datetime.tm_year);
	printf("%d:%d\n", datetime.tm_hour, datetime.tm_min);

	epoch = mktime(&datetime);
	return 0;
}

int main(int argc, char **argv){

	const string datestr = "12-01-2019";
	const string timestr1 = "12:00";
	const string timestr2 = "16:30";

	time_t t1, t2;
	parsedatetime(datestr, timestr1, t1);
	parsedatetime(datestr, timestr2, t2);
	
	Region qr;
	qr.lower[0] = static_cast<uint32_t>((-72.338000+180)*1000000);
	qr.lower[1] = static_cast<uint32_t>((41.570000+90)*1000000);
	qr.lower[2] = static_cast<uint32_t>(t1);

	qr.upper[0] = static_cast<uint32_t>((-72.330000+180.0)*1000000);
	qr.upper[1] = static_cast<uint32_t>((41.577000+90.0)*1000000);
	qr.upper[2] = static_cast<uint32_t>(t2);

	printf("Region:\n");
	print_region(qr);
	
	Seqn s;
	s.arr[0] = 509014686;
	s.arr[1] = 998317515;
	s.arr[2] = 3536833014;

	Point p;
	spfc_decode(s, p);

	printf("seqn:");
	printf_point(s);
	printf("point: ");
	printf_point(p);
	
	printf("contain: %d\n", contains(qr, s));

	
	printf("Done.\n");
	return 0;
}
