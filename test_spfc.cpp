#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <ctime>
#include <string>
#include "spfc.hpp"

using namespace std;

#define SCALE_FACTOR 1000000


void printf_seqn(Seqn s){
	for (int i=0;i<DIMS;i++){
		printf("%u ", s.arr[i]);
	}
	printf("\n");
}

void printf_point(Point p){
	for (int i=0;i<DIMS;i++){
		printf("%u ", p.arr[i]);
	}
	printf("\n");
}

void printf_region(Region r){
	for (int i=0;i<DIMS;i++){
		printf("dim[%d] %u to %u\n", i, r.lower[i], r.upper[i]);
	}
	printf("\n");
}

int parsedatetime(const string &str, time_t &epoch){
	tm datetime;
	datetime.tm_sec = 0;
	datetime.tm_isdst = -1;
	sscanf(str.c_str(), "%d-%d-%d %d:%d", &datetime.tm_mon, &datetime.tm_mday,
		   &datetime.tm_year, &datetime.tm_hour, &datetime.tm_min);
	datetime.tm_mon -= 1;
	datetime.tm_year = (datetime.tm_year >= 2000) ? datetime.tm_year%100 + 100 : datetime.tm_year%100;
	epoch = mktime(&datetime);
	return 0;
}

int main(int argc, char **argv){

	time_t tt1, tt2;
	parsedatetime("12-01-2019 12:00", tt1);
	parsedatetime("12-01-2019 16:30", tt2);
	
	Region qr;
	qr.lower[0] = static_cast<uint32_t>((-72.337773 + 180.0)*SCALE_FACTOR);
	qr.upper[0] = static_cast<uint32_t>((-72.330500 + 180.0)*SCALE_FACTOR);
	qr.lower[1] = static_cast<uint32_t>((41.571082 + 90.0)*SCALE_FACTOR);
	qr.upper[1] = static_cast<uint32_t>((41.575244+ 90.0)*SCALE_FACTOR);
	qr.lower[2] = static_cast<uint32_t>(tt1);
	qr.upper[2] = static_cast<uint32_t>(tt2);

	printf("Region: ");
	printf_region(qr);


	printf("------- node-400--------\n");
	
	time_t t1;
	parsedatetime("12-06-2019 8:30", t1);

	Point n1, r1;
	n1.arr[0] = static_cast<uint32_t>((-92.017164 + 180.0)*SCALE_FACTOR);
	n1.arr[1] = static_cast<uint32_t>((37.434472 + 90.0)*SCALE_FACTOR);
	n1.arr[2] = static_cast<uint32_t>(t1);

	Seqn s1;
	spfc_encode(n1, s1);
	spfc_decode(s1, r1);
	printf("p1: ");
	printf_point(n1);
	printf("s1: ");
	printf_seqn(s1);
	printf("r1: ");
	printf_point(r1);
	assert(n1 == r1);
	assert(!contains(qr, s1));


	printf("--------node-100-----------\n");
	
	time_t t2;
	parsedatetime("12-01-2019 12:00", t2);

	Point n2, r2;
	n2.arr[0] = static_cast<uint32_t>((-75.130396 + 180.0)*SCALE_FACTOR);
	n2.arr[1] = static_cast<uint32_t>((39.996101 + 90.0)*SCALE_FACTOR);
	n2.arr[2] = static_cast<uint32_t>(t2);

	Seqn s2;
	spfc_encode(n2, s2);
	spfc_decode(s2, r2);
	printf("p2: ");
	printf_point(n2);
	printf("s2: ");
	printf_seqn(s2);
	printf("r2: ");
	printf_point(r2);
	assert(n2 == r2);
	assert(!contains(qr, s2));

	printf("---------node-1200----------\n");
	
	time_t t3;
	parsedatetime("12-01-2019 12:00", t3);

	Point n3, r3;
	n3.arr[0] = static_cast<uint32_t>((-72.337773 + 180.0)*SCALE_FACTOR);
	n3.arr[1] = static_cast<uint32_t>((41.571082 + 90.0)*SCALE_FACTOR);
	n3.arr[2] = static_cast<uint32_t>(t3);

	Seqn s3;
	spfc_encode(n3, s3);
	spfc_decode(s3, r3);
	printf("p3: ");
	printf_point(n3);
	printf("s3: ");
	printf_seqn(s3);
	printf("r3: ");
	printf_point(r3);
	assert(n3 == r3);
	assert(contains(qr, s3));


	printf("--------node-1300-------------\n");
	
	time_t t4;
	parsedatetime("12-01-2019 12:30", t4);

	Point n4, r4;
	n4.arr[0] = static_cast<uint32_t>((-72.330500 + 180.0)*SCALE_FACTOR);
	n4.arr[1] = static_cast<uint32_t>((41.575244 + 90.0)*SCALE_FACTOR);
	n4.arr[2] = static_cast<uint32_t>(t4);

	Seqn s4;
	spfc_encode(n4, s4);
	spfc_decode(s4, r4);
	printf("p4: ");
	printf_point(n4);
	printf("s4: ");
	printf_seqn(s4);
	printf("r4: ");
	printf_point(r4);
	assert(n4 == r4);
	assert(contains(qr, s4));


	printf("--------node-1100------------\n");
	
	time_t t5;
	parsedatetime("12-01-2019 15:00", t5);

	Point n5, r5;
	n5.arr[0] = static_cast<uint32_t>((-72.333230 + 180.0)*SCALE_FACTOR);
	n5.arr[1] = static_cast<uint32_t>((41.574518 + 90.0)*SCALE_FACTOR);
	n5.arr[2] = static_cast<uint32_t>(t5);

	Seqn s5;
	spfc_encode(n5, s5);
	spfc_decode(s5, r5);
	printf("p5: ");
	printf_point(n5);
	printf("s5: ");
	printf_seqn(s5);
	printf("r5: ");
	printf_point(r5);
	assert(n5 == r5);
	assert(contains(qr, s5));
	
	return 0;
}
