#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <string>
#include <ctime>
#include "spfc.hpp"

using namespace std;


void print_query_region(const Region qr){
	for (int i=0;i<DIMS;i++){
		printf("%u to %u\n", qr.lower[i], qr.upper[i]);
	}
	printf("\n");
}


int test_query_region(const Region qr){
	
	Seqn prev, next;;

	int n_segments = 0;
	while (next_match(qr, prev, next)){
		n_segments++;
		do {
			next++;
		} while (contains(qr, next));
		prev = next;
	}

	// check each value in query region 
	for (uint64_t i=qr.lower[0];i <= qr.upper[0];i++){
		for (uint64_t j=qr.lower[1];j <= qr.upper[1];j++){
			for (uint64_t k=qr.lower[2];k <= qr.upper[2];k++){
				Point p;
				p.arr[0] = i;
				p.arr[1] = j;
				p.arr[2] = k;

				Seqn s;
				spfc_encode(p, s);
				assert(next_match(qr, s, next) == true);
				assert(s == next);
			}
		}
	}

	return n_segments;
}


int main(int argc, char **argv){

	const int n_iterations = 1000000;

	printf("Test spfc encode/decode over %d iterations\n", n_iterations);
	for (int i=0;i<n_iterations;i++){
		Point p;
		for (int j=0;j<DIMS;j++){
			p.arr[j] = (uint32_t)rand();
		}

		Seqn s;
		spfc_encode(p, s);

		Point r;
		spfc_decode(s, r);
		assert(p == r);
	}


	printf("Test query regions\n");
	
	Region qr;
	qr.lower[0] = 1500000;
	qr.upper[0] = 1500025;
	qr.lower[1] = 25000000;
	qr.upper[1] = 25000050;
	qr.lower[2] = 3000500;
	qr.upper[2] = 3000550;
	print_query_region(qr);
	
	int n = test_query_region(qr);
	printf("n = %d\n", n);
	assert(n == 947);


	Region qr2;
	qr.lower[0] = 25000;
	qr.upper[0] = 25100;
	qr.lower[1] = 55000;
	qr.upper[1] = 55200;
	qr.lower[2] = 5000;
	qr.upper[2] = 5050;
	print_query_region(qr);
	
	n = test_query_region(qr);
	printf("n = %d\n", n);
	assert(n = 10534);
	
	return 0;
}
