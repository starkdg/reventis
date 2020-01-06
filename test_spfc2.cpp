#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <cassert>
#include "spfc.hpp"

using namespace std;

void printf_point(const Point t){
	for (int i=0;i<DIMS;i++) printf("%u ", t.arr[i]);
	printf("\n");
}

int main(int argc, char **argv){

	const int n_iterations = 1000000;
	Point p, r;
	Seqn s;

	printf("Test spfc_encode/spfc_decode for %d iterations\n", n_iterations);
	int i=0;
	do {
		for (int j=0;j<DIMS;j++)
			p.arr[j] = (uint32_t)rand();

		Seqn s;
		spfc_encode(p, s);
		spfc_decode(s, r);

		assert(p == r);
		i++;
	} while (i < n_iterations);

	printf("Done.\n");
	return 0;
}
