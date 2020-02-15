#ifndef _SPFC_HPP
#define _SPFC_HPP

#include <cstdint>
#include <cstring>

using namespace std;

#define DIMS 3
#define N 8
#define NSTATES 12
#define ORDER 64

typedef struct point_t {
	uint64_t arr[DIMS];
	point_t(){
		for (int i=0;i<DIMS;i++) arr[i] = 0;
	}
	point_t& operator=(const point_t &other){
		if (this != &other){
			for (int i=0;i<DIMS;i++){
				arr[i] = other.arr[i];
			}
		}
		return *this;
	}

	point_t& operator++(int){
		int i = DIMS-1;
		arr[i]++;
		while (arr[i] == 0 && i > 0){
			arr[--i]++;
		}

		return *this;
	}

	bool operator==(const point_t &other){
		for (int i=0;i<DIMS;i++){
			if (arr[i] != other.arr[i])
				return false;
		}
		return true;
	}

	bool operator!=(const point_t &other){
		for (int i=0;i<DIMS;i++){
			if (arr[i] == other.arr[i])
				return false;
		}
		return true;
	}
	
	bool operator<(const point_t &other){
		for (int i=0;i<DIMS;i++){
			if (arr[i] < other.arr[i])
				return true;
			else if (arr[i] > other.arr[i])
				return false;
		}
		return false;
	}
} Point;

typedef Point Seqn;

typedef struct region_t {
	uint64_t lower[DIMS];
	uint64_t upper[DIMS];

	region_t(){
		memset(lower, 0, DIMS*sizeof(uint64_t));
		memset(upper, 0, DIMS*sizeof(uint64_t));
	}
	region_t(const region_t &other){
		for (int i=0;i<DIMS;i++){
			lower[i] = other.lower[i];
			upper[i] = other.upper[i];
		}
	}
	region_t& operator=(const region_t &other){
		if (this != &other){
			for (int i=0;i<DIMS;i++){
				lower[i] = other.lower[i];
				upper[i] = other.upper[i];
			}
		}
		return *this;
	}
} Region;


bool contains(Region r, Seqn s);

bool is_contiguous(const Seqn a, const Seqn b);

int cmpseqnums(const Seqn &a, const Seqn &b);

int spfc_encode(const Point &p, Seqn &s);

int spfc_decode(const Seqn &s, Point &p);

bool next_match(Region qr, const Seqn prevkey, Seqn &nextkey);

#endif /* _SPFC_HPP */








