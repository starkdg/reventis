#ifndef _SPFC_HPP
#define _SPFC_HPP

#include <cstdint>
#include <cstring>

using namespace std;

#define DIMS 3
#define N 8
#define NSTATES 12
#define ORDER 32

typedef struct point_t {
	uint32_t arr[DIMS];
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
		arr[DIMS-1]++;
		for (int i=DIMS-1;i>=0;i--){
			if (i-1 >= 0 && arr[i] == 0xffffffff)
				arr[i-1]++;
		}
		return *this;
	}

	friend bool operator==(const point_t &a, const point_t &b){
		for (int i=0;i<DIMS;i++){
			if (a.arr[i] != b.arr[i])
				return false;
		}
		return true;
	}

	friend bool operator!=(const point_t &a, const point_t &b){
		for (int i=0;i<DIMS;i++){
			if (a.arr[i] == b.arr[i])
				return false;
		}
		return true;
	}
	
	friend bool operator<(const point_t &a, const point_t &b){
		for (int i=0;i<DIMS;i++){
			if (a.arr[i] < b.arr[i])
				return true;
			else if (a.arr[i] > b.arr[i])
				return false;
		}
		return false;
	}
} Point;

typedef Point Seqn;

typedef struct region_t {
	uint32_t lower[DIMS];
	uint32_t upper[DIMS];

	region_t(){
		memset(lower, 0, DIMS*sizeof(uint32_t));
		memset(upper, 0, DIMS*sizeof(uint32_t));
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








