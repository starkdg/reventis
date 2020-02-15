#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <algorithm>
#include <stack>
#include <vector>
#include <limits>
#include "spfc.hpp"

/** 
static const int npoints[NSTATES][N] = {{0, 1, 3, 2},
										{0, 2, 3, 1},
										{3, 1, 0, 2},
										{3, 2, 0, 1}};
static const int tm1[NSTATES][N] = {{1, 0, 0, 2},
                                    {0, 1, 1, 3},
                                    {3, 2, 2, 0},
                                    {2, 3, 3, 1}};

static const int seqnums[NSTATES][N] = {{0, 1, 3, 2},
                                        {0, 3, 1, 2},
                                        {2, 1, 3, 0},
                                        {2, 3, 1, 0}};
static const int tm2[NSTATES][N] = {{1, 0, 2, 0},
                                    {0, 3, 1, 1},
                                    {2, 2, 0, 3},
                                    {3, 1, 3, 2}};

**/

/* utility matrices for seqnum to npoint  conversions */
/* use seqnum as index to lookup a npoint number */
/* each row is a different state */

static const int npoints[NSTATES][N] = {{0, 1, 3, 2, 6, 7, 5, 4},
										{0, 2, 6, 4, 5, 7, 3, 1},
										{0, 4, 5, 1, 3, 7, 6, 2},
										{3, 2, 0, 1, 5, 4, 6, 7},
										{5, 7, 3, 1, 0, 2, 6, 4},
										{6, 2, 3, 7, 5, 1, 0, 4},
										{3, 7, 6, 2, 0, 4, 5, 1},
										{5, 4, 6, 7, 3, 2, 0, 1},
										{6, 4, 0, 2, 3, 1, 5, 7},
										{5, 1, 0, 4, 6, 2, 3, 7},
										{6, 7, 5, 4, 0, 1, 3, 2},
										{3, 1, 5, 7, 6, 4, 0, 2}};
										

static const int tm1[NSTATES][N] = {{1, 2, 2, 3, 3, 5, 5, 4},
									{2, 0, 0, 8, 8, 7, 7, 6},
									{0, 1, 1, 9, 9, 11, 11, 10},
									{11, 6, 6, 0, 0, 9, 9, 8},
									{9, 7, 7, 11, 11, 0, 0, 5},
									{10, 8, 8, 6, 6, 4, 4, 0},
									{3, 11, 11, 5, 5, 1, 1, 7},
									{4, 9, 9, 10, 10, 6, 6, 1},
									{5, 10, 10, 1, 1, 3, 3, 9},
									{7, 4, 4, 2, 2, 8, 8, 3},
									{8, 5, 5, 7, 7, 2, 2, 11},
									{6, 3, 3, 4, 4, 10, 10, 2}};

/* utility matrices for npoint to seqnum conversions */
/* use npoint to lookup seqnum */

 
static const int seqnums[NSTATES][N] = {{0, 1, 3, 2, 7, 6, 4, 5},
										{0, 7, 1, 6, 3, 4, 2, 5},
										{0, 3, 7, 4, 1, 2, 6, 5},
										{2, 3, 1, 0, 5, 4, 6, 7},
										{4, 3, 5, 2, 7, 0, 6, 1},
										{6, 5, 1, 2, 7, 4, 0, 3},
										{4, 7, 3, 0, 5, 6, 2, 1},
										{6, 7, 5, 4, 1, 0, 2, 3},
										{2, 5, 3, 4, 1, 6, 0, 7},
										{2, 1, 5, 6, 3, 0, 4, 7},
										{4, 5, 7, 6, 3, 2, 0, 1},
										{6, 1, 7, 0, 5, 2, 4, 3}};

static const int tm2[NSTATES][N] = {{1, 2, 3, 2, 4, 5, 3, 5},
									{2, 6, 0, 7, 8, 8, 0, 7},
									{0, 9, 10, 9, 1, 1, 11, 11},
									{6, 0, 6, 11, 9, 0, 9, 8},
									{11, 11, 0, 7, 5, 9, 0, 7},
									{4, 4, 8, 8, 0, 6, 10, 6},
									{5, 7, 5, 3, 1, 1, 11, 11},
									{6, 1, 6, 10, 9, 4, 9, 10},
									{10, 3, 1, 1, 10, 3, 5, 9},
									{4, 4, 8, 8, 2, 7, 2, 3},
									{7, 2, 11, 2, 7, 5, 8, 5},
									{10, 3, 2, 6, 10, 3, 4, 4}};


int cmpseqnums(const Seqn &a, const Seqn &b){
	for (int i=0;i<DIMS;i++){
		if (a.arr[i] < b.arr[i])
			return -1;
		else if (a.arr[i] > b.arr[i])
			return 1;
	}
	return 0;
}

bool is_contiguous(const Seqn a, const Seqn b){
	return (b.arr[DIMS-1] - a.arr[DIMS-1] == 1 || b.arr[DIMS-1] - a.arr[DIMS-1] == 0xffffffff);
}

bool contains(Region r, Seqn s){
	Point p;
	spfc_decode(s, p);
	for (int i=0;i<DIMS;i++){
		if (p.arr[i] < r.lower[i] || p.arr[i] > r.upper[i])
			return false;
	}
	return true;
}

/* set bit at bitpos */
static void setbit(uint64_t arr[], const int bitpos){
	assert(bitpos >= 0 && bitpos < DIMS*64);
	arr[bitpos/64] |= (0x8000000000000000ULL >> (bitpos % 64));
}

/* clear bits from [bitpos, end] */
static void clearbits(uint64_t arr[], const int bitpos){
	assert(bitpos >= 0 && bitpos < DIMS*64);
	arr[bitpos/64] &= ~(0xffffffffffffffffULL >> (bitpos%64));
	for (int i=bitpos/64+1;i < DIMS;i++) arr[i] = 0;
}

/* get bit at bitpos */
static bool getbit(const uint64_t arr[], const int bitpos){
	assert(bitpos >= 0 && bitpos < DIMS*64);
	return ((arr[bitpos/64] & (0x8000000000000000ULL >> (bitpos % 64))) != 0);
}

int spfc_encode(const Point &p, Seqn &sn){
	uint64_t coords[DIMS];
	for (int i=0;i<DIMS;i++){
		coords[i] = p.arr[i] << (64-ORDER);
		sn.arr[i] = 0;
	}

	int level = 0;
	int state = 0;
	const uint64_t bitmask = 0x8000000000000000ULL;

	int pos = 64*DIMS - ORDER*DIMS;
	while (level < ORDER) {
		int npoint = 0;
		int coord_index = DIMS-1;
		for (int i=63;i > 63-DIMS;i--){
			npoint |= (int)((coords[coord_index--] & bitmask) >> i);
		}
		int s = seqnums[state][npoint];

		//printf("level = %d, state = %d, npoint = %d, seq = %d, next = %d\n",
		//                     level, state, npoint, s, tm2[state][npoint]);

		uint32_t mask = 0x01 << (DIMS-1);
		for (int i=0;i<DIMS;i++){
			if (s & mask)
				setbit(sn.arr, pos+i);
			mask >>= 1;
		}

		for (int i=0;i<DIMS;i++){
			coords[i] <<= 1;
		}
		
		state = tm2[state][npoint];
		pos += DIMS;
		level += 1;
	}

	return 0;
}

int spfc_decode(const Seqn &s, Point &p){
	uint64_t coords[DIMS];
	for (int i=0;i<DIMS;i++){
		p.arr[i] = 0;
		coords[i] = 0;
	}
	
	int level = 0, state = 0, pos = 64*DIMS - ORDER*DIMS;
	while (level < ORDER){
		int d = 0;

		uint32_t mask = 0x01 << (DIMS-1);
		for (int i=0;i<DIMS;i++){
			if (getbit(s.arr, pos+i))
				d |= mask;
			mask >>= 1;
		}
		
		int npnt = npoints[state][d];

		//printf("level = %d, state = %d, seq = %d, npoint = %d, next = %d\n",
		//	   level, state, d, npnt, tm1[state][d]);


		mask = 0x01;
		int coord_index = DIMS-1;
		for (int i=63;i>63-DIMS;i--){
			if (i - level >= 0)
				coords[coord_index--] |= (npnt & mask) << (i - level);
			else
				coords[coord_index--] |= (npnt & mask) >> abs(i-level);
			mask <<= 1;
		}
		
		state = tm1[state][d];
		pos += DIMS;
		level += 1;
	}

	for (int i=0;i<DIMS;i++){
		coords[i] >>= 64 - ORDER;
		p.arr[i] = coords[i];
	}

	return 0;
}

static int extractnpoints(const Region &r, const int level, int &ql, int &qu){
	assert(level >= 0 && level < ORDER);

	int pos = 64 - ORDER + level;
	ql = qu = 0;
	int mask = 0x01 << (DIMS-1);
	for (int i=0;i<DIMS;i++){
		if (r.lower[i] & (0x8000000000000000ULL >> pos)) ql |= mask;
		if (r.upper[i] & (0x8000000000000000ULL >> pos)) qu |= mask;
		mask >>= 1;
	}
		
	return 0;
}

static int extractsubseqn(const Seqn s, const int pos, int &hkey){
	assert(pos >= 64*DIMS - DIMS*ORDER && pos < 64*DIMS);
	
	hkey = 0;
	int mask = 0x01 << (DIMS-1);
	for (int i=0;i<DIMS;i++){
		if (getbit(s.arr, pos+i)) hkey |= mask;
		mask >>= 1;
	}

	return 0;
}

static int restrict_search_region(Region &s, int quadr){
	assert(quadr >= 0 && quadr < N);

	int mask = 0x01 << (DIMS-1);
	for (int i=0;i<DIMS;i++){
		int64_t diff = (s.upper[i] - s.lower[i])/2 + 1;
		if (quadr & mask){
			s.lower[i] += diff;
		} else {
			s.upper[i] -= diff;
		}
		mask >>= 1;
	}
	return 0;
}

static bool regions_coincide(Region ss, Region qr){
	for (int i=0;i<DIMS;i++){
		if (ss.lower[i] != qr.lower[i] || ss.upper[i] != qr.upper[i])
			return false;
	}
	return true;
}

static int restrict_search_region_by_half(Region &s, int pd, bool lower){

	int i = 0;
	int mask = 0x01 << (DIMS-1);
	while (pd != mask) {
		i++;
		mask >>= 1;
	}
	assert(i >= 0 && i < DIMS);
	
	int64_t diff = (s.upper[i] - s.lower[i])/2 + 1;

	if (lower)
		s.upper[i] -= diff;
	else
		s.lower[i] += diff;

	return 0;
}

static int restrict_query_to_search_space(Region ss, Region &qr){
	for (int i=0;i<DIMS;i++){
		qr.lower[i] = max(qr.lower[i], ss.lower[i]);
		qr.upper[i] = min(qr.upper[i], ss.upper[i]);
	}
	return 0;
}

typedef struct bt_record_t {
	Region ss, qr;
	int hkey;
	int state;
	int level;
	int offset;
	int n;
} Record;

static int select_quadrant2(Region ss, Region qr, int h, stack<Record> &bt,
							int level, const int state,
							int offset=0, int n=N){
	int quad = -1;
	while (n >= 1){
		if (n == 1){
			quad = npoints[state][offset];
			//printf("return quad = %d\n", quad);
			break;
		}
		int maxlower = n/2 - 1;
		int minhigher = n/2;
		int pd = npoints[state][offset+maxlower]^npoints[state][offset+minhigher];

		int ql, qu;
		extractnpoints(qr, level, ql, qu);

		if ((((npoints[state][offset+maxlower] & pd) == (ql & pd)) ||
		 ((npoints[state][offset+maxlower] & pd) == (qu & pd))) && (offset+maxlower >= h)){

			if (((npoints[state][offset+minhigher] & pd) == (ql & pd)
				 ||(npoints[state][offset+minhigher] & pd) == (qu & pd))) {
				// save upper quadrant space in backup;
				//printf("select: save upper [%d ...]\n", npoints[state][offset+minhigher]);
				Region ssupper = ss, qrupper = qr;
				restrict_search_region_by_half(ssupper, pd, false);
				restrict_query_to_search_space(ssupper, qrupper);

				Record rec;
				rec.ss = ssupper;
				rec.qr = qrupper;
				rec.hkey = h;
				rec.state = state;
				rec.level = level;
				rec.offset = offset + minhigher;
				rec.n = minhigher;
				bt.push(rec);
			}
			// check lower quadrant space
			//printf("select: check lower [... %d]\n", npoints[state][offset+maxlower]);
			restrict_search_region_by_half(ss, pd, true);
			restrict_query_to_search_space(ss, qr);
			n = minhigher;
		} else {
			if (((npoints[state][offset+minhigher] & pd) == (ql & pd)
				 ||(npoints[state][offset+minhigher] & pd) == (qu & pd)) && offset+n-1 >= h){
				// check upper quadrant space
				//printf("select: check upper [%d...]\n", npoints[state][offset+minhigher]);
				restrict_search_region_by_half(ss, pd, false);
				restrict_query_to_search_space(ss, qr);
				offset = offset+minhigher;
				n = minhigher;
			} else {
				break;
			}
		}
	}

	return quad;;
}

static int select_quadrant(Region ss, Region qr, int h, stack<Record> &bt, 
						   int level, const int state,
						   int offset=0, int n=N){
	if (n == 1)	return npoints[state][offset];
		
	int quad = -1;
	int maxlower = n/2 - 1;
	int minhigher = n/2;
	int pd = npoints[state][offset+maxlower]^npoints[state][offset+minhigher];

	int ql, qu;
	extractnpoints(qr, level, ql, qu);

	if ((((npoints[state][offset+maxlower] & pd) == (ql & pd)) ||
		 ((npoints[state][offset+maxlower] & pd) == (qu & pd))) && (offset+maxlower >= h)){
		
		if (((npoints[state][offset+minhigher] & pd) == (ql & pd)
			 ||(npoints[state][offset+minhigher] & pd) == (qu & pd))) {
			// save upper quadrant space in backup;
			Region ssupper = ss, qrupper = qr;
			restrict_search_region_by_half(ssupper, pd, false);
			restrict_query_to_search_space(ssupper, qrupper);

			Record rec;
			rec.ss = ssupper;
			rec.qr = qrupper;
			rec.hkey = h;
			rec.state = state;
			rec.level = level;
			rec.offset = offset + minhigher;
			rec.n = minhigher;
			bt.push(rec);
		}
		// check lower quadrant space
		Region sslower = ss;
		Region qrlower = qr;
		restrict_search_region_by_half(sslower, pd, true);
		restrict_query_to_search_space(sslower, qrlower);
		quad = select_quadrant(sslower, qrlower, h, bt, level, state, offset, minhigher);
	} else {
		if ((npoints[state][offset+minhigher] & pd) == (ql & pd)
			||(npoints[state][offset+minhigher] & pd) == (qu & pd)){
			// check upper quadrant space
			Region ssupper = ss;
			Region qrupper = qr;
			restrict_search_region_by_half(ssupper, pd, false);
			restrict_query_to_search_space(ssupper, qrupper);
			quad = select_quadrant(ssupper, qrupper, h, bt, level, state, offset+minhigher, minhigher);
		}
	}
	return quad;
}

static bool calc_next_match(Region ss, Region qr, const Seqn prev, Seqn &next){
	stack<Record> bt;
	vector<Region> sslist, qrlist;
	int level = 0, state = 0, pos = 64*DIMS - ORDER*DIMS;
	
	while (level < ORDER){
		//printf("(level %d) state = %d, pos = %d\n", level, state, pos);
		if (level >= (int)sslist.size()){
			sslist.push_back(ss);
			qrlist.push_back(qr);
		} else {
			ss = sslist[level];
			qr = qrlist[level];
		}
		
		int h;
		extractsubseqn(prev, pos, h);
		//printf("h = %d\n", h);
		
		int quad = select_quadrant2(ss, qr, h, bt, level, state);
		//printf("quad = %d\n", quad);
		if (quad < 0){
			if (!bt.empty()){
				Record rec = bt.top();
				level = rec.level;
				state = rec.state;
				pos = 64*DIMS - ORDER*DIMS + level*DIMS;
				h = rec.hkey;
				ss = sslist[level];
				qr = qrlist[level];
				clearbits(next.arr, pos);
				quad = select_quadrant2(rec.ss, rec.qr, 0, bt, level, state, rec.offset, rec.n);
				//printf("BT: level = %d, pos = %d, h = %d, quad = %d\n", level, pos, h, quad);
				//printf("BT: ss x %u to %u, y %u to %u\n", ss.lower[0], ss.upper[0], ss.lower[1], ss.upper[1]);
				//printf("BT: qr x %u to %u, y %u to %u\n", qr.lower[0], qr.upper[0], qr.lower[1], qr.upper[1]);
			} else {
				//printf("No next match found\n");
				next.arr[0] = next.arr[1] = next.arr[1] = numeric_limits<uint32_t>::max();
				return false;
			}
		}

		if (level < ORDER){
			restrict_search_region(ss, quad);
			restrict_query_to_search_space(ss, qr);
			//printf("restrict search, query spaces\n");
			//printf("ss: x %u to %u, y %u to %u\n", ss.lower[0], ss.upper[0], ss.lower[1], ss.upper[1]);
			//printf("qr: x %u to %u, y %u to %u\n", qr.lower[0], qr.upper[0], qr.lower[1], qr.upper[1]);
		}

		int quads = seqnums[state][quad];
		//printf("quads = %d\n", quads);
		
		if (h == quads && regions_coincide(ss, qr)){
			//printf("(h=%d) == (quads=%d) and ss == qr\n", h, quads);
			next = prev;
			return true;
		}
		//printf("quads = %d\n", quads);
		
		uint32_t mask = 0x01 << (DIMS-1);
		for (int i=0;i<DIMS;i++){
			if (quads & mask)
				setbit(next.arr, pos+i);
			mask >>= 1;
		}

		state = tm2[state][quad];
		level++;
		pos += DIMS;

		//printf("++++++++++++++++++++++++++++++++++++++\n");
		if (h < quads){
			//printf("quads =%d > h %d\n", quads, h);
			break;
		}
	}

	while (level < ORDER){
		//printf("(level %d) state = %d, pos = %d\n", level, state, pos);
		if (regions_coincide(ss, qr)){
			//printf("ss == qr\n");
			clearbits(next.arr, pos);
			break;
		}

		int quad = select_quadrant2(ss, qr, 0, bt, level, state);
		//printf("quad = %d\n", quad);
		assert(quad >= 0 && quad < N);

		if (level < ORDER){
			restrict_search_region(ss, quad);
			restrict_query_to_search_space(ss, qr);
			//printf("restrict search, query spaces\n");
			//printf("ss: x %u to %u, y %u to %u\n", ss.lower[0], ss.upper[0], ss.lower[1], ss.upper[1]);
			//printf("qr: x %u to %u, y %u to %u\n", qr.lower[0], qr.upper[0], qr.lower[1], qr.upper[1]);
		}

		int quads = seqnums[state][quad];
		//printf("quads = %d\n", quads);
		
		uint32_t mask = 0x01 << (DIMS-1);
		for (int i=0;i<DIMS;i++){
			if (quads & mask)
				setbit(next.arr, pos+i);
			mask >>= 1;
		}

		state = tm2[state][quad];
		level++;
		pos += DIMS;

		//printf("--------------------------------------\n");
	}

	return true;
}

bool next_match(Region qr, const Seqn prevkey, Seqn &nextkey){
	memset(nextkey.arr, 0, DIMS*sizeof(uint64_t));
	
	Region ss;
	for (int i=0;i<DIMS;i++){
		ss.lower[i] = 0;
		ss.upper[i] = 0xffffffffffffffffULL >> (64-ORDER);
	}
	return calc_next_match(ss, qr, prevkey, nextkey);
}


