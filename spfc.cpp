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

/** 2-dim utility matrices **/

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

/* 3-dim utility matrices */
/* utility matrices for seqnum to npoint  conversions */
/* use seqnum as index to lookup a npoint number */
/* each row is a different state */
/**
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
**/

/** 4-dim utility matrices **/
static const int npoints[NSTATES][N] = {{0, 1, 3, 2, 6, 7, 5, 4, 12, 13, 15, 14, 10, 11, 9, 8},
										{0, 2, 6, 4, 12, 14, 10, 8, 9, 11, 15, 13, 5, 7, 3, 1},
										{0, 4, 12, 8, 9, 13, 5, 1, 3, 7, 15, 11, 10, 14, 6, 2},
										{3, 11, 10, 2, 0, 8, 9, 1, 5, 13, 12, 4, 6, 14, 15, 7},
										{5, 4, 6, 7, 3, 2, 0, 1, 9, 8, 10, 11, 15, 14, 12, 13},
										{6, 2, 10, 14, 15, 11, 3, 7, 5, 1, 9, 13, 12, 8, 0, 4},
										{9, 11, 15, 13, 5, 7, 3, 1, 0, 2, 6, 4, 12, 14, 10, 8},
										{10, 14, 6, 2, 3, 7, 15, 11, 9, 13, 5, 1, 0, 4, 12, 8},
										{15, 7, 6, 14, 12, 4, 5, 13, 9, 1, 0, 8, 10, 2, 3, 11},
										{12, 8, 0, 4, 5, 1, 9, 13, 15, 11, 3, 7, 6, 2, 10, 14},
										{3, 7, 15, 11, 10, 14, 6, 2, 0, 4, 12, 8, 9, 13, 5, 1},
										{0, 8, 9, 1, 3, 11, 10, 2, 6, 14, 15, 7, 5, 13, 12, 4},
										{5, 13, 12, 4, 6, 14, 15, 7, 3, 11, 10, 2, 0, 8, 9, 1},
										{6, 7, 5, 4, 0, 1, 3, 2, 10, 11, 9, 8, 12, 13, 15, 14},
										{15, 14, 12, 13, 9, 8, 10, 11, 3, 2, 0, 1, 5, 4, 6, 7},
										{10, 8, 12, 14, 6, 4, 0, 2, 3, 1, 5, 7, 15, 13, 9, 11},
										{12, 4, 5, 13, 15, 7, 6, 14, 10, 2, 3, 11, 9, 1, 0, 8},
										{9, 1, 0, 8, 10, 2, 3, 11, 15, 7, 6, 14, 12, 4, 5, 13},
										{5, 1, 9, 13, 12, 8, 0, 4, 6, 2, 10, 14, 15, 11, 3, 7},
										{6, 14, 15, 7, 5, 13, 12, 4, 0, 8, 9, 1, 3, 11, 10, 2},
										{9, 8, 10, 11, 15, 14, 12, 13, 5, 4, 6, 7, 3, 2, 0, 1},
										{10, 11, 9, 8, 12, 13, 15, 14, 6, 7, 5, 4, 0, 1, 3, 2},
										{3, 2, 0, 1, 5, 4, 6, 7, 15, 14, 12, 13, 9, 8, 10, 11},
										{12, 14, 10, 8, 0, 2, 6, 4, 5, 7, 3, 1, 9, 11, 15, 13},
										{15, 13, 9, 11, 3, 1, 5, 7, 6, 4, 0, 2, 10, 8, 12, 14},
										{3, 1, 5, 7, 15, 13, 9, 11, 10, 8, 12, 14, 6, 4, 0, 2},
										{5, 7, 3, 1, 9, 11, 15, 13, 12, 14, 10, 8, 0, 2, 6, 4},
										{6, 4, 0, 2, 10, 8, 12, 14, 15, 13, 9, 11, 3, 1, 5, 7},
										{9, 13, 5, 1, 0, 4, 12, 8, 10, 14, 6, 2, 3, 7, 15, 11},
										{10, 2, 3, 11, 9, 1, 0, 8, 12, 4, 5, 13, 15, 7, 6, 14},
										{15, 11, 3, 7, 6, 2, 10, 14, 12, 8, 0, 4, 5, 1, 9, 13},
										{12, 13, 15, 14, 10, 11, 9, 8, 0, 1, 3, 2, 6, 7, 5, 4}};

static const int tm1[NSTATES][N] = {{1, 2, 2, 3, 3, 5, 5, 4, 4, 9, 9, 8, 8, 7, 7, 6},
									{2, 11, 11, 13, 13, 16, 16, 15, 15, 17, 17, 14, 14, 12, 12, 10},
									{11, 0, 0, 23, 23, 20, 20, 18, 18, 22, 22, 24, 24, 21, 21, 19},
									{22, 25, 25, 7, 7, 1, 1, 17, 17, 26, 26, 9, 9, 27, 27, 14},
									{26, 18, 18, 19, 19, 10, 10, 0, 0, 28, 28, 29, 29, 30, 30, 23},
									{19, 13, 13, 15, 15, 14, 14, 10, 10, 4, 4, 6, 6, 31, 31, 11},
									{28, 17, 17, 14, 14, 12, 12, 25, 25, 11, 11, 13, 13, 16, 16, 7},
									{29, 21, 21, 27, 27, 22, 22, 30, 30, 20, 20, 26, 26, 0, 0, 16},
									{14, 24, 24, 5, 5, 23, 23, 12, 12, 6, 6, 2, 2, 15, 15, 22},
									{16, 31, 31, 1, 1, 4, 4, 28, 28, 14, 14, 25, 25, 13, 13, 29},
									{3, 22, 22, 24, 24, 21, 21, 5, 5, 0, 0, 23, 23, 20, 20, 12},
									{0, 1, 1, 28, 28, 25, 25, 29, 29, 27, 27, 30, 30, 26, 26, 31},
									{4, 26, 26, 9, 9, 27, 27, 8, 8, 25, 25, 7, 7, 1, 1, 20},
									{27, 5, 5, 12, 12, 2, 2, 22, 22, 7, 7, 17, 17, 9, 9, 24},
									{24, 30, 30, 16, 16, 28, 28, 21, 21, 10, 10, 11, 11, 18, 18, 27},
									{7, 29, 29, 31, 31, 19, 19, 1, 1, 3, 3, 4, 4, 8, 8, 28},
									{31, 23, 23, 18, 18, 24, 24, 19, 19, 15, 15, 10, 10, 6, 6, 0},
									{20, 6, 6, 2, 2, 15, 15, 3, 3, 24, 24, 5, 5, 23, 23, 4},
									{12, 4, 4, 6, 6, 31, 31, 2, 2, 13, 13, 15, 15, 14, 14, 3},
									{13, 27, 27, 30, 30, 26, 26, 16, 16, 1, 1, 28, 28, 25, 25, 21},
									{6, 28, 28, 29, 29, 30, 30, 31, 31, 18, 18, 19, 19, 10, 10, 1},
									{15, 7, 7, 17, 17, 9, 9, 14, 14, 5, 5, 12, 12, 2, 2, 25},
									{25, 10, 10, 11, 11, 18, 18, 13, 13, 30, 30, 16, 16, 28, 28, 15},
									{9, 16, 16, 21, 21, 11, 11, 27, 27, 12, 12, 22, 22, 17, 17, 30},
									{30, 8, 8, 20, 20, 3, 3, 26, 26, 19, 19, 0, 0, 29, 29, 9},
									{10, 3, 3, 4, 4, 8, 8, 6, 6, 29, 29, 31, 31, 19, 19, 2},
									{18, 12, 12, 22, 22, 17, 17, 24, 24, 16, 16, 21, 21, 11, 11, 5},
									{5, 19, 19, 0, 0, 29, 29, 23, 23, 8, 8, 20, 20, 3, 3, 18},
									{17, 20, 20, 26, 26, 0, 0, 9, 9, 21, 21, 27, 27, 22, 22, 8},
									{21, 15, 15, 10, 10, 6, 6, 11, 11, 23, 23, 18, 18, 24, 24, 13},
									{8, 14, 14, 25, 25, 13, 13, 7, 7, 31, 31, 1, 1, 4, 4, 17},
									{23, 9, 9, 8, 8, 7, 7, 20, 20, 2, 2, 3, 3, 5, 5, 26}};


static const int seqnums[NSTATES][N] ={{0, 1, 3, 2, 7, 6, 4, 5, 15, 14, 12, 13, 8, 9, 11, 10},
									   {0, 15, 1, 14, 3, 12, 2, 13, 7, 8, 6, 9, 4, 11, 5, 10},
									   {0, 7, 15, 8, 1, 6, 14, 9, 3, 4, 12, 11, 2, 5, 13, 10},
									   {4, 7, 3, 0, 11, 8, 12, 15, 5, 6, 2, 1, 10, 9, 13, 14},
									   {6, 7, 5, 4, 1, 0, 2, 3, 9, 8, 10, 11, 14, 15, 13, 12},
									   {14, 9, 1, 6, 15, 8, 0, 7, 13, 10, 2, 5, 12, 11, 3, 4},
									   {8, 7, 9, 6, 11, 4, 10, 5, 15, 0, 14, 1, 12, 3, 13, 2},
									   {12, 11, 3, 4, 13, 10, 2, 5, 15, 8, 0, 7, 14, 9, 1, 6},
									   {10, 9, 13, 14, 5, 6, 2, 1, 11, 8, 12, 15, 4, 7, 3, 0},
									   {2, 5, 13, 10, 3, 4, 12, 11, 1, 6, 14, 9, 0, 7, 15, 8},
									   {8, 15, 7, 0, 9, 14, 6, 1, 11, 12, 4, 3, 10, 13, 5, 2},
									   {0, 3, 7, 4, 15, 12, 8, 11, 1, 2, 6, 5, 14, 13, 9, 10},
									   {12, 15, 11, 8, 3, 0, 4, 7, 13, 14, 10, 9, 2, 1, 5, 6},
									   {4, 5, 7, 6, 3, 2, 0, 1, 11, 10, 8, 9, 12, 13, 15, 14},
									   {10, 11, 9, 8, 13, 12, 14, 15, 5, 4, 6, 7, 2, 3, 1, 0},
									   {6, 9, 7, 8, 5, 10, 4, 11, 1, 14, 0, 15, 2, 13, 3, 12},
									   {14, 13, 9, 10, 1, 2, 6, 5, 15, 12, 8, 11, 0, 3, 7, 4},
									   {2, 1, 5, 6, 13, 14, 10, 9, 3, 0, 4, 7, 12, 15, 11, 8},
									   {6, 1, 9, 14, 7, 0, 8, 15, 5, 2, 10, 13, 4, 3, 11, 12},
									   {8, 11, 15, 12, 7, 4, 0, 3, 9, 10, 14, 13, 6, 5, 1, 2},
									   {14, 15, 13, 12, 9, 8, 10, 11, 1, 0, 2, 3, 6, 7, 5, 4},
									   {12, 13, 15, 14, 11, 10, 8, 9, 3, 2, 0, 1, 4, 5, 7, 6},
									   {2, 3, 1, 0, 5, 4, 6, 7, 13, 12, 14, 15, 10, 11, 9, 8},
									   {4, 11, 5, 10, 7, 8, 6, 9, 3, 12, 2, 13, 0, 15, 1, 14},
									   {10, 5, 11, 4, 9, 6, 8, 7, 13, 2, 12, 3, 14, 1, 15, 0},
									   {14, 1, 15, 0, 13, 2, 12, 3, 9, 6, 8, 7, 10, 5, 11, 4},
									   {12, 3, 13, 2, 15, 0, 14, 1, 11, 4, 10, 5, 8, 7, 9, 6},
									   {2, 13, 3, 12, 1, 14, 0, 15, 5, 10, 4, 11, 6, 9, 7, 8},
									   {4, 3, 11, 12, 5, 2, 10, 13, 7, 0, 8, 15, 6, 1, 9, 14},
									   {6, 5, 1, 2, 9, 10, 14, 13, 7, 4, 0, 3, 8, 11, 15, 12},
									   {10, 13, 5, 2, 11, 12, 4, 3, 9, 14, 6, 1, 8, 15, 7, 0},
									   {8, 9, 11, 10, 15, 14, 12, 13, 7, 6, 4, 5, 0, 1, 3, 2}};


static const int tm2[NSTATES][N] ={{1, 2, 3, 2, 4, 5, 3, 5, 6, 7, 8, 7, 4, 9, 8, 9},
								   {2, 10, 11, 12, 13, 14, 11, 12, 15, 15, 16, 17, 13, 14, 16, 17},
								   {11, 18, 19, 18, 0, 20, 21, 22, 23, 23, 24, 24, 0, 20, 21, 22},
								   {7, 17, 7, 22, 9, 17, 9, 14, 1, 1, 25, 25, 26, 26, 27, 27},
								   {10, 0, 10, 19, 18, 26, 18, 19, 28, 0, 28, 29, 30, 23, 30, 29},
								   {31, 4, 13, 14, 11, 10, 19, 10, 31, 4, 13, 14, 6, 6, 15, 15},
								   {25, 25, 11, 12, 13, 14, 11, 12, 7, 28, 16, 17, 13, 14, 16, 17},
								   {26, 26, 27, 27, 0, 20, 21, 22, 16, 30, 29, 30, 0, 20, 21, 22},
								   {6, 6, 15, 15, 23, 23, 24, 24, 2, 12, 2, 22, 5, 12, 5, 14},
								   {31, 4, 13, 14, 1, 1, 25, 25, 31, 4, 13, 14, 16, 28, 29, 28},
								   {5, 12, 5, 3, 0, 20, 21, 22, 23, 23, 24, 24, 0, 20, 21, 22},
								   {0, 28, 29, 28, 31, 30, 29, 30, 1, 1, 25, 25, 26, 26, 27, 27},
								   {7, 20, 7, 8, 9, 4, 9, 8, 1, 1, 25, 25, 26, 26, 27, 27},
								   {12, 2, 22, 2, 12, 5, 27, 5, 17, 7, 22, 7, 17, 9, 24, 9},
								   {10, 11, 10, 21, 18, 11, 18, 27, 28, 16, 28, 21, 30, 16, 30, 24},
								   {19, 3, 1, 1, 19, 3, 31, 4, 29, 8, 7, 28, 29, 8, 31, 4},
								   {6, 6, 15, 15, 23, 23, 24, 24, 0, 10, 19, 10, 31, 18, 19, 18},
								   {6, 6, 15, 15, 23, 23, 24, 24, 2, 20, 2, 3, 5, 4, 5, 3},
								   {31, 4, 13, 14, 2, 12, 2, 3, 31, 4, 13, 14, 6, 6, 15, 15},
								   {16, 28, 21, 28, 16, 30, 13, 30, 1, 1, 25, 25, 26, 26, 27, 27},
								   {10, 1, 10, 19, 18, 31, 18, 19, 28, 6, 28, 29, 30, 31, 30, 29},
								   {12, 2, 25, 2, 12, 5, 14, 5, 17, 7, 15, 7, 17, 9, 14, 9},
								   {10, 11, 10, 25, 18, 11, 18, 13, 28, 16, 28, 15, 30, 16, 30, 13},
								   {21, 22, 11, 12, 27, 27, 11, 12, 21, 22, 16, 17, 9, 30, 16, 17},
								   {19, 3, 0, 20, 19, 3, 26, 26, 29, 8, 0, 20, 29, 8, 9, 30},
								   {19, 3, 2, 10, 19, 3, 31, 4, 29, 8, 6, 6, 29, 8, 31, 4},
								   {21, 22, 11, 12, 5, 18, 11, 12, 21, 22, 16, 17, 24, 24, 16, 17},
								   {19, 3, 0, 20, 19, 3, 5, 18, 29, 8, 0, 20, 29, 8, 23, 23},
								   {26, 26, 27, 27, 0, 20, 21, 22, 9, 17, 9, 8, 0, 20, 21, 22},
								   {6, 6, 15, 15, 23, 23, 24, 24, 11, 10, 21, 10, 11, 18, 13, 18},
								   {31, 4, 13, 14, 1, 1, 25, 25, 31, 4, 13, 14, 7, 17, 7, 8},
								   {20, 2, 3, 2, 26, 5, 3, 5, 20, 7, 8, 7, 23, 9, 8, 9}};		   


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


