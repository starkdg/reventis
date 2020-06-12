#ifndef _RBTREE_H
#define _RBTREE_H

#include <ctime>
#include "spfc.hpp"

/* value struct for nodes */
typedef struct value_t {
	double x, y;
	long long id;
	long long obj_id;
	unsigned long long cat;
	time_t start, end;
	value_t(){
		x = y = 0;
		id = 0;
		obj_id = 0;
		cat = 0;
		start = end = 0;
	}
	value_t(const value_t &other){
		x = other.x;
		y = other.y;
		id = other.id;
		obj_id = other.obj_id;
		cat = other.cat;
		start = other.start;
		end = other.end;
	}
	value_t& operator=(const value_t &other){
		x = other.x;
		y = other.y;
		id = other.id;
		obj_id = other.obj_id;
		cat = other.cat;
		start = other.start;
		end = other.end;
		return *this;
	}
} Value;

typedef struct query_region_t {
	double x_lower, x_upper;
	double y_lower, y_upper;
	time_t t_lower, t_upper;
} QueryRegion;

typedef struct rbnode_t {
	Seqn s;
	Seqn maxseqn;
	Value val;
	long long count;
	bool red;
    rbnode_t *left, *right;
} RBNode;

/* aux. structure to keep track of state of a node w.r.t. which child nodes 
have been visited in a query */
typedef struct node_state_t {
	RBNode *node;
	int visited; // 0 - no children visited, 1 - left visited, 2 - both visited
} NodeSt;

typedef struct result_t {
	Seqn s;
	Value val;
} Result;


int GetNodeCount(RBNode *h);

bool IsRed(RBNode *h);

RBNode* RotateLeft(RBNode *h);

RBNode* RotateRight(RBNode *h);

void FlipColors(RBNode *h);

RBNode* balance(RBNode *h);

RBNode* MoveRedLeft(RBNode *h);

RBNode* MoveRedRight(RBNode *h);

RBNode* minimum(RBNode *node);

long long RBTreeDepth(const RBNode *node);

#endif /* _RBTREE_H */
