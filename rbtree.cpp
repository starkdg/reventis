#include "rbtree.hpp"


int GetNodeCount(RBNode *h){
	if (h == NULL) return 0;
	return h->count;
}

bool IsRed(RBNode *h){
	if (h == NULL) return false;
	return h->red;
}

RBNode* RotateLeft(RBNode *h){
	RBNode *x = h->right;
	h->right = x->left;
	x->left = h;
	x->red = h->red;
	h->red = true;
	x->count = h->count;
	h->count = 1 + GetNodeCount(h->left) + GetNodeCount(h->right);
	h->maxseqn = (h->right != NULL) ? h->right->maxseqn : h->s;
	return x;
}

RBNode* RotateRight(RBNode *h){
	RBNode *x = h->left;
	h->left = x->right;
	x->right = h;
	x->red = h->red;
	h->red = true;
	x->count = h->count;
	h->count = 1 + GetNodeCount(h->left) + GetNodeCount(h->right);
	x->maxseqn = h->maxseqn;
	return x;
}

void FlipColors(RBNode *h){
	h->red = !(h->red);
	h->left->red = !(h->left->red);
	h->right->red = !(h->right->red);
}
RBNode* balance(RBNode *h){
	if (IsRed(h->right)) h = RotateLeft(h);
	if (IsRed(h->left) && IsRed(h->left->left)) h = RotateRight(h);
	if (IsRed(h->left) && IsRed(h->right)) FlipColors(h);
	h->count = 1 + GetNodeCount(h->left) + GetNodeCount(h->right);
	return h;
}

RBNode* MoveRedLeft(RBNode *h){
	FlipColors(h);
	if (IsRed(h->right->left)){
		h->right = RotateRight(h->right);
		h = RotateLeft(h);
	}
	return h;
}

RBNode* MoveRedRight(RBNode *h){
	FlipColors(h);
	if (IsRed(h->left->left)){
		h = RotateRight(h);
	}
	return h;
}

RBNode* minimum(RBNode *node){
	while (node->left != NULL)
		node = node->left;
	return node;
}

long long RBTreeDepth(const RBNode *node){
	long long depth = 0;
	while (node != NULL){
		if (!IsRed(node->left))	depth++;
		node = node->left;
	}
	return depth;
}

