#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <cstdint>
#include <vector>
#include <queue>
#include <ctime>
#include <cassert>
#include <limits>
#include "spfc.hpp"
#include "redismodule.h"

using namespace std;

#define LONG_LAT_SCALE_FACTOR 1000000
#define RBTREE_ENCODING_VERSION 0

static const char *date_fmt = "%d-%d-%d";         // MM-DD-YYY
static const char *time_fmt = "%d:%d";            // HH:MM
static const char *datetime_fmt = "%m-%d-%Y %H:%M"; // for use with strftime time parsing

static RedisModuleType *RBTreeType; 

/* value struct for nodes */
typedef struct value_t {
	double x, y;
	long long id;
	time_t start, end;
	RedisModuleString *descr;
	value_t(){}
	value_t(const value_t &other){
		x = other.x;
		y = other.y;
		id = other.id;
		start = other.start;
		end = other.end;
		descr = other.descr;
	}
	value_t& operator=(const value_t &other){
		x = other.x;
		y = other.y;
		id = other.id;
		start = other.start;
		end = other.end;
		descr = other.descr;
		return *this;
	}
} Value;

typedef struct result_t {
	Seqn s;
	double x, y;
	time_t start, end;
	long long id;
	RedisModuleString *descr;
	result_t(){}
	result_t(const result_t &other){
		x = other.x;
		y = other.y;
		s = other.s;
		id = other.id;
		start = other.start;
		end = other.end;
		descr = other.descr;
	}
} Result;


typedef struct rbnode_t {
	Seqn s;
	Seqn maxseqn;
	Value val;
	long long count;
	bool red;
    rbnode_t *left, *right;
} RBNode;

typedef struct rbtree_t {
	RBNode *root;
	RedisModuleDict *dict;
} RBTree;

/*================ Aux. functions ======================================= */

int GetNodeCount(RBNode *h){
	if (h == NULL) return 0;
	return h->count;
}

bool IsRed(RBNode *h){
	if (h == NULL) return false;
	return h->red;
}

/*================== Get RBTree with key =================================  */

RBTree* GetRBTree(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(key);
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return NULL;
	}
	
	if (RedisModule_ModuleTypeGetType(key) != RBTreeType){
		RedisModule_CloseKey(key);
		throw -1;
	}

	RBTree *tree = (RBTree*)RedisModule_ModuleTypeGetValue(key);

	RedisModule_CloseKey(key);
	return tree;
}

RBTree* CreateRBTree(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != RBTreeType){
		RedisModule_CloseKey(key);
		throw -1;
	}

	RBTree *tree = NULL;
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		tree = (RBTree*)RedisModule_Calloc(1, sizeof(RBTree));
		tree->dict = RedisModule_CreateDict(NULL);
		RedisModule_ModuleTypeSetValue(key, RBTreeType, tree);
	} else {
		tree = (RBTree*)RedisModule_ModuleTypeGetValue(key);
	}

	RedisModule_CloseKey(key);
	return tree;
}

/* ===============  tree manipulation functions ==================*/

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

RBNode* delete_min(RedisModuleCtx *ctx, RBNode *node){
	if (node->left == NULL) {
		RedisModule_FreeString(ctx, node->val.descr);
		RedisModule_Free(node);
		return NULL;
	}
	if (!IsRed(node->left) && !IsRed(node->left->left))
		node = MoveRedLeft(node);
	node->left = delete_min(ctx, node->left);
	return balance(node);
}

void RBTreeDeleteMin(RedisModuleCtx *ctx, RBTree *tree){
	if (tree == NULL || tree->root == NULL)
		return;
	if (!IsRed(tree->root->left) && !IsRed(tree->root->right))
		tree->root->red = true;

	tree->root = delete_min(ctx, tree->root);
	
	if (tree->root != NULL)
		tree->root->red = false;
}

RBNode* delete_key(RedisModuleCtx *ctx, RBNode *node, Seqn s, long long id, int level = 0){
	if (node == NULL) return NULL;

	if (cmpseqnums(s, node->s) < 0 || (cmpseqnums(s, node->s) == 0 && id != node->val.id)){
		if (node->left != NULL && !IsRed(node->left) && !IsRed(node->left->left)){
			node = MoveRedLeft(node);
		}
		node->left = delete_key(ctx, node->left, s, id, level+1);
	} else {
		if (IsRed(node->left)) node = RotateRight(node);
			
		if (cmpseqnums(s, node->s) == 0 && id == node->val.id && node->right == NULL){
			RedisModule_FreeString(ctx, node->val.descr);
			RedisModule_Free(node);
			return NULL;
		}

		if (node->right != NULL && !IsRed(node->right) && !IsRed(node->right->left))
			node = MoveRedRight(node);

		if (cmpseqnums(s, node->s) == 0){
			if (id == node->val.id){
				RBNode *xnode = minimum(node->right);
				node->s = xnode->s;
				node->val = xnode->val;
				node->maxseqn = node->right->maxseqn;
				node->right = delete_min(ctx, node->right);
				node->count = 1 + GetNodeCount(node->left) + GetNodeCount(node->right);
			}
		} else if (cmpseqnums(s, node->maxseqn) <= 0){
			node->right = delete_key(ctx, node->right, s, id, level+1);
		}
	}
	node->count = 1 + GetNodeCount(node->left) + GetNodeCount(node->right);
	return balance(node);
}

int RBTreeDelete(RedisModuleCtx *ctx, RBTree *tree, Seqn s, long long eventid){
	if (tree == NULL || tree->root == NULL)	return -1;

	if (!IsRed(tree->root->left) && !IsRed(tree->root->right))
		tree->root->red = true;

	tree->root = delete_key(ctx, tree->root, s, eventid);
	
	if (tree->root != NULL)
		tree->root->red = false;

	RedisModule_DictDelC(tree->dict, &eventid, sizeof(long long), NULL);
	return 0;
}

RBNode* rbtree_insert(RBTree *tree, RBNode *node, Seqn s, Value val, int level=0){
	if (node == NULL){
		RBNode *x = (RBNode*)RedisModule_Calloc(1, sizeof(RBNode));
		x->s = s;
		x->maxseqn = s;
		x->val = val;
		x->red = true;
		x->count = 1;
		x->left = x->right = NULL;
		RedisModule_DictReplaceC(tree->dict, &(x->val.id), sizeof(long long), x);
		return x;
	}

	if (cmpseqnums(s, node->s) <= 0){
		node->left = rbtree_insert(tree, node->left, s, val, level+1);
	} else {
		node->right = rbtree_insert(tree, node->right, s, val, level+1);
		node->maxseqn = node->right->maxseqn;
	}

	if (IsRed(node->right) && !IsRed(node->left))     node = RotateLeft(node);
	if (IsRed(node->left) && IsRed(node->left->left)) node = RotateRight(node);
	if (IsRed(node->left) && IsRed(node->right))      FlipColors(node);

	node->count = 1 + GetNodeCount(node->left) + GetNodeCount(node->right);

	return node;
}

int RBTreeInsert(RBTree *tree, Seqn s, Value val){
	if (tree == NULL) return -1;

	RBNode *node = rbtree_insert(tree, tree->root, s, val);
	if (node == NULL) return -1;
	
	node->red = false;
	tree->root = node;
	return 0;
}

/* in-order travsal */
void print_tree(RedisModuleCtx *ctx, RBNode *node, int level = 0){
	if (node == NULL) return;

	// print left 
	print_tree(ctx, node->left, level+1);

	// print node 
	double x = node->val.x;
	double y = node->val.y;

	char scratch[64];
	strftime(scratch, 64, datetime_fmt, localtime(&(node->val.start)));

	RedisModule_Log(ctx, "info", "print (level %d) %s %.6f/%.6f id=%lld %s red=%d", level,
					RedisModule_StringPtrLen(node->val.descr, NULL), x, y,
					node->val.id, scratch, node->red);

	// print right 
	print_tree(ctx, node->right, level+1);
}

long long RBTreePrint(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RBTree *tree = GetRBTree(ctx, keystr);
	if (tree == NULL || tree->root == NULL){
		RedisModule_Log(ctx, "info", "%s is empty", RedisModule_StringPtrLen(keystr, NULL));
		return 0;
	}

	RedisModule_Log(ctx, "info", "print tree: %s", RedisModule_StringPtrLen(keystr, NULL));
	print_tree(ctx, tree->root);
	return tree->root->count;
}

int rbtree_query(RBNode *node, const Region qr, Seqn &next, vector<Result> &results, int level=0){
	if (node == NULL) return 0;

	if (cmpseqnums(next, node->s) <= 0){
		if (rbtree_query(node->left, qr, next, results, level+1) < 0)
			return -1;
	}

	if (contains(qr, node->s)){
		Point p;
		spfc_decode(node->s, p);

		Result res;
		res.s = node->s;
		res.x = node->val.x;
		res.y = node->val.y;
		res.start = node->val.start;
		res.end = node->val.end;
		res.id = node->val.id;
		res.descr = node->val.descr;
		results.push_back(res);
	}

	next = node->s;
	Seqn prev = next;
	if (!contains(qr, next) && !next_match(qr, prev, next))
		return 0;
	
	if (cmpseqnums(next, node->maxseqn) <= 0){
		if (rbtree_query(node->right, qr, next, results, level+1) < 0)
			return -1;
	}

	return 0;
}

int RBTreeQuery(RBTree *tree, const Region qr, vector<Result> &results){
	if (tree->root == NULL)	return -1;

	Seqn prev, next;
	if (next_match(qr, prev, next)){
		if (rbtree_query(tree->root, qr, next, results) < 0)
			return -1;
		
	}
	return 0;
}

long long RBTreeDepth(const RBNode *node){
	long long depth = 0;
	while (node != NULL){
		if (!IsRed(node->left))
			depth++;
		node = node->left;
	}
	return depth;
}

/* =============== parse longitude latitude functions ==================== */

int ParseLongLat(RedisModuleString *xstr, RedisModuleString *ystr, double &x, double &y){
	if (RedisModule_StringToDouble(xstr, &x) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_StringToDouble(ystr, &y) == REDISMODULE_ERR)
		return -1;
	return 0;
}

/* ================ parse date time functions ============================ */

int ParseDateTime(RedisModuleString *datestr, RedisModuleString *timestr, time_t &epoch){
	const char *ptr = RedisModule_StringPtrLen(datestr, NULL);
	const char *ptr2 = RedisModule_StringPtrLen(timestr, NULL);
	if (ptr == NULL || ptr2 == NULL) return -1;
	
	tm datetime;
	datetime.tm_sec = 0;
	datetime.tm_isdst = 0;
	datetime.tm_wday = -1;
	datetime.tm_yday = -1;
	if (sscanf(ptr, date_fmt, &datetime.tm_mon, &datetime.tm_mday, &datetime.tm_year) != 3)	return -1;
	if (sscanf(ptr2, time_fmt, &datetime.tm_hour, &datetime.tm_min) != 2) return -1;
	datetime.tm_mon -= 1;
	datetime.tm_year = (datetime.tm_year >= 2000) ? datetime.tm_year%100 + 100 : datetime.tm_year%100;

	if (datetime.tm_mon < 0 || datetime.tm_mon > 11) return -1;
	if (datetime.tm_mday <= 0 || datetime.tm_mday > 31)	return -1;
	if (datetime.tm_year < 0 || datetime.tm_year > 300)	return -1;
	epoch = mktime(&datetime);

	return 0;
}


/* ------------------ RBTree type methods -----------------------------*/

extern "C" void* RBTreeTypeRdbLoad(RedisModuleIO *rdb, int encver){
	if (encver != RBTREE_ENCODING_VERSION){
		RedisModule_LogIOError(rdb, "warning", "rdbload: unnable to encode for encver %d", encver);
		return NULL;
	}

	RBTree *tree = (RBTree*)RedisModule_Calloc(1, sizeof(RBTree));
	tree->dict = RedisModule_CreateDict(NULL);
	
	uint64_t n_nodes = RedisModule_LoadUnsigned(rdb);

	RedisModule_LogIOError(rdb, "debug", "rdbload: %llu nodes", n_nodes);
	for (uint64_t i=0;i<n_nodes;i++){
		Seqn s; // load seqn
		s.arr[0] = RedisModule_LoadUnsigned(rdb);
		s.arr[1] = RedisModule_LoadUnsigned(rdb);
		s.arr[2] = RedisModule_LoadUnsigned(rdb);

		Value val; // load id, longitude, latitude, start, end, descr
		val.x = RedisModule_LoadDouble(rdb);
		val.y = RedisModule_LoadDouble(rdb);
		val.id = RedisModule_LoadSigned(rdb);
		val.start = static_cast<time_t>(RedisModule_LoadUnsigned(rdb));
		val.end = static_cast<time_t>(RedisModule_LoadUnsigned(rdb));
		val.descr = RedisModule_LoadString(rdb);

		if (RBTreeInsert(tree, s, val) < 0){
			RedisModule_LogIOError(rdb, "warning", "rdbload: unable to insert %ld", val.id);
			return NULL;
		}
	}
	return (void*)tree;
}

extern "C" void RBTreeTypeRdbSave(RedisModuleIO *rdb, void *value){
	RBTree *tree = (RBTree*)value;

	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tree->dict, "^", NULL, 0);

	uint64_t sz = RedisModule_DictSize(tree->dict);
	RedisModule_SaveUnsigned(rdb, sz);
	RedisModule_LogIOError(rdb, "debug", "rdbsave: %llu nodes", sz);
	
	unsigned char *dict_key;
	RBNode *node;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, NULL, (void**)&node)) != NULL){

		// save seqn 
		RedisModule_SaveUnsigned(rdb, node->s.arr[0]);
		RedisModule_SaveUnsigned(rdb, node->s.arr[1]);
		RedisModule_SaveUnsigned(rdb, node->s.arr[2]);

		// long./lat.
		RedisModule_SaveDouble(rdb, node->val.x);
		RedisModule_SaveDouble(rdb, node->val.y);
		
		// id
		RedisModule_SaveSigned(rdb, node->val.id);

		// time start/end
		RedisModule_SaveUnsigned(rdb, node->val.start);
		RedisModule_SaveUnsigned(rdb, node->val.end);

		// title string
		RedisModule_SaveString(rdb, node->val.descr);
	}
	RedisModule_DictIteratorStop(iter);
}

extern "C" void RBTreeTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value){
	RBTree *tree = (RBTree*)value;

	uint64_t sz = RedisModule_DictSize(tree->dict);
	RedisModule_LogIOError(aof, "debug", "AofRewrite: %llu nodes", sz);

	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tree->dict, "^", NULL, 0);
	
	char s1[64];
	char s2[64];
	unsigned char *dict_key;
	RBNode *node;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, NULL, (void**)&node)) != NULL){
		strftime(s1, 64, "%m-%d-%Y %H:%M", localtime(&(node->val.start)));
		strftime(s2, 64, "%m-%d-%Y %H:%M", localtime(&(node->val.end)));
		
		RedisModuleString *argstr = RedisModule_CreateStringPrintf(NULL, "%s %.6f %.6f %s %s %s",
												RedisModule_StringPtrLen(key, NULL),
												node->val.x, node->val.y, s1, s2,
												RedisModule_StringPtrLen(node->val.descr, NULL));
		RedisModule_EmitAOF(aof, "reventis.insert", "sl", argstr, node->val.id);
	}
	
	RedisModule_DictIteratorStop(iter);
}

extern "C" void RBTreeTypeFree(void *value){
	RBTree *tree = (RBTree*)value;

	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tree->dict, "^", NULL, 0);
	unsigned char *dict_key;
	size_t keylen;
	RBNode *node;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, &keylen, (void**)&node)) != NULL){
		RedisModule_FreeString(NULL, node->val.descr);
		RedisModule_Free(node);
		RedisModule_DictDelC(tree->dict, dict_key, keylen, NULL);
	}
	RedisModule_FreeDict(NULL, tree->dict);
}

extern "C" size_t RBTreeTypeMemUsage(const void *value){
	RBTree *tree = (RBTree*)value;
	uint64_t sz = RedisModule_DictSize(tree->dict);
	return sz*sizeof(RBNode);
}

extern "C" void RBTreeTypeDigest(RedisModuleDigest *digest, void *value){
	REDISMODULE_NOT_USED(digest);
	REDISMODULE_NOT_USED(value);
}

/* ---------------RedisCommand functions ------------------------------*/

/* args: key longitude latitude date-start time-start date-end time-end title-description [id]*/
/* an optional event_id argument is included for replication commands */
/* return event-id assigned to entry */
extern "C" int RBTreeInsert_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 9 || argc > 10) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	
	RedisModuleString *keystr = argv[1];
	RedisModuleString *longitudestr = argv[2];
	RedisModuleString *latitudestr = argv[3];
	RedisModuleString *startdatestr = argv[4];
	RedisModuleString *starttimestr = argv[5];
	RedisModuleString *enddatestr = argv[6];
	RedisModuleString *endtimestr = argv[7];
	RedisModuleString *titlestr = argv[8];
	long long event_id;

	if (argc < 10){
		// assign id number for new entry 
		RedisModuleCallReply *reply = RedisModule_Call(ctx, "INCRBY", "cl", "event:id", 10);
		if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to generate unique Id");
			return REDISMODULE_ERR;
		}
		event_id = RedisModule_CallReplyInteger(reply);
	} else {
		if (RedisModule_StringToLongLong(argv[9], &event_id) != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to parse Id arg");
			return REDISMODULE_ERR;
		}
	}
	double x, y;
	if (ParseLongLat(longitudestr, latitudestr, x, y) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - bad longitude/latitude arg values");
		return REDISMODULE_ERR;
	}

	if (x < -180.0 || x > 180.0 || y < -90.0 || y > 90.0){
		RedisModule_ReplyWithError(ctx, "ERR - longitude/latitude args out of range");
		return REDISMODULE_ERR;
	}
	
	time_t t1;
	if (ParseDateTime(startdatestr, starttimestr, t1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse start time");
		return REDISMODULE_ERR;
	}

	time_t t2;
	if (ParseDateTime(enddatestr, endtimestr, t2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse end time");
		return REDISMODULE_ERR;
	}

	if (t1 > t2){
		RedisModule_ReplyWithError(ctx, "ERR - start time cannot be later than end time");
		return REDISMODULE_ERR;
	}

	Point pnt;
	pnt.arr[0] = static_cast<uint32_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[1] = static_cast<uint32_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[2] = static_cast<uint32_t>(t1);

	RedisModule_RetainString(ctx, titlestr);
	
	Value val;
	val.x = x;
	val.y = y;
	val.id = event_id;
	val.start = t1;
	val.end = t2;
	val.descr = titlestr;
	
	Seqn s;
	spfc_encode(pnt, s);

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, keystr);
		if (tree == NULL) tree = CreateRBTree(ctx, keystr);
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type. Delete first.");
		return REDISMODULE_ERR;
	}
	
	if (RBTreeInsert(tree, s, val) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Insert failed");
		return REDISMODULE_ERR;
	}
	
	RedisModule_ReplyWithLongLong(ctx, event_id);

	/* replicate command with eventid tacked on as last argument */
	int r = RedisModule_Replicate(ctx, "reventis.insert", "ssssssssl",
								  keystr, longitudestr, latitudestr,
								  startdatestr, starttimestr, enddatestr, endtimestr,
								  titlestr, event_id);
	if (r == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "WARN - Unable to replicate for id %ld", event_id);
		return REDISMODULE_ERR;
	}
	
	return REDISMODULE_OK;
}

/* args: mytree event_id */
extern "C" int RBTreeLookup_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 3) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	RedisModuleString *keystr = argv[1];

	long long event_id;
	if (RedisModule_StringToLongLong(argv[2], &event_id) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse id arg value");
		return REDISMODULE_ERR;
	}

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, keystr);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - No such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - Key exists for different type");
		return REDISMODULE_ERR;
	}

	int nokey;
	RBNode *idnode = (RBNode*)RedisModule_DictGetC(tree->dict, (void*)&event_id, sizeof(event_id), &nokey);
	if (nokey || idnode == NULL){
		RedisModule_ReplyWithSimpleString(ctx, "None");
		return REDISMODULE_OK;
	}


	char s1[64];
	char s2[64];
	strftime(s1, 64, datetime_fmt, localtime(&(idnode->val.start)));
	strftime(s2, 64, datetime_fmt, localtime(&(idnode->val.end)));

	RedisModuleString *replystr = RedisModule_CreateStringPrintf(ctx, "%s %f %f %s %s",
											  RedisModule_StringPtrLen(idnode->val.descr, NULL),
										      idnode->val.x, idnode->val.y, s1, s2);
	
	RedisModule_ReplyWithString(ctx, replystr);
	return REDISMODULE_OK;
}

/* args: mytree eventid */
extern "C" int RBTreeDelete_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 3) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	RedisModuleString *keystr = argv[1];
	RedisModuleString *eventidstr = argv[2];

	long long event_id;
	if (RedisModule_StringToLongLong(eventidstr, &event_id) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse id arg value");
		return REDISMODULE_ERR;
	}

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, keystr);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - Empty key");
			return REDISMODULE_OK;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - Key exists for different type");
		return REDISMODULE_ERR;
	}

	int nokey;
	RBNode *target = (RBNode*)RedisModule_DictGetC(tree->dict, (void*)&event_id, sizeof(event_id), &nokey);
	if (nokey || target == NULL){
		RedisModule_ReplyWithNull(ctx);
		return REDISMODULE_OK;
	}

	if (RBTreeDelete(ctx, tree, target->s, event_id) < 0){
		RedisModule_ReplyWithError(ctx, "ERR Unable to delete");
		return REDISMODULE_ERR;
	}
	
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);
	
	return REDISMODULE_OK;
}

/* purge all entries before specied datetime */
/* args: mytree date time */
/* return number of deletions */
int RBTreePurge_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 4) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	RedisModuleString *keystr = argv[1];
	RedisModuleString *datestr = argv[2];
	RedisModuleString *timestr = argv[3];

	time_t t;
	if (ParseDateTime(datestr, timestr, t) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse date/time");
		return REDISMODULE_ERR;
	}

	Region qr;
	qr.lower[0] = qr.lower[1] = qr.lower[2] = 0;
	qr.upper[0] = qr.upper[1] = numeric_limits<uint32_t>::max();
	qr.upper[2] = static_cast<uint32_t>(t);

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, keystr);
		if (tree == NULL){
			RedisModule_ReplyWithNull(ctx);
			return REDISMODULE_OK;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key exists for different type");
		return REDISMODULE_ERR;
	}

	vector<Result> results;
	if (RBTreeQuery(tree, qr, results) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to query");
		return REDISMODULE_ERR;
	}

	long long n_deletes = (long long)results.size();

	for (Result r : results){
		if (RBTreeDelete(ctx, tree, r.s, r.id) < 0){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to delete");
			return REDISMODULE_OK;
		}
	}
	
	RedisModule_ReplyWithLongLong(ctx, n_deletes);
	return REDISMODULE_OK;
}

/* args: mytree longitude-range latitude-range start-range end-range */
int RBTreeDelBlk_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 10) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	RedisModuleString *keystr = argv[1];
	
	double x1, y1;
	if (ParseLongLat(argv[2], argv[4], x1, y1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse longitude/latitude arg values");
		return REDISMODULE_ERR;
	}
	double x2, y2;
	if (ParseLongLat(argv[3], argv[5], x2, y2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Uable to parse longitude/latitude arg values");
		return REDISMODULE_ERR;
	}

	if (x1 < -180.0 || x1 > 180.0 || x2 < -180.0 || x2 > 180.0){
		RedisModule_ReplyWithError(ctx, "ERR - longitude/latitude out of range");
		return REDISMODULE_ERR;
	}

	time_t t1, t2;
	if (ParseDateTime(argv[6], argv[7], t1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse start date time arg values");
		return REDISMODULE_ERR;
	}
	if (ParseDateTime(argv[8], argv[9], t2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse end date time arg values");
		return REDISMODULE_ERR;
	}

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, keystr);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key already exists for different type");
		return REDISMODULE_ERR;
	}
	
	Region qr;
	qr.lower[0] = static_cast<uint32_t>((x1+180.0)*LONG_LAT_SCALE_FACTOR);
	qr.lower[1] = static_cast<uint32_t>((y1+90.0)*LONG_LAT_SCALE_FACTOR);
	qr.lower[2] = static_cast<uint32_t>(t1);
	qr.upper[0] = static_cast<uint32_t>((x2+180.0)*LONG_LAT_SCALE_FACTOR);
	qr.upper[1] = static_cast<uint32_t>((y2+90.0)*LONG_LAT_SCALE_FACTOR);
	qr.upper[2] = static_cast<uint32_t>(t2);

	vector<Result> results;
	if (RBTreeQuery(tree, qr, results) < 0){
		RedisModule_ReplyWithNull(ctx);
		return REDISMODULE_ERR;
	}

	for (Result r : results){
		if (RBTreeDelete(ctx, tree, r.s, r.id) < 0){
			RedisModule_ReplyWithError(ctx, "ERR - unable to delete");
			return REDISMODULE_ERR;
		}
	}

	RedisModule_ReplyWithLongLong(ctx, results.size());
	RedisModule_ReplicateVerbatim(ctx);

	return REDISMODULE_OK;
}

/* args: mytree longitude-range latitude-range start-range end-range */
int RBTreeQuery_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 10) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	
	RedisModuleString *keystr = argv[1];

	double x1, y1;
	if (ParseLongLat(argv[2], argv[4], x1, y1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse lower longitude/latitude arg values");
		return REDISMODULE_ERR;
	}
	double x2, y2;
	if (ParseLongLat(argv[3], argv[5], x2, y2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse upper longitude/latitude arg values");
		return REDISMODULE_ERR;
	}

	if (x1 < -180.0 || x1 > 180.0 || x2 < -180.0 || x2 > 180.0){
		RedisModule_ReplyWithError(ctx, "ERR - longitude/latitude arg values out of range");
		return REDISMODULE_ERR;
	}

	time_t t1, t2;
	if (ParseDateTime(argv[6], argv[7], t1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse lower date time arg values");
		return REDISMODULE_ERR;
	}
	if (ParseDateTime(argv[8], argv[9], t2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse upper date time arg values");
		return REDISMODULE_ERR;
	}

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, keystr);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - No such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key already exists for different type");
		return REDISMODULE_ERR;
	}
	
	Region qr;
	qr.lower[0] = static_cast<uint32_t>((x1+180.0)*LONG_LAT_SCALE_FACTOR);
	qr.lower[1] = static_cast<uint32_t>((y1+90.0)*LONG_LAT_SCALE_FACTOR);
	qr.lower[2] = static_cast<uint32_t>(t1);
	qr.upper[0] = static_cast<uint32_t>((x2+180.0)*LONG_LAT_SCALE_FACTOR);
	qr.upper[1] = static_cast<uint32_t>((y2+90.0)*LONG_LAT_SCALE_FACTOR);
	qr.upper[2] = static_cast<uint32_t>(t2);

	const char *out_fmt = "title: %s id: %lld %f/%f %s to %s";
	
	vector<Result> results;
	if (RBTreeQuery(tree, qr, results) < 0){
		RedisModule_ReplyWithError(ctx, "Err - Unable to query");
		return REDISMODULE_ERR;
	}

	char s1[64];
	char s2[64];

	RedisModule_ReplyWithArray(ctx, results.size());
	for (Result v : results){
		strftime(s1, 64, datetime_fmt, localtime(&(v.start)));
		strftime(s2, 64, datetime_fmt, localtime(&(v.end)));
		RedisModuleString *resp = RedisModule_CreateStringPrintf(ctx, out_fmt,
									RedisModule_StringPtrLen(v.descr, NULL),
									v.id, v.x, v.y, s1, s2);
		RedisModule_ReplyWithString(ctx, resp);
	}

	return REDISMODULE_OK;
}

/* args: mytree */
/* print tree to log file */
/* return a count for number of nodes */
int RBTreePrint_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);
	RedisModuleString *keystr = argv[1];
	long long count = RBTreePrint(ctx, keystr);
	RedisModule_ReplyWithLongLong(ctx, count);
	return REDISMODULE_OK;
}

int RBTreeSize_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - No such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key already exists for different type");
		return REDISMODULE_ERR;
	}

	RedisModule_ReplyWithLongLong(ctx, tree->root->count);
	return REDISMODULE_OK;
}

extern "C" int RBTreeDepth_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);

	RBTree *tree;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - No such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key already exists for different type");
		return REDISMODULE_ERR;
	}

	long long height = RBTreeDepth(tree->root);
	RedisModule_ReplyWithLongLong(ctx, height);
	return REDISMODULE_OK;
}

/*  ============Onload init function ======================= */
extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (RedisModule_Init(ctx, "reventis", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "unable to init module");
		return REDISMODULE_ERR;
	}
	
	RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
	                             .rdb_load = RBTreeTypeRdbLoad,
	                             .rdb_save = RBTreeTypeRdbSave,
	                             .aof_rewrite = RBTreeTypeAofRewrite,
								 .mem_usage = RBTreeTypeMemUsage,
								 .digest = RBTreeTypeDigest,
								 .free = RBTreeTypeFree};
	
	RBTreeType = RedisModule_CreateDataType(ctx, "RBTree-DS", RBTREE_ENCODING_VERSION, &tm);
	if (RBTreeType == NULL)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.insert",  RBTreeInsert_RedisCmd,
								  "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.lookup", RBTreeLookup_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR);
	
	if (RedisModule_CreateCommand(ctx, "reventis.del", RBTreeDelete_RedisCmd,
								  "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.purge", RBTreePurge_RedisCmd,
								  "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.delblk", RBTreeDelBlk_RedisCmd,
								  "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.query", RBTreeQuery_RedisCmd,
								  "readonly", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.print", RBTreePrint_RedisCmd,
								  "readonly", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.size", RBTreeSize_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.depth", RBTreeDepth_RedisCmd,
								  "readonly admin fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	return REDISMODULE_OK;
}

