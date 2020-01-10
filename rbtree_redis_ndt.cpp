#include <cstdlib>
#include <cstring>
#include <clocale>
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

static const struct lconv *locale;

static RedisModuleType *RBTreeType; 

/* value struct for nodes */
typedef struct value_t {
	double x, y;
	long long id;
	time_t start, end;
	RedisModuleString *descr;
	value_t(){}
	value_t(const value_t &other){
		id = other.id;
		start = other.start;
		end = other.end;
		descr = other.descr;
	}
	value_t& operator=(const value_t &other){
		id = other.id;
		start = other.start;
		end = other.end;
		descr = other.descr;
		return *this;
	}
} Value;


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
} RBTree;

int GetNodeCount(RBNode *h){
	if (h == NULL)
		return 0;
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
	if (RedisModule_ModuleTypeGetType(key) != RBTreeType){
		return NULL;
	}

	RBTree *tree = static_cast<RBTree*>(RedisModule_ModuleTypeGetValue(key));
	return tree;
}

RBTree* CreateRBTree(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != RBTreeType){
		return NULL;
	}
	RBTree *tree = NULL;
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		tree = static_cast<RBTree*>(RedisModule_Calloc(1, sizeof(RBTree)));
		RedisModule_ModuleTypeSetValue(key, RBTreeType, tree);
	} else {
		tree = static_cast<RBTree*>(RedisModule_ModuleTypeGetValue(key));
	}
	return tree;
}

/* ===============  tree manipulation functions ==================*/

RBNode* RotateLeft(RBNode *h){
	RBNode *x = h->right;
	h->right = x->left;
	x->left = h;
	x->red = h->red;
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

RBNode* rbtree_insert(RBNode *node, Seqn s, Value val, int level=0){
	if (node == NULL){
		RedisModule_Log(NULL, "debug", "insert (level %d) creating node", level);
		RBNode *x = static_cast<RBNode*>(RedisModule_Calloc(1, sizeof(RBNode)));
		x->s = s;
		x->maxseqn = s;
		x->val = val;
		x->red = true;
		x->count = 1;
		return x;
	}

	RedisModule_Log(NULL, "debug", "insert (level %d) node %u %u %u",
					level, node->s.arr[0], node->s.arr[1], node->s.arr[2]);
	int cmp = cmpseqnums(s, node->s);
	if (cmp <= 0){
		RedisModule_Log(NULL, "debug", "branch left");
		node->left = rbtree_insert(node->left, s, val, level+1);
	} else if (cmpseqnums(s, node->s)){
		RedisModule_Log(NULL, "debug", "branch right");
		node->right = rbtree_insert(node->right, s, val, level+1);
		node->maxseqn = node->right->maxseqn;
	}

	if (IsRed(node->right) && !IsRed(node->left)){
		node = RotateLeft(node);
	}

	if (IsRed(node->left) && IsRed(node->left->left)){
		node = RotateRight(node);
	}

	if (IsRed(node->left) && IsRed(node->right)){
		FlipColors(node);
	}

	node->count = 1 + GetNodeCount(node->left) + GetNodeCount(node->right);
	return node;
}

int RBTreeInsert(RBTree *tree, Seqn s, Value val){
	if (tree == NULL) return -1;

	RedisModule_Log(NULL, "debug", "Insert into rbtree");
	RedisModule_Log(NULL, "debug", "insert %u %u %u", s.arr[0], s.arr[1], s.arr[2]);
	RedisModule_Log(NULL, "debug", "insert %s %ld", RedisModule_StringPtrLen(val.descr, NULL), val.id);
	RBNode *node = rbtree_insert(tree->root, s, val);
	if (node == NULL){
		return -1;
	}
	node->red = false;
	tree->root = node;
	return 0;
}


void print_tree(RedisModuleCtx *ctx, RBNode *node, int level = 0){

	RedisModule_Log(ctx, "info", "(level %d) %s %lld red=%d", level,
					RedisModule_StringPtrLen(node->val.descr, NULL), node->val.id, node->red);
	
	if (node->left != NULL) print_tree(ctx, node->left, level+1);
	if (node->right != NULL) print_tree(ctx, node->right, level+1);
	return;
}

int RBTreePrint(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RBTree *tree = GetRBTree(ctx, keystr);
	if (tree == NULL || tree->root == NULL){
		RedisModule_Log(ctx, "info", "%s is empty", RedisModule_StringPtrLen(keystr, NULL));
		return 0;
	}
	print_tree(ctx, tree->root);
	return 0;
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
	datetime.tm_isdst = -1;
	if (sscanf(ptr, date_fmt, &datetime.tm_mon, &datetime.tm_mday, &datetime.tm_year) != 3)
		return -1;
	if (sscanf(ptr2, time_fmt, &datetime.tm_hour, &datetime.tm_min) != 2)
		return -1;
	datetime.tm_mon -= 1;
	datetime.tm_year = (datetime.tm_year >= 2000) ? datetime.tm_year%100 + 100 : datetime.tm_year%100;
	epoch = mktime(&datetime);
	return 0;
}


/* ------------------ RBTree type methods -----------------------------*/

extern "C" void* RBTreeTypeRdbLoad(RedisModuleIO *rdb, int encver){
	if (encver != RBTREE_ENCODING_VERSION){
		RedisModule_LogIOError(rdb, "verbose", "rdbload: unnable to encode for encver %d", encver);
		return NULL;
	}

	RBTree *tree = (RBTree*)RedisModule_Calloc(1, sizeof(RBTree*));
	uint64_t n_nodes = RedisModule_LoadUnsigned(rdb);
	for (size_t i=0;i<n_nodes;i++){
		Seqn s; // load seqn
		s.arr[0] = RedisModule_LoadUnsigned(rdb);
		s.arr[1] = RedisModule_LoadUnsigned(rdb);
		s.arr[2] = RedisModule_LoadUnsigned(rdb);

		Value val; // load id, longitude, latitude, start, end, descr
		val.id = RedisModule_LoadSigned(rdb);
		val.x  = RedisModule_LoadDouble(rdb);
		val.y = RedisModule_LoadDouble(rdb);
		val.start = RedisModule_LoadUnsigned(rdb);
		val.end = RedisModule_LoadUnsigned(rdb);
		val.descr = RedisModule_LoadString(rdb);

		RBTreeInsert(tree, s, val);
	}
	return (void*)tree;
}

extern "C" void RBTreeTypeRdbSave(RedisModuleIO *rdb, void *value){
	RBTree *tree = (RBTree*)value;
	queue<RBNode*> q;
	if (tree != NULL && tree->root != NULL)
		q.push(tree->root);

	RedisModule_SaveUnsigned(rdb, tree->root->count);
	while (!q.empty()){
		RBNode *node = q.front();

		// save seqn 
		RedisModule_SaveUnsigned(rdb, node->s.arr[0]);
		RedisModule_SaveUnsigned(rdb, node->s.arr[1]);
		RedisModule_SaveUnsigned(rdb, node->s.arr[1]);
		
		// id
		RedisModule_SaveSigned(rdb, node->val.id);

		// longitude and latitude
		RedisModule_SaveDouble(rdb, node->val.x);
		RedisModule_SaveDouble(rdb, node->val.y);

		// time start/end
		RedisModule_SaveUnsigned(rdb, node->val.start);
		RedisModule_SaveUnsigned(rdb, node->val.end);

		// title string
		RedisModule_SaveString(rdb, node->val.descr);
		if (node->left) q.push(node->left);
		if (node->right) q.push(node->right);
		q.pop();
	}
}

extern "C" void RBTreeTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value){
	RBTree *tree = (RBTree*)value;
	queue<RBNode*> q;
	if (tree != NULL && tree->root != NULL)
		q.push(tree->root);

	char s1[64];
	char s2[64];
	while (!q.empty()){
		RBNode *node = q.front();
		strftime(s1, 64, "%m-%d-%Y %H:%M", localtime(&(node->val.start)));
		strftime(s2, 64, "%m-%d-%Y %H:%M", localtime(&(node->val.end)));
		RedisModuleString *argstr = RedisModule_CreateStringPrintf(NULL, "%s %.6f %.6f %s %s %s",
												RedisModule_StringPtrLen(key, NULL),
												node->val.x, node->val.y, s1, s2,
												RedisModule_StringPtrLen(node->val.descr, NULL));
		RedisModule_EmitAOF(aof, "reventis.insert", "ssl", argstr, node->val.id);
		if (node->left) q.push(node->left);
		if (node->right) q.push(node->right);
		q.pop();
	}
}

extern "C" void RBTreeTypeFree(void *value){
	RBTree *tree = (RBTree*)value;
	queue<RBNode*> q;
	if (tree->root != NULL)
		q.push(tree->root);
	
	while (!q.empty()){
		RBNode *node = q.front();
		if (node->left != NULL)
			q.push(node->left);
		if (node->right != NULL)
			q.push(node->right);
		RedisModule_FreeString(NULL, node->val.descr);
		RedisModule_Free(node);
		q.pop();
	}
}

extern "C" size_t RBTreeTypeMemUsage(const void *value){
	RBTree *tree = (RBTree*)value;
	size_t node_sz = 0;
	if (tree->root != NULL)
		node_sz = sizeof(RBNode)*tree->root->count;
	return sizeof(*tree) + node_sz;
}

extern "C" void RBTreeTypeDigest(RedisModuleDigest *digest, void *value){
	REDISMODULE_NOT_USED(digest);
	REDISMODULE_NOT_USED(value);
}


/* ---------------RedisCommand functions ------------------------------*/

/* args: key longitude latitude date-start time-start date-end time-end title-description [id]*/
extern "C" int RBTreeInsert_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 9 || argc > 10)
		return RedisModule_WrongArity(ctx);

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
			RedisModule_ReplyWithError(ctx, "unable to retrieve id");
			return REDISMODULE_ERR;
		}
		event_id = RedisModule_CallReplyInteger(reply);
	} else {
		if (RedisModule_StringToLongLong(argv[9], &event_id) != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "unable to parse eventid arg");
			return REDISMODULE_ERR;
		}
	}
	double x, y;
	if (ParseLongLat(longitudestr, latitudestr, x, y) < 0){
		RedisModule_ReplyWithError(ctx, "bad longitude/latitude values");
		return REDISMODULE_ERR;
	}

	if (x < -180.0 || x > 180.0 || y < -90.0 || y > 90.0){
		RedisModule_ReplyWithError(ctx, "long./lat. values out of range");
		return REDISMODULE_ERR;
	}
	
	time_t t1;
	if (ParseDateTime(startdatestr, starttimestr, t1) < 0){
		RedisModule_ReplyWithError(ctx, "bad start time");
		return REDISMODULE_ERR;
	}

	time_t t2;
	if (ParseDateTime(enddatestr, endtimestr, t2) < 0){
		RedisModule_ReplyWithError(ctx, "bad end time");
		return REDISMODULE_ERR;
	}

	if (t1 > t2){
		RedisModule_ReplyWithError(ctx, "end time cannot be greater than start time");
		REDISMODULE_ERR;
	}

	Point pnt;
	pnt.arr[0] = static_cast<uint32_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[1] = static_cast<uint32_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[2] = static_cast<uint32_t>(t1);

	Value val;
	val.x = x;
	val.y = y;
	val.id = event_id;
	val.start = t1;
	val.end = t2;
	val.descr = titlestr;
	
	Seqn s;
	spfc_encode(pnt, s);

	RedisModule_Log(ctx, "debug", "Insert into %s", RedisModule_StringPtrLen(keystr, NULL));
	RedisModule_Log(ctx, "debug", "seqn = %u %u %u", s.arr[0], s.arr[1], s.arr[2]);
	RedisModule_Log(ctx, "debug", "id = %lld, %.6f/%.6f %s", val.id, val.x, val.y,
					RedisModule_StringPtrLen(val.descr, NULL));

	RBTree *tree = GetRBTree(ctx, keystr);
	if (tree == NULL){
		tree = CreateRBTree(ctx, keystr);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "unable to get datatype");
			return REDISMODULE_ERR;
		}
	}
	
	if (RBTreeInsert(tree, s, val) < 0){
		RedisModule_ReplyWithError(ctx, "unable to insert");
		return REDISMODULE_ERR;
	}
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	
	return REDISMODULE_OK;
}

/* args: mytree */
int RBTreePrint_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModuleString *keystr = argv[1];
	RBTreePrint(ctx, keystr);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}


/*  ============Onload init function ======================= */
extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	REDISMODULE_NOT_USED(argv);
	REDISMODULE_NOT_USED(argc);
	
	locale = localeconv();

	RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
	                             .rdb_load = RBTreeTypeRdbLoad,
	                             .rdb_save = RBTreeTypeRdbSave,
	                             .aof_rewrite = RBTreeTypeAofRewrite,
								 .mem_usage = RBTreeTypeMemUsage,
								 .digest = RBTreeTypeDigest,
								 .free = RBTreeTypeFree
	};
	

	if (RedisModule_Init(ctx, "reventis", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	RBTreeType = RedisModule_CreateDataType(ctx, "rbtree-ds", RBTREE_ENCODING_VERSION, &tm);
	if (RBTreeType == NULL)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.insert",  RBTreeInsert_RedisCmd,
								  "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.print", RBTreePrint_RedisCmd,
								  "read fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	return REDISMODULE_OK;
}

