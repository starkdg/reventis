#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <cstdint>
#include <vector>
#include <queue>
#include <stack>
#include <set>
#include <map>
#include <algorithm>
#include <ctime>
#include <chrono>
#include <cassert>
#include <limits>
#include "spfc.hpp"
#include "rbtree.hpp"
#include "haversine.hpp"
#include "redismodule.h"

using namespace std;

#define LONG_LAT_SCALE_FACTOR 1000000
#define RBTREE_ENCODING_VERSION 1

static const char *date_fmt = "%d-%d-%d";              // MM-DD-YYY
static const char *time_fmt = "%d:%d:%d";              // HH:MM
static const char *datetime_fmt = "%m-%d-%Y %H:%M:%S"; // for use with strftime time parsing

static const char *descr_field = "descr";

static RedisModuleType *RBTreeType; 

typedef struct rbtree_t {
	RBNode *root;
	RedisModuleDict *dict;
	RedisModuleDict *obj_dict;
	uint64_t object_count;
} RBTree;

/*======================= dyn mem management ===============================*/

void* operator new(size_t sz){
	void *ptr = RedisModule_Alloc(sz);
	return ptr;
}

void operator delete(void *p){
	RedisModule_Free(p);
}

/*================ Aux. RedisModule functions ======================================= */

/* check to see if point defined in Value is contained in query region*/
bool contains(const QueryRegion qr, const Value val){
	if (val.x < qr.x_lower || val.x > qr.x_upper)
		return false;
	else if (val.y < qr.y_lower || val.y > qr.y_upper)
		return false;
	else if (val.start < qr.t_lower || val.start > qr.t_upper)
		return false;
	return true;
}

/* cast a query in terms of the spfc function */
int cast_query_region(const QueryRegion qr, Region &r){
	r.lower[0] = static_cast<uint64_t>((qr.x_lower + 180.0)*LONG_LAT_SCALE_FACTOR);
	r.lower[1] = static_cast<uint64_t>((qr.y_lower + 90.0)*LONG_LAT_SCALE_FACTOR);
	r.lower[2] = static_cast<uint64_t>(qr.t_lower);
	r.upper[0] = static_cast<uint64_t>((qr.x_upper + 180.0)*LONG_LAT_SCALE_FACTOR);
	r.upper[1] = static_cast<uint64_t>((qr.y_upper + 90.0)*LONG_LAT_SCALE_FACTOR);
	r.upper[2] = static_cast<uint64_t>(qr.t_upper);
	return 0;
}

int ParseLongLat(RedisModuleString *xstr, RedisModuleString *ystr, double &x, double &y){
	if (RedisModule_StringToDouble(xstr, &x) == REDISMODULE_ERR)
		return -1;
	if (RedisModule_StringToDouble(ystr, &y) == REDISMODULE_ERR)
		return -1;
	return 0;
}

int ParseDateTime(RedisModuleString *datestr, RedisModuleString *timestr, time_t &epoch){
	const char *ptr = RedisModule_StringPtrLen(datestr, NULL);
	const char *ptr2 = RedisModule_StringPtrLen(timestr, NULL);
	if (ptr == NULL || ptr2 == NULL) return -1;

	setenv("TZ", "GMT", 1);
	
	tm datetime;
	memset(&datetime, 0, sizeof(tm));
	if (sscanf(ptr, date_fmt, &datetime.tm_mon, &datetime.tm_mday, &datetime.tm_year) < 3)	return -1;
	if (sscanf(ptr2, time_fmt, &datetime.tm_hour, &datetime.tm_min, &datetime.tm_sec) < 2) return -1;
	datetime.tm_mon -= 1;
	datetime.tm_year = (datetime.tm_year >= 2000) ? datetime.tm_year%100 + 100 : datetime.tm_year%100;

	if (datetime.tm_mon < 0 || datetime.tm_mon > 11) return -1;
	if (datetime.tm_mday <= 0 || datetime.tm_mday > 31)	return -1;
	if (datetime.tm_year < 0 || datetime.tm_year > 300)	return -1;
	epoch = mktime(&datetime);

	unsetenv("TZ");
	
	return 0;
}

int ParseRadius(RedisModuleString *radius_str, RedisModuleString *unit_str, double &radius){
	double val;
	if (RedisModule_StringToDouble(radius_str, &val) == REDISMODULE_ERR)
		return -1;

	const char *str = RedisModule_StringPtrLen(unit_str, NULL);
	if (strcmp(str, "mi") == 0){
		val *= 1.60934;
		radius = val;
	}  else if (strcmp(str, "m") == 0){
		val /= 1000;
		radius = val;
	} else if (strcmp(str, "ft") == 0){
		val /= 3281;
		radius = val;
	} else if (strcmp(str, "km") == 0){
		radius = val;
	} else {
		return -1;
	}
	return 0;
}

void InsertObjectInList(RBTree *tree, long long obj_id, RBNode *x){
	int nokey;
	multimap<time_t, RBNode*> *mmap =
		(multimap<time_t, RBNode*>*)RedisModule_DictGetC(tree->obj_dict,
												 &obj_id, sizeof(long long), &nokey);
	if (nokey || mmap == NULL){
		mmap = new multimap<time_t, RBNode*>();
		RedisModule_DictReplaceC(tree->obj_dict, &obj_id, sizeof(long long), mmap);
	}

	mmap->insert({x->val.start, x});
	tree->object_count++;
 	return;
}

void RemoveObjectFromList(RBTree *tree, long long obj_id, RBNode *x){
	int nokey;
	multimap<time_t, RBNode*> *mmap =
		(multimap<time_t, RBNode*>*)RedisModule_DictGetC(
							   tree->obj_dict, &obj_id, sizeof(long long), &nokey);

	if (nokey || mmap == NULL) return;

	auto range = mmap->equal_range(x->val.start);
	for (auto iter=range.first;iter != range.second;iter++){
		if (iter->second == x){
			mmap->erase(iter->first);
			break;
		}
	}
	
	return;
}

int get_next_id(RedisModuleCtx *ctx, RedisModuleString *keystr, long long &id){
	id = RedisModule_Milliseconds() << 32;

	id |= (int64_t)(rand() & 0xffff0000);

	string key = RedisModule_StringPtrLen(keystr, NULL);
	key += ":counter";

	RedisModuleCallReply *reply = RedisModule_Call(ctx, "INCRBY", "cl", key.c_str(), 1);
	if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER){
		return REDISMODULE_ERR;
	}

	id |= (0x0000ffffULL & RedisModule_CallReplyInteger(reply));
	RedisModule_FreeCallReply(reply);
	return REDISMODULE_OK;
}

/* retrieve a descr field stored in keystr+id hash redis datatype */
RedisModuleString* GetDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_READ);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return NULL;
	}

	RedisModuleString *descr = NULL;
	RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS, descr_field, &descr, NULL);
	RedisModule_CloseKey(key);
	return descr;
}

/* set a descr field for a keystr+id redis hash data type */ 
void SetDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id, RedisModuleString *descr){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS|REDISMODULE_HASH_NX, descr_field, descr, NULL);
	RedisModule_CloseKey(key);
}

void DeleteDescriptionField(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_HashSet(key, REDISMODULE_HASH_CFIELDS, descr_field, REDISMODULE_HASH_DELETE, NULL);
	RedisModule_CloseKey(key);
}

void DeleteDescriptionKey(RedisModuleCtx *ctx, RedisModuleString *keystr, long long id){
	string idstr = RedisModule_StringPtrLen(keystr, NULL);
	idstr += ":" + to_string(id);

	RedisModuleString *keyidstr = RedisModule_CreateString(ctx, idstr.c_str(), idstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keyidstr, REDISMODULE_WRITE);
	if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return;
	}

	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
}

void DeleteCounterKey(RedisModuleCtx *ctx, RedisModuleString *keystr){
	string counterstr = RedisModule_StringPtrLen(keystr, NULL);
	counterstr += ":counter";

	RedisModuleString *keycounterstr = RedisModule_CreateString(ctx, counterstr.c_str(), counterstr.length());
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keycounterstr, REDISMODULE_WRITE);
	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
	return;
}

void DeleteKey(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, keystr, REDISMODULE_WRITE);
	RedisModule_DeleteKey(key);
	RedisModule_CloseKey(key);
	return;
}

/*================== Get RBTree with key =================================  */

/* Return the native data type. NULL if does not yet exist, throw -1 exception
   if the key exists but for a different type. */
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

/* Create a new data type, throw -1 exception if already exists for a different type */
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
		tree->obj_dict = RedisModule_CreateDict(NULL);
		RedisModule_ModuleTypeSetValue(key, RBTreeType, tree);
	} else {
		tree = (RBTree*)RedisModule_ModuleTypeGetValue(key);
	}

	RedisModule_CloseKey(key);
	return tree;
}

/* ===============  module tree manipulation functions ==================*/

RBNode* delete_min(RedisModuleCtx *ctx, RBTree *tree, RBNode *node){
	if (node->left == NULL) {
		RedisModule_DictDelC(tree->dict, &(node->val.id), sizeof(long long), NULL);
		if (node->val.obj_id > 0) RemoveObjectFromList(tree, node->val.obj_id, node);
		delete node;
		return NULL;
	}
	if (!IsRed(node->left) && !IsRed(node->left->left))
		node = MoveRedLeft(node);
	node->left = delete_min(ctx, tree, node->left);
	return balance(node);
}

void RBTreeDeleteMin(RedisModuleCtx *ctx, RBTree *tree){
	if (tree == NULL || tree->root == NULL)
		return;
	if (!IsRed(tree->root->left) && !IsRed(tree->root->right))
		tree->root->red = true;

	tree->root = delete_min(ctx, tree, tree->root);
	
	if (tree->root != NULL)
		tree->root->red = false;
}

RBNode* delete_key(RedisModuleCtx *ctx, RBTree *tree, RBNode *node, Seqn s, long long id, int level = 0){
	if (node == NULL) return NULL;

	if (cmpseqnums(s, node->s) < 0 || (cmpseqnums(s, node->s) == 0 && id != node->val.id)){
		if (node->left != NULL && !IsRed(node->left) && !IsRed(node->left->left)){
			node = MoveRedLeft(node);
		}
		node->left = delete_key(ctx, tree, node->left, s, id, level+1);
	} else {
		if (IsRed(node->left)) node = RotateRight(node);
			
		if (cmpseqnums(s, node->s) == 0 && id == node->val.id && node->right == NULL){
			if (node->val.obj_id > 0) RemoveObjectFromList(tree, node->val.obj_id, node);
			RedisModule_DictDelC(tree->dict, &id, sizeof(long long), NULL);
		    delete node;
			return NULL;
		}

		if (node->right != NULL && !IsRed(node->right) && !IsRed(node->right->left))
			node = MoveRedRight(node);

		if (cmpseqnums(s, node->s) == 0){
			if (id == node->val.id){
				RBNode *xnode = minimum(node->right);

				if (node->val.obj_id > 0) RemoveObjectFromList(tree, node->val.obj_id, node);

				node->s = xnode->s;
				node->val = xnode->val;
				node->maxseqn = xnode->maxseqn;
				node->right = delete_min(ctx, tree, node->right);
				if (node->right) node->maxseqn = node->right->maxseqn;

				if (node->val.obj_id > 0) InsertObjectInList(tree, node->val.obj_id, node);
				node->count = 1 + GetNodeCount(node->left) + GetNodeCount(node->right);

				RedisModule_DictDelC(tree->dict, &id, sizeof(long long), NULL);
				RedisModule_DictReplaceC(tree->dict, &(node->val.id), sizeof(long long), node);
			}
		} else if (cmpseqnums(s, node->maxseqn) <= 0){
			node->right = delete_key(ctx, tree, node->right, s, id, level+1);
		}
	}
	node->count = 1 + GetNodeCount(node->left) + GetNodeCount(node->right);
	return balance(node);
}

int RBTreeDelete(RedisModuleCtx *ctx, RBTree *tree, Seqn s, long long eventid){
	if (tree == NULL || tree->root == NULL)	return -1;

	if (!IsRed(tree->root->left) && !IsRed(tree->root->right))
		tree->root->red = true;

	tree->root = delete_key(ctx, tree, tree->root, s, eventid);
	
	if (tree->root != NULL)	tree->root->red = false;

	return 0;
}

RBNode* rbtree_insert(RBTree *tree, RBNode *node, Seqn s, Value val, int level=0){
	if (node == NULL){
		RBNode *x = new RBNode();
		x->s = s;
		x->maxseqn = s;
		x->val = val;
		x->red = true;
		x->count = 1;
		x->left = x->right = NULL;
		if (x->val.obj_id > 0) InsertObjectInList(tree, x->val.obj_id, x);
		RedisModule_DictSetC(tree->dict, &(x->val.id), sizeof(long long), x);
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
void print_tree(RedisModuleCtx *ctx, RedisModuleString *keystr, RBNode *node, int level = 0){
	if (node == NULL) return;

	// print left 
	print_tree(ctx, keystr, node->left, level+1);

	// print node 
	double x = node->val.x;
	double y = node->val.y;

	char scratch[64];
	strftime(scratch, 64, datetime_fmt, gmtime(&(node->val.start)));

	RedisModuleString *node_descr = GetDescriptionField(ctx, keystr, node->val.id);
	
	RedisModule_Log(ctx, "info", "print (level %d) %s %.6f/%.6f id=%lld %s red=%d cat=%ld",
					level, node_descr, x, y, node->val.id, scratch, node->red, node->val.cat);
	RedisModule_FreeString(ctx, node_descr);
	
	// print right 
	print_tree(ctx, keystr, node->right, level+1);
}

long long RBTreePrint(RedisModuleCtx *ctx, RedisModuleString *keystr){
	RBTree *tree = GetRBTree(ctx, keystr);
	if (tree == NULL || tree->root == NULL){
		RedisModule_Log(ctx, "info", "%s is empty", RedisModule_StringPtrLen(keystr, NULL));
		return 0;
	}

	RedisModule_Log(ctx, "info", "print tree: %s", RedisModule_StringPtrLen(keystr, NULL));
	print_tree(ctx, keystr, tree->root);
	return tree->root->count;
}

/* non-recursive implementation */
int RBTreeQuery(RBTree *tree, const QueryRegion qr, vector<Result> &results){
	Region r;
	cast_query_region(qr, r);

	Seqn prev, next;
	if (!next_match(r, prev, next)) return 0;
		
	stack<NodeSt> s;
	if (tree->root) s.push({tree->root, 0});

	while (!s.empty()){
		if (s.top().visited == 0){ /* if needed, visit left */
			// visit left
			s.top().visited++;
			if (s.top().node->left && cmpseqnums(next, s.top().node->s) <= 0){
				s.push({s.top().node->left, 0});
			}
		} else if (s.top().visited == 1){ /* check node.  if needed, visit right */
			s.top().visited++;

			next = s.top().node->s;
			Seqn prev = next;
			if (contains(qr, s.top().node->val)){
				results.push_back({s.top().node->s, s.top().node->val});
			} else if (!next_match(r, prev, next))
				break;

			if (s.top().node->right && cmpseqnums(next, s.top().node->maxseqn) <= 0){
				s.push({s.top().node->right, 0});
			}
		} else { /* both left right visited */
			s.pop();
		}
	}
	
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
	tree->obj_dict = RedisModule_CreateDict(NULL);
	tree->object_count = 0;
	
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
		val.obj_id = RedisModule_LoadSigned(rdb);
		val.cat = RedisModule_LoadUnsigned(rdb);
		val.start = static_cast<time_t>(RedisModule_LoadSigned(rdb));
		val.end = static_cast<time_t>(RedisModule_LoadSigned(rdb));

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
		RedisModule_SaveSigned(rdb, node->val.obj_id);
		RedisModule_SaveUnsigned(rdb, node->val.cat);
		
		// time start/end
		RedisModule_SaveSigned(rdb, node->val.start);
		RedisModule_SaveSigned(rdb, node->val.end);
	}
	RedisModule_DictIteratorStop(iter);
}

extern "C" void RBTreeTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value){
	RBTree *tree = (RBTree*)value;

	uint64_t sz = RedisModule_DictSize(tree->dict);
	RedisModule_LogIOError(aof, "debug", "AofRewrite: %llu nodes", sz);

	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tree->dict, "^", NULL, 0);
	
	char s1[16];
	char s2[16];
	char t1[16];
	char t2[16];
	char px[16];
	char py[16];
	unsigned char *dict_key = NULL;
	RBNode *node = NULL;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, NULL, (void**)&node)) != NULL){
		strftime(s1, 16, "%m-%d-%Y", gmtime(&(node->val.start)));
		strftime(s2, 16, "%m-%d-%Y", gmtime(&(node->val.end)));
		strftime(t1, 16, "%H:%M", gmtime(&(node->val.start)));
		strftime(t2, 16, "%H:%M", gmtime(&(node->val.end)));
		snprintf(px, 16, "%f", node->val.x);
		snprintf(py, 16, "%f", node->val.y);

		
		if (node->val.obj_id == 0){
			RedisModule_EmitAOF(aof, "reventis.insertrepl", "sccccccl",
								key, px, py, s1, t1, s2, t2, node->val.id);
		} else {
			RedisModule_EmitAOF(aof, "reventis.updaterepl", "sccccll",
								key, px, py, s1, t1, node->val.obj_id, node->val.id);
		}

		unsigned long long cat_id = 0x0001ULL;
		long long pos = 1;
	  
		while (cat_id){
			if (cat_id & node->val.cat){
				RedisModule_EmitAOF(aof, "reventis.addcategory", "sl", key, pos);
			}
			cat_id <<= 1;
			pos++;
		}
	}
	RedisModule_DictIteratorStop(iter);
}

extern "C" void RBTreeTypeFree(void *value){
	RBTree *tree = (RBTree*)value;
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tree->dict, "^", NULL, 0);
	long long *dict_key = NULL;
	size_t keylen;
	RBNode *node = NULL;
	while ((dict_key = (long long*)RedisModule_DictNextC(iter, &keylen, (void**)&node)) != NULL){
		delete node;
	}
	RedisModule_DictIteratorStop(iter);

	iter = RedisModule_DictIteratorStartC(tree->obj_dict, "^", NULL, 0);
	multimap<time_t, RBNode*> *mm = NULL;
	while ((dict_key = (long long*)RedisModule_DictNextC(iter, &keylen, (void**)&mm)) != NULL){
		delete mm;
	}
	RedisModule_DictIteratorStop(iter);
	
	RedisModule_FreeDict(NULL, tree->dict);
	RedisModule_FreeDict(NULL, tree->obj_dict);
}

extern "C" size_t RBTreeTypeMemUsage(const void *value){
	RBTree *tree = (RBTree*)value;
	uint64_t n_nodes = RedisModule_DictSize(tree->dict);

	uint64_t n_objects = 0;
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tree->obj_dict, "^", NULL, 0);
	unsigned char *dict_key = NULL;
	size_t keylen;
	multimap<time_t, RBNode*> *mmap;
	while ((dict_key = (unsigned char*)RedisModule_DictNextC(iter, &keylen, (void**)&mmap)) != NULL){
		n_objects += mmap->size();
	}
	RedisModule_DictIteratorStop(iter);

	return n_nodes*sizeof(RBNode) + n_objects*(sizeof(RBNode*) + sizeof(long long));
}

extern "C" void RBTreeTypeDigest(RedisModuleDigest *digest, void *value){
	REDISMODULE_NOT_USED(digest);
	REDISMODULE_NOT_USED(value);
}

/* ---------------RedisCommand functions ------------------------------*/
int RBTreeQueryResults(RedisModuleCtx *ctx, RedisModuleString *key,
					   RedisModuleString *x1str, RedisModuleString *x2str,
					   RedisModuleString *y1str, RedisModuleString *y2str,
					   RedisModuleString *startdatestr, RedisModuleString *starttimestr,
					   RedisModuleString *enddatestr, RedisModuleString *endtimestr,
					   vector<Result> &results){
	double x1, y1;
	if (ParseLongLat(x1str, y1str, x1, y1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse lower longitude/latitude arg values");
		return REDISMODULE_ERR;
	}

	double x2, y2;
	if (ParseLongLat(x2str, y2str, x2, y2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse upper longitude/latitude arg values");
		return REDISMODULE_ERR;
	}

	if (x1 < -180.0 || x1 > 180.0 || x2 < -180.0 || x2 > 180.0){
		RedisModule_ReplyWithError(ctx, "ERR - longitude/latitude arg values out of range");
		return REDISMODULE_ERR;
	}

	time_t t1, t2;
	if (ParseDateTime(startdatestr, starttimestr, t1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse lower date time arg values");
		return REDISMODULE_ERR;
	}

	if (ParseDateTime(enddatestr, endtimestr, t2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse upper date time arg values");
		return REDISMODULE_ERR;
	}

	if (t1 > t2){
		RedisModule_ReplyWithError(ctx, "ERR - start time cannot be later than end time");
		return REDISMODULE_ERR;
	}

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, key);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - No such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key already exists for different type");
		return REDISMODULE_ERR;
	}
	
	QueryRegion qr;
	qr.x_lower = x1;
	qr.x_upper = x2;
	qr.y_lower = y1;
	qr.y_upper = y2;
	qr.t_lower = t1;
	qr.t_upper = t2;

	if (RBTreeQuery(tree, qr, results) < 0){
		RedisModule_ReplyWithError(ctx, "Err - Unable to query");
		return REDISMODULE_ERR;
	}

	return REDISMODULE_OK;
}

int RBTreeInsertCommon(RedisModuleCtx *ctx, RedisModuleString *keystr,
					   RedisModuleString *longitudestr, RedisModuleString *latitudestr,
					   RedisModuleString *startdatestr, RedisModuleString *starttimestr,
					   RedisModuleString *enddatestr, RedisModuleString *endtimestr,
					   unsigned long long cat_id, long long event_id){

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
	pnt.arr[0] = static_cast<uint64_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[1] = static_cast<uint64_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[2] = static_cast<uint64_t>(t1);

	Value val;
	val.x = x;
	val.y = y;
	val.cat = cat_id;
	val.id = event_id;
	val.start = t1;
	val.end = t2;
	
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

	return REDISMODULE_OK;
}

/* args: key longitude latitude date-start time-start date-end time-end [id] */
extern "C" int RBTreeInsertRepl_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 8 || argc > 9) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	long long event_id;
	if (argc == 8){
		if (get_next_id(ctx, argv[1], event_id) == REDISMODULE_ERR){
			RedisModule_ReplyWithError(ctx, "ERR - unable to assign event id");
			return REDISMODULE_ERR;
		}
	} else {
		if (RedisModule_StringToLongLong(argv[9], &event_id) == REDISMODULE_ERR){
			RedisModule_ReplyWithError(ctx, "ERR - unable to parse event id");
			return REDISMODULE_ERR;
		}
	}

	int rc = RBTreeInsertCommon(ctx, argv[1], argv[2], argv[3], argv[4], argv[5],
								argv[6], argv[7], 0, event_id);
	if (rc == REDISMODULE_ERR) return REDISMODULE_ERR;


	rc = RedisModule_Replicate(ctx, "reventis.insertrepl", "sssssssl",
							   argv[1], argv[2], argv[3], argv[4], argv[5],
							   argv[6], argv[7], event_id);
	if (rc == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "Unable to replicate for id %lld", event_id);
		return REDISMODULE_ERR;
	}
	
	return REDISMODULE_OK;
}

/* args: key longitude latitude date-start time-start date-end time-end title-description [id]*/
/* an optional event_id argument is included for replication commands */
/* return event-id assigned to entry */
extern "C" int RBTreeInsert_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 9 || argc > 10) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	long long event_id;
	if (argc == 9){
		if (get_next_id(ctx, argv[1], event_id) == REDISMODULE_ERR){
			RedisModule_ReplyWithError(ctx, "ERR - unable to assign event id");
			return REDISMODULE_ERR;
		}
	} else {
		if (RedisModule_StringToLongLong(argv[9], &event_id) == REDISMODULE_ERR){
			RedisModule_ReplyWithError(ctx, "ERR - unable to parse event id");
			return REDISMODULE_ERR;
		}
	}
	
	int rc = RBTreeInsertCommon(ctx, argv[1], argv[2], argv[3], argv[4], argv[5],
					   argv[6], argv[7], 0, event_id);
	if (rc == REDISMODULE_ERR) return REDISMODULE_ERR;

	SetDescriptionField(ctx, argv[1], event_id, argv[8]);

	/* replicate command with eventid tacked on as last argument */
	rc = RedisModule_Replicate(ctx, "reventis.insert", "ssssssssl",
							   argv[1], argv[2], argv[3], argv[4], argv[5],
							   argv[6], argv[7], argv[8], event_id);
	if (rc == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "Unable to replicate for id %ld", event_id);
		return REDISMODULE_ERR;
	}
	
	return REDISMODULE_OK;
}

/* args: mytree eventid cat_id */
/* add=true to add a category, add=false to remove category*/
int ModifyCategories(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool add=true){

	RedisModule_AutoMemory(ctx);
	RedisModuleString *keystr = argv[1];
   
	long long event_id;
	if (RedisModule_StringToLongLong(argv[2], &event_id) != REDISMODULE_OK){
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
		RedisModule_ReplyWithError(ctx, "ERR - no such node");
		return REDISMODULE_ERR;
	}

	for (int i=3;i<argc;i++){
		long long cat_id;
		if (RedisModule_StringToLongLong(argv[i], &cat_id) != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "ERR - unable to parse cat id value");
			return REDISMODULE_ERR;
		}
		if (add){
			idnode->val.cat |= 0x0001ULL << (cat_id-1);
		} else {
			idnode->val.cat &= ~0x0001ULL << (cat_id-1);
		}
	}

	RedisModule_Log(ctx, "debug", "modify category for node - %llx", idnode->val.cat);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);

	return REDISMODULE_OK;
}

/* args: mytree event_id [cat_id...}]*/
extern "C" int RBTreeAddCategory_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);
	return ModifyCategories(ctx, argv, argc, true);
}

/* args: mytree event_id [cat_id...]*/
extern "C" int RBTreeRemoveCategory_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);
	return ModifyCategories(ctx, argv, argc, false);
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
		RedisModule_ReplyWithNull(ctx);
		return REDISMODULE_OK;
	}

	char s1[64];
	char s2[64];
	strftime(s1, 64, datetime_fmt, gmtime(&(idnode->val.start)));
	strftime(s2, 64, datetime_fmt, gmtime(&(idnode->val.end)));


	RedisModuleString *descr = GetDescriptionField(ctx, keystr, event_id);
	
	RedisModule_ReplyWithArray(ctx, 7);
	RedisModule_ReplyWithString(ctx, descr);
	RedisModule_ReplyWithLongLong(ctx, idnode->val.id);
	RedisModule_ReplyWithLongLong(ctx, idnode->val.obj_id);
	RedisModule_ReplyWithDouble(ctx, idnode->val.x);
	RedisModule_ReplyWithDouble(ctx, idnode->val.y);
	RedisModule_ReplyWithStringBuffer(ctx, s1, strlen(s1)+1);
	RedisModule_ReplyWithStringBuffer(ctx, s2, strlen(s2)+1);
	
	return REDISMODULE_OK;
}

/* args: mytree eventid */
extern "C" int RBTreeDelete_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 3) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();

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

	DeleteDescriptionField(ctx, keystr, event_id);
	
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	RedisModule_ReplicateVerbatim(ctx);

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;
	RedisModule_Log(ctx, "debug", "delete eventid=%lld  in %u microseconds", event_id, dur);
	
	return REDISMODULE_OK;
}

/* purge all entries before specied datetime */
/* args: mytree date time */
/* return number of deletions */
extern "C" int RBTreePurge_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 4) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();

	RedisModuleString *keystr = argv[1];
	RedisModuleString *datestr = argv[2];
	RedisModuleString *timestr = argv[3];

	time_t t;
	if (ParseDateTime(datestr, timestr, t) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse date/time");
		return REDISMODULE_ERR;
	}

	QueryRegion qr;
	qr.x_lower = -180.0;
	qr.x_upper = 180.0;
	qr.y_lower = -90.0;
	qr.y_upper = 90.0;
	qr.t_lower = numeric_limits<time_t>::min();
	qr.t_upper = t;

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

	for (Result res : results){
		DeleteDescriptionField(ctx, keystr, res.val.id);
		if (RBTreeDelete(ctx, tree, res.s, res.val.id) < 0){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to delete");
			return REDISMODULE_OK;
		}
	}

	/* reply with number of deletes performed */
	RedisModule_ReplyWithLongLong(ctx, n_deletes);
	RedisModule_ReplicateVerbatim(ctx);

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;
	RedisModule_Log(ctx, "debug", "purge %lld nodes  in %u microseconds", n_deletes, dur);
	
	return REDISMODULE_OK;
}

/* args: mytree longitude-range latitude-range start-range end-range [cat_id]*/
extern "C" int RBTreeDelBlk_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 10) return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();

	unsigned long long cat_flag = 0ULL;
	for (int i=10;i<argc;i++){
		long long catid;
		if (RedisModule_StringToLongLong(argv[i], &catid) != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to parse category id value");
			return REDISMODULE_ERR;
		}
		if (catid < 1 || catid > 64){
			RedisModule_ReplyWithError(ctx, "ERR - category values out of range");
			return REDISMODULE_ERR;
		}
		cat_flag |= 0x0001ULL << (catid - 1);
	}
	
	vector<Result> results;
	if (RBTreeQueryResults(ctx, argv[1], argv[2], argv[3], argv[4], argv[5],
						   argv[6], argv[7], argv[8], argv[9], results) != REDISMODULE_OK){
		return REDISMODULE_ERR;
	}


    RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - no such key existts");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key exists for different type");
		return REDISMODULE_ERR;
	}

	for (Result res: results){
		DeleteDescriptionField(ctx, argv[1], res.val.id);
		if (cat_flag == 0 || (res.val.cat & cat_flag)){
			if (RBTreeDelete(ctx, tree, res.s, res.val.id) < 0){
				RedisModule_ReplyWithError(ctx, "ERR - unable to delete");
				return REDISMODULE_ERR;
			}
		}
	}

	/* reply with number of deletes performed */
	RedisModule_ReplyWithLongLong(ctx, (long long)results.size());
	RedisModule_ReplicateVerbatim(ctx);

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;
	RedisModule_Log(ctx, "debug", "delete block in %u microseconds", dur);
	
	return REDISMODULE_OK;
}


/* args: mytree longitude-range latitude-range start-range end-range [cat_id ...] */
extern "C" int RBTreeQuery_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 10) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();

	unsigned long long cat_flag = 0;
	for (int i=10;i<argc;i++){
		long long catid;
		if (RedisModule_StringToLongLong(argv[i], &catid) != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to parse category id value");
			return REDISMODULE_ERR;
		}
		if (catid < 1 || catid > 64){
			RedisModule_ReplyWithError(ctx, "ERR - category values out of range");
			return REDISMODULE_ERR;
		}
		cat_flag |= 0x0001ULL << (catid - 1);
	}

	vector<Result> results;
	if (RBTreeQueryResults(ctx, argv[1], argv[2], argv[3], argv[4], argv[5],
						   argv[6], argv[7], argv[8], argv[9], results) != REDISMODULE_OK){
		return REDISMODULE_ERR;
	}

	const char *out_fmt = "title: %s id: %lld %f/%f %s to %s";

	char s1[64];
	char s2[64];
	long long count = 0;

	/* reply with results in embedded arrays */
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	for (Result res : results){
		if (cat_flag == 0 || (res.val.cat & cat_flag)){
			if (res.val.obj_id == 0){
				RedisModuleString *descr = GetDescriptionField(ctx, argv[1], res.val.id);
				
				strftime(s1, 64, datetime_fmt, gmtime(&(res.val.start)));
				strftime(s2, 64, datetime_fmt, gmtime(&(res.val.end)));

				RedisModule_ReplyWithArray(ctx, 6);
				RedisModule_ReplyWithString(ctx, descr);
				RedisModule_ReplyWithLongLong(ctx, res.val.id);
				RedisModule_ReplyWithDouble(ctx, res.val.x);
				RedisModule_ReplyWithDouble(ctx, res.val.y);
				RedisModule_ReplyWithStringBuffer(ctx, s1, strlen(s1)+1);
				RedisModule_ReplyWithStringBuffer(ctx, s2, strlen(s2)+1);
				count++;
			}
		}
	}
	RedisModule_ReplySetArrayLength(ctx, count);
	
	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;
	RedisModule_Log(ctx, "debug", "query time - %u microseconds", dur);
	
	return REDISMODULE_OK;
}

/* args: key longitude latitude radius unit date-start time-start date-end time-end [cat_id...] */
extern "C" int RBTreeQueryByRadius_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 10) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();

    RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL){
			RedisModule_ReplyWithError(ctx, "ERR - no such key existts");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key exists for different type");
		return REDISMODULE_ERR;
	}

	Position pt;
	if (ParseLongLat(argv[2], argv[3], pt.x, pt.y) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse lower longitude/latitude arg values");
		return REDISMODULE_ERR;
	}

	double radius;
	if (ParseRadius(argv[4], argv[5], radius) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse radius");
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

	if (t1 > t2){
		RedisModule_ReplyWithError(ctx, "ERR - start time cannot be later than end time");
		return REDISMODULE_ERR;
	}

	unsigned long long cat_flag = 0;
	for (int i=10;i<argc;i++){
		long long catid;
		if (RedisModule_StringToLongLong(argv[i], &catid) != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to parse category id value");
			return REDISMODULE_ERR;
		}
		if (catid < 1 || catid > 64){
			RedisModule_ReplyWithError(ctx, "ERR - category values out of range");
			return REDISMODULE_ERR;
		}
		cat_flag |= 0x0001ULL << (catid - 1);
	}

	
	double xdelta, ydelta;
	get_xyradius(pt, radius, xdelta, ydelta);

	QueryRegion qr;
	qr.x_lower = pt.x - xdelta;
	qr.x_upper = pt.x + xdelta;
	qr.y_lower = pt.y - ydelta;
	qr.y_upper = pt.y + ydelta;
	qr.t_lower = t1;
	qr.t_upper = t2;

	vector<Result> results;
	if (RBTreeQuery(tree, qr, results) < 0){
		RedisModule_ReplyWithError(ctx, "Err - Unable to query");
		return REDISMODULE_ERR;
	}

	const char *out_fmt = "title: %s id: %lld %f/%f %s to %s";

	char s1[64];
	char s2[64];
	long long count = 0;

	/* reply with results in embedded arrays */
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	for (Result res : results){
		if (cat_flag == 0 || (res.val.cat & cat_flag)){
			if (res.val.obj_id == 0){

				RedisModuleString *descr = GetDescriptionField(ctx, argv[1], res.val.id);
				
				strftime(s1, 64, datetime_fmt, gmtime(&(res.val.start)));
				strftime(s2, 64, datetime_fmt, gmtime(&(res.val.end)));

				RedisModule_ReplyWithArray(ctx, 6);
				RedisModule_ReplyWithString(ctx, descr);
				RedisModule_ReplyWithLongLong(ctx, res.val.id);
				RedisModule_ReplyWithDouble(ctx, res.val.x);
				RedisModule_ReplyWithDouble(ctx, res.val.y);
				RedisModule_ReplyWithStringBuffer(ctx, s1, strlen(s1)+1);
				RedisModule_ReplyWithStringBuffer(ctx, s2, strlen(s2)+1);
				count++;
			}
		}
	}
	RedisModule_ReplySetArrayLength(ctx, count);
	return REDISMODULE_OK;
}

/* args: mytree */
/* print tree to log file */
/* return a count for number of nodes */
extern "C" int RBTreePrint_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);
	RedisModuleString *keystr = argv[1];
	long long count = RBTreePrint(ctx, keystr);
	RedisModule_ReplyWithLongLong(ctx, count);
	return REDISMODULE_OK;
}

extern "C" int RBTreeSize_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL) {
			RedisModule_ReplyWithError(ctx, "no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "key already exists for different type");
		return REDISMODULE_ERR;
	}

	RedisModule_ReplyWithLongLong(ctx, tree->root->count);
	return REDISMODULE_OK;
}

/* args: key */
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


/* args: key longitude latitude date time object_id [id] */
extern "C" int RBTreeUpdateRepl_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 7 || argc > 8) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	long long object_id;
	if (RedisModule_StringToLongLong(argv[6], &object_id) != REDISMODULE_OK || object_id <= 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse object id");
		return REDISMODULE_ERR;
	}

	long long event_id;
	if (argc == 7){ // assign id number for new entry
		if (get_next_id(ctx, argv[1], event_id) == REDISMODULE_ERR) {
			RedisModule_ReplyWithError(ctx, "ERR - Unable to get next id");
			return REDISMODULE_ERR;
		}
	} else {
		if (RedisModule_StringToLongLong(argv[7], &event_id) != REDISMODULE_OK){
			RedisModule_ReplyWithError(ctx, "ERR - Unable to parse Id arg");
			return REDISMODULE_ERR;
		}
	}

	double x, y;
	if (ParseLongLat(argv[2], argv[3], x, y) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - bad longitude/latitude arg values");
		return REDISMODULE_ERR;
	}

	if (x < -180.0 || x > 180.0 || y < -90.0 || y > 90.0){
		RedisModule_ReplyWithError(ctx, "ERR - longitude/latitude args out of range");
		return REDISMODULE_ERR;
	}
	
	time_t t1;
	if (ParseDateTime(argv[4], argv[5], t1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse start time");
		return REDISMODULE_ERR;
	}


	Point pnt;
	pnt.arr[0] = static_cast<uint64_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[1] = static_cast<uint64_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[2] = static_cast<uint64_t>(t1);

	Value val;
	val.x = x;
	val.y = y;
	val.id = event_id;
	val.obj_id = object_id;
	val.start = t1;
	val.end = t1;
	
	Seqn s;
	spfc_encode(pnt, s);

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL) tree = CreateRBTree(ctx, argv[1]);
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type. Delete first.");
		return REDISMODULE_ERR;
	}
	
	if (RBTreeInsert(tree, s, val) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Insert failed");
		return REDISMODULE_ERR;
	}

	RedisModule_ReplyWithLongLong(ctx, event_id);

	if (RedisModule_Replicate(ctx, "reventis.updaterepl", "ssssssl",
							  argv[1], argv[2], argv[3], argv[4],
							  argv[5], argv[6], event_id) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "Unable to replicate for id %ld", event_id);
		return REDISMODULE_ERR;
	}
	
	return REDISMODULE_OK;
}


/* args: key longitude latitude date time object_id descr [id] */
extern "C" int RBTreeUpdate_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 8 || argc > 9) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	RedisModuleString *keystr = argv[1];
	RedisModuleString *longitudestr = argv[2];
	RedisModuleString *latitudestr = argv[3];
	RedisModuleString *datestr = argv[4];
	RedisModuleString *timestr = argv[5];
	RedisModuleString *objidstr = argv[6];
	RedisModuleString *descr = argv[7];
	long long object_id, event_id;

	if (RedisModule_StringToLongLong(objidstr, &object_id) != REDISMODULE_OK || object_id <= 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse object id");
		return REDISMODULE_ERR;
	}
	
	if (argc < 9){ // assign id number for new entry
		if (get_next_id(ctx, argv[1], event_id) == REDISMODULE_ERR) {
			RedisModule_ReplyWithError(ctx, "ERR - Unable to get next id");
			return REDISMODULE_ERR;
		}
	} else {
		if (RedisModule_StringToLongLong(argv[8], &event_id) != REDISMODULE_OK){
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
	if (ParseDateTime(datestr, timestr, t1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR - Unable to parse start time");
		return REDISMODULE_ERR;
	}

	Point pnt;
	pnt.arr[0] = static_cast<uint64_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[1] = static_cast<uint64_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[2] = static_cast<uint64_t>(t1);

	Value val;
	val.x = x;
	val.y = y;
	val.id = event_id;
	val.obj_id = object_id;
	val.start = t1;
	val.end = t1;
	
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

	SetDescriptionField(ctx, keystr, event_id, descr);
	
	/* returned assigned event_id to node  */
	RedisModule_ReplyWithLongLong(ctx, event_id);

	/* replicate command with eventid tacked on as last argument */
	if (RedisModule_Replicate(ctx, "reventis.update", "sss",
							  keystr, longitudestr, latitudestr,
							  datestr, timestr, objidstr, descr,
							  event_id) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "warning", "Unable to replicate for id %ld", event_id);
		return REDISMODULE_ERR;
	}

	return REDISMODULE_OK;
}

/* args: key longitude-range latitude-range time-range */
extern "C" int RBTreeQueryObjects_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 10) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();

	vector<Result> results;
	if (RBTreeQueryResults(ctx, argv[1], argv[2], argv[3], argv[4], argv[5],
						   argv[6], argv[7], argv[8], argv[9], results) != REDISMODULE_OK){
		return REDISMODULE_ERR;
	}

	const char *out_fmt = "%s id=%lld obj=%lld long.=%f lat.=%f time=%s";
	char s1[32];

	/* return results in embedded arrays */
	long count = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
	for (Result res: results){
		if (res.val.obj_id > 0) {
			RedisModuleString *descr = GetDescriptionField(ctx, argv[1], res.val.id);

			strftime(s1, 32, datetime_fmt, gmtime(&(res.val.start)));

			RedisModule_ReplyWithArray(ctx, 6);
			RedisModule_ReplyWithString(ctx, descr);
			RedisModule_ReplyWithLongLong(ctx, res.val.id);
			RedisModule_ReplyWithLongLong(ctx, res.val.obj_id);
			RedisModule_ReplyWithDouble(ctx, res.val.x);
			RedisModule_ReplyWithDouble(ctx, res.val.y);
			RedisModule_ReplyWithStringBuffer(ctx, s1, strlen(s1)+1);
		
			RedisModuleString *resp = RedisModule_CreateStringPrintf(ctx, out_fmt,
									   RedisModule_StringPtrLen(descr, NULL),
									   res.val.id, res.val.obj_id,
								       res.val.x, res.val.y, s1);
			RedisModule_ReplyWithString(ctx, resp);
			count++;
		}
	}
	RedisModule_ReplySetArrayLength(ctx, count);
	
	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;
	RedisModule_Log(ctx, "debug", "query objects in  %u microseconds", dur);

	return REDISMODULE_OK;
}

/* args: key longitude-range latitude-range time-range */
extern "C" int RBTreeTrackAll_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 10) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();

	vector<Result> results;
	if (RBTreeQueryResults(ctx, argv[1], argv[2], argv[3], argv[4], argv[5],
						   argv[6], argv[7], argv[8], argv[9], results) != REDISMODULE_OK){
		return REDISMODULE_ERR;
	}

	set<long long> objects;
	for (Result res : results){
		if (res.val.obj_id > 0)
			objects.insert(res.val.obj_id);
	}

	/* return array of object_id's found */
	RedisModule_ReplyWithArray(ctx, objects.size());
	for (auto iter=objects.begin();iter != objects.end();iter++){
		RedisModule_ReplyWithLongLong(ctx, *iter);
	}

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;
	RedisModule_Log(ctx, "debug", "trackall time - %u microseconds", dur);

	return REDISMODULE_OK;
}

/* args: key object_id datetime-start datetime-end */
extern "C" int RBTreeHist_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 3) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();
	
	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type. Delete first.");
		return REDISMODULE_ERR;
	}

	long long object_id;
	if (RedisModule_StringToLongLong(argv[2], &object_id) || object_id <= 0){
		RedisModule_ReplyWithError(ctx, "ERR - unable to parse object_id arg");
		return REDISMODULE_ERR;
	}

	time_t t1, t2;
	t1 = numeric_limits<time_t>::min();
	t2 = numeric_limits<time_t>::max();
	if (argc >= 7){
		if (ParseDateTime(argv[3], argv[4], t1) < 0){
			RedisModule_ReplyWithError(ctx, "ERR - unable to parse beginning time");
			return REDISMODULE_ERR;
		}
		if (ParseDateTime(argv[5], argv[6], t2) < 0){
			RedisModule_ReplyWithError(ctx, "ERR - unable to parse end time");
			REDISMODULE_ERR;
		}
	}

	RedisModule_Log(ctx, "debug", "hist for objid=%lld from %lld to %lld", object_id, t1, t2);
	
	int nokey;
	multimap<time_t, RBNode*> *nodes =
		(multimap<time_t, RBNode*>*)RedisModule_DictGetC(tree->obj_dict, &object_id,
																	sizeof(long long), &nokey);
	if (nokey || nodes == NULL){
		RedisModule_ReplyWithError(ctx, "ERR - no such object_id found");
		return REDISMODULE_ERR;
	}

	const char *out_fmt = "%s id=%lld obj=%lld long.=%f lat.=%f time=%s";
	char s1[32];

	/* return results in embedded arrays */
	long long count = 0;
	RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

	auto iter = nodes->lower_bound(t1);

	while (iter != nodes->end()){
		RBNode *n = iter->second;
		if (n->val.start > t2) break;

		RedisModuleString *descr = GetDescriptionField(ctx, argv[1], n->val.id);
		strftime(s1, 32, datetime_fmt, gmtime(&(n->val.start)));

		RedisModule_ReplyWithArray(ctx, 6);
		RedisModule_ReplyWithString(ctx, descr);
		RedisModule_ReplyWithLongLong(ctx, n->val.id);
		RedisModule_ReplyWithLongLong(ctx, n->val.obj_id);
		RedisModule_ReplyWithDouble(ctx, n->val.x);
		RedisModule_ReplyWithDouble(ctx, n->val.y);
		RedisModule_ReplyWithStringBuffer(ctx, s1, strlen(s1)+1);

		iter++;
		count++;
	}
	RedisModule_ReplySetArrayLength(ctx, count);

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;

	RedisModule_Log(ctx, "debug", "hist obj_id=%lld in %lu microseconds", object_id, elapsed);
	
	return REDISMODULE_OK;
}

/* args: key object_id */
extern "C" int RBTreeDelObj_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 3) return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	chrono::time_point<chrono::high_resolution_clock> start = chrono::high_resolution_clock::now();
	
	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type. Delete first.");
		return REDISMODULE_ERR;
	}

	long long object_id;
	if (RedisModule_StringToLongLong(argv[2], &object_id) || object_id <= 0){
		RedisModule_ReplyWithError(ctx, "ERR - unable to parse object_id arg");
		return REDISMODULE_ERR;
	}

	int nokey;
	multimap<time_t, RBNode*> *nodes = NULL;
	RedisModule_DictDelC(tree->obj_dict, &object_id, sizeof(long long), &nodes);
	if (nodes == NULL){
		RedisModule_ReplyWithError(ctx, "ERR - no such object_id found");
		return REDISMODULE_ERR;
	}

	vector<Result> results;
	for (auto iter=nodes->begin();iter != nodes->end();iter++){
		RBNode *n = iter->second;
		results.push_back({n->s, n->val});
	}

	RedisModule_Log(ctx, "debug", "delobj %lld - count = %ld", object_id, results.size());
	long long count = 0;
	for (Result res : results){
		printf("delete %ld\n", res.val.id);
		RedisModuleString *descr = GetDescriptionField(ctx, argv[1], res.val.id);
		
		RedisModule_Log(ctx, "debug", "del %lld %s",
						res.val.id, RedisModule_StringPtrLen(descr, NULL));

		DeleteDescriptionField(ctx, argv[1], res.val.id);
		if (RBTreeDelete(ctx, tree, res.s, res.val.id) < 0){
			printf("ERR - unable to delete %ld\n", res.val.id);
			RedisModule_ReplyWithError(ctx, "ERR - unable to delete");
			return REDISMODULE_ERR;
		}
		count++;
	}

	/* return number of deletes performed */
	RedisModule_ReplyWithLongLong(ctx, count);
	RedisModule_ReplicateVerbatim(ctx);

	chrono::time_point<chrono::high_resolution_clock> end = chrono::high_resolution_clock::now();
	auto elapsed = chrono::duration_cast<chrono::microseconds>(end - start).count();
	unsigned int dur = elapsed;

	RedisModule_Log(ctx, "debug", "delete object %ld in %lu microseconds", object_id, elapsed);
	
	return REDISMODULE_OK;
}

/* args: key */
extern "C" int RBTreeDeleteKey_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2) return RedisModule_WrongArity(ctx);

	RBTree *tree = NULL;
	try {
		tree = GetRBTree(ctx, argv[1]);
		if (tree == NULL) {
			RedisModule_ReplyWithError(ctx, "ERR - no such key");
			return REDISMODULE_ERR;
		}
	} catch (int &e){
		RedisModule_ReplyWithError(ctx, "ERR - key exists for different type. Delete first.");
		return REDISMODULE_ERR;
	}

	// delete id fields containing description fields
	RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(tree->dict, "^", NULL, 0);
	long long *id_key = NULL;
	size_t keylen;
	RBNode *node = NULL;
	while ((id_key = (long long*)RedisModule_DictNextC(iter, &keylen, (void**)&node)) != NULL){
		DeleteDescriptionKey(ctx, argv[1], *id_key);
	}
	RedisModule_DictIteratorStop(iter);

	DeleteCounterKey(ctx, argv[1]);
	DeleteKey(ctx, argv[1]);
	
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	
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
	if (RBTreeType == NULL)	return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.delkey", RBTreeDeleteKey_RedisCmd,
								  "write", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.insertrepl", RBTreeInsertRepl_RedisCmd,
								  "write deny-oom fast", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.insert",  RBTreeInsert_RedisCmd,
								  "write deny-oom fast", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.addcategory", RBTreeAddCategory_RedisCmd,
								  "write fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.remcategory", RBTreeRemoveCategory_RedisCmd,
								  "write fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.lookup", RBTreeLookup_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR);
	
	if (RedisModule_CreateCommand(ctx, "reventis.del", RBTreeDelete_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.purge", RBTreePurge_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.delblk", RBTreeDelBlk_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.query", RBTreeQuery_RedisCmd,
								  "readonly", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.querybyradius", RBTreeQueryByRadius_RedisCmd,
								  "readonly", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.print", RBTreePrint_RedisCmd,
								  "readonly", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.size", RBTreeSize_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.depth", RBTreeDepth_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	/* ------------- object-tracking commands --------------------------*/

	if (RedisModule_CreateCommand(ctx, "reventis.updaterepl", RBTreeUpdate_RedisCmd,
								  "write deny-oom fast", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.update", RBTreeUpdate_RedisCmd,
								  "write deny-oom fast", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.queryobj", RBTreeQueryObjects_RedisCmd,
								  "readonly", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.trackall", RBTreeTrackAll_RedisCmd,
								  "readonly", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.hist", RBTreeHist_RedisCmd,
								  "readonly fast", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.delobj", RBTreeDelObj_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	return REDISMODULE_OK;
}
