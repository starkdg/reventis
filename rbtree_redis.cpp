#include "redismodule.h"
#include "defs.h"

using namespace std;

/* struct to return results for range query */
typedef struct value_t {
	double longitude, latitude;
	long long id;
	Interval dur;
	RedisModuleString *descr;
} Value;

/* struct to return query results for deletion */
typedef struct result_t {
	Seqn key;
	long long id;
} Result;

/* parse longitude latitude values - x for longitude, y for latitude*/
/* return -1 on error */
int ParseLongLat(RedisModuleString *xstr, RedisModuleString *ystr, double &x, double &y){
	if (RedisModule_StringToDouble(xstr, &x) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	if (RedisModule_StringToDouble(ystr, &y) == REDISMODULE_ERR)
		return -1;
	return 0;
}

/* parse date time strings into epoch time*/
/* return -1 on error */
int ParseDatetime(RedisModuleString *datestr, RedisModuleString *timestr, time_t &epoch){
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

/* Aux. function to set root node in redis table */
int SetRoot(RedisModuleCtx *ctx, RedisModuleString *tree, RedisModuleString *node){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, tree, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_STRING && keytype != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return -1;
	}

	if (node == NULL)
		RedisModule_UnlinkKey(key);
	else
		RedisModule_StringSet(key, node);
		
	RedisModule_CloseKey(key);
	return 0;
}

/* get root node represented as string */
RedisModuleString* GetRoot(RedisModuleCtx *ctx, RedisModuleString *tree){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, tree, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_STRING){
		RedisModule_CloseKey(key);
		return NULL;
	}

	size_t len;
	char *ptr = RedisModule_StringDMA(key, &len, REDISMODULE_READ);
	if (ptr == NULL){
		RedisModule_CloseKey(key);
		return NULL;
	}
	
	RedisModuleString *root = RedisModule_CreateString(ctx, ptr, len);
	RedisModule_CloseKey(key);
	return root;
}

/* node count is number of nodes at and beneath a node */
int GetNodeCount(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	if (nodelink == NULL) return 0;
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return 0;
	}

	RedisModuleString *countstr;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							count_field_name, &countstr, NULL) != REDISMODULE_OK){
		RedisModule_CloseKey(linkkey);
		return 0;
	}
	
	long long count;
	if (RedisModule_StringToLongLong(countstr, &count) != REDISMODULE_OK){
		RedisModule_CloseKey(linkkey);
		return 0;
	}

	RedisModule_CloseKey(linkkey);
	return (int)count;
}

int SetNodeCount(RedisModuleCtx *ctx, RedisModuleString *nodelink, int count){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *countstr = RedisModule_CreateStringFromLongLong(ctx, (long long)count);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
							count_field_name, countstr, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	
	RedisModule_CloseKey(linkkey);
	return 0;
}

bool IsRedNode(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	if (nodelink == NULL)
		return false;
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return false;
	}

	bool red = false;
	RedisModuleString *colorstr;
	long long color;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							color_field_name, &colorstr, NULL) == REDISMODULE_OK)
		if (RedisModule_StringToLongLong(colorstr, &color) == REDISMODULE_OK)
			red = (color != 0) ? true : false;
		
	RedisModule_CloseKey(linkkey);
	return red;
}

int SetRedNode(RedisModuleCtx *ctx, RedisModuleString *nodelink, bool red){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return 0;
	}

	long long lred = (red) ? 1 : 0;
	RedisModuleString *colorstr = RedisModule_CreateStringFromLongLong(ctx, lred);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
							color_field_name, colorstr, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}
RedisModuleString* GetLeft(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}

	RedisModuleString *left = NULL;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							left_field_name, &left, NULL) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}
	RedisModule_CloseKey(linkkey);
	return left;
}

RedisModuleString* GetRight(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}

	RedisModuleString *right = NULL;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							right_field_name, &right, NULL) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}
	RedisModule_CloseKey(linkkey);
	return right;
}

int SetLeft(RedisModuleCtx *ctx, RedisModuleString *nodelink, RedisModuleString *childlink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	if (childlink == NULL)
		RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
							left_field_name, REDISMODULE_HASH_DELETE, NULL);
	else if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
								 left_field_name, childlink, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	RedisModule_CloseKey(linkkey);
	return 0;
}


int SetRight(RedisModuleCtx *ctx, RedisModuleString *nodelink, RedisModuleString *childlink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	if (childlink == NULL)
		RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
							right_field_name, REDISMODULE_HASH_DELETE, NULL);
	else if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
								 right_field_name, childlink, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	RedisModule_CloseKey(linkkey);
	return 0;
}


int DeleteRBNode(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return -1;
	}
	RedisModule_UnlinkKey(key);
	RedisModule_CloseKey(key);
	return 0;
}

bool IsNodeNull(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(linkkey);
		return true;
	}
	RedisModule_CloseKey(linkkey);
	return false;
}

int GetKey(RedisModuleCtx *ctx, RedisModuleString *nodelink, Seqn &s){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							key_field_name, &keystr, NULL) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	const char *ptr = RedisModule_StringPtrLen(keystr, NULL);
	if (sscanf(ptr, key_fmt, &s.arr[0], &s.arr[1], &s.arr[2]) != 3){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}

int SetKey(RedisModuleCtx *ctx, RedisModuleString *nodelink, Seqn s){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr = RedisModule_CreateStringPrintf(ctx, key_fmt, s.arr[0], s.arr[1], s.arr[2]);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, key_field_name, keystr, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}

int GetMaxKey(RedisModuleCtx *ctx, RedisModuleString *nodelink, Seqn &s){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							max_field_name, &keystr, NULL) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	const char *ptr = RedisModule_StringPtrLen(keystr, NULL);
	if (sscanf(ptr, key_fmt, &s.arr[0], &s.arr[1], &s.arr[2]) != 3){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}

int SetMaxKey(RedisModuleCtx *ctx, RedisModuleString *nodelink, Seqn s){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr = RedisModule_CreateStringPrintf(ctx, key_fmt, s.arr[0], s.arr[1], s.arr[2]);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, max_field_name, keystr, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	
	RedisModule_CloseKey(linkkey);
	return 0;
}

int GetValue(RedisModuleCtx *ctx, RedisModuleString *nodelink, Value &val){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *value_str;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							value_field_name, &value_str, NULL)== REDISMODULE_ERR){
		RedisModule_Log(ctx, "debug", "ERR: unable to get value str");
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	const char *ptr = RedisModule_StringPtrLen(value_str, NULL);
	if (sscanf(ptr, value_fmt, &(val.id), &(val.dur.start), &(val.dur.end)) != 3){
		RedisModule_Log(ctx, "debug", "ERR: unable to get id, start, end times");
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	
	
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS,
							descr_field_name, &(val.descr), NULL) == REDISMODULE_ERR){
		RedisModule_Log(ctx, "debug", "ERR: unable to get descr field");
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}

int SetValue(RedisModuleCtx *ctx, RedisModuleString *nodelink, Value val){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
    int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;;
	}

	RedisModuleString *value_str = RedisModule_CreateStringPrintf(ctx, value_fmt, val.id, 
																  val.dur.start, val.dur.end);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, value_field_name, value_str, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	if (val.descr != NULL){
		if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, descr_field_name, val.descr, NULL) != 1){
			RedisModule_CloseKey(linkkey);
			return -1;
		}
	}
	
	RedisModule_CloseKey(linkkey);
	return 0;
}

int SetRBNode(RedisModuleCtx *ctx, RedisModuleString *nodelink, Seqn s, Value val){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH && keytype != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr = RedisModule_CreateStringPrintf(ctx,
								key_fmt, s.arr[0], s.arr[1], s.arr[2]);
	RedisModuleString *countstr = RedisModule_CreateStringPrintf(ctx, "%d", 1);
	RedisModuleString *redstr = RedisModule_CreateStringPrintf(ctx, "%d", 1);
	RedisModuleString *valuestr = RedisModule_CreateStringPrintf(ctx,
									 value_fmt,val.id, val.dur.start, val.dur.end);
	
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
							key_field_name, keystr,
							count_field_name, countstr,
							color_field_name, redstr,
							value_field_name, valuestr,
							max_field_name, keystr,
							descr_field_name, val.descr, NULL) != 4){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}

int FlipNode(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	return SetRedNode(ctx, nodelink, !IsRedNode(ctx, nodelink));
}


int FlipNodeAndChildNodes(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	if (FlipNode(ctx, nodelink) < 0)
		return -1;

	RedisModuleString *left = GetLeft(ctx, nodelink);
	if (FlipNode(ctx, left) < 0)
		return -1;
	
	RedisModuleString *right = GetRight(ctx, nodelink);
	if (FlipNode(ctx, right) < 0)
		return -1;

	return 0;
}

RedisModuleString* RotateLeft(RedisModuleCtx *ctx, RedisModuleString *h){
	RedisModuleString *x = GetRight(ctx, h);
	RedisModuleString *xleft = GetLeft(ctx, x);
	
	SetRight(ctx, h, xleft);
   	SetLeft(ctx, x, h);

	SetRedNode(ctx, x, IsRedNode(ctx, h));
	SetRedNode(ctx, h, true);

	RedisModuleString *hleft = GetLeft(ctx, h);
	RedisModuleString *hright = GetRight(ctx, h);
	
	SetNodeCount(ctx, x, GetNodeCount(ctx, h));
	SetNodeCount(ctx, h, 1 + GetNodeCount(ctx,hleft) + GetNodeCount(ctx, hright));

	Seqn hmax;
	if (hright != NULL){
		GetMaxKey(ctx, hright, hmax);
	} else {
		GetKey(ctx, h, hmax);
	}
	SetMaxKey(ctx, h, hmax);
	return x;
}

RedisModuleString* RotateRight(RedisModuleCtx *ctx, RedisModuleString *h){
	RedisModuleString *x = GetLeft(ctx, h);

	RedisModuleString *xright = GetRight(ctx, x);
	SetLeft(ctx, h, xright);
	SetRight(ctx, x, h);

	SetRedNode(ctx, x, IsRedNode(ctx, h));
	SetRedNode(ctx, h, true);

	RedisModuleString *hleft = GetLeft(ctx, h);
	RedisModuleString *hright = GetRight(ctx, h);
	
	SetNodeCount(ctx, x, GetNodeCount(ctx, h));
	SetNodeCount(ctx, h, 1 + GetNodeCount(ctx, hleft) + GetNodeCount(ctx, hright));

	Seqn hmax;
	GetMaxKey(ctx, h, hmax);
	SetMaxKey(ctx, x, hmax);
	
	return x;
}

RedisModuleString* balance(RedisModuleCtx *ctx, RedisModuleString *h){

	if (IsRedNode(ctx, GetRight(ctx, h)))
		h = RotateLeft(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, h)) && IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h))))
		h = RotateRight(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, h)) && IsRedNode(ctx, GetRight(ctx, h)))
		FlipNodeAndChildNodes(ctx, h);

	SetNodeCount(ctx, h, 1 + GetNodeCount(ctx, GetLeft(ctx, h)) + GetNodeCount(ctx, GetRight(ctx, h)));
	return h;
}

RedisModuleString* rbtree_insert(RedisModuleCtx *ctx, RedisModuleString *tree, RedisModuleString *node,
								 Seqn key, Value val, int level = 0){
	if (node == NULL){
		RedisModuleString *linkstr = RedisModule_CreateStringPrintf(ctx, linkfmtstr,
												RedisModule_StringPtrLen(tree, NULL), next_link());
		RedisModule_Log(ctx, "debug", "(level %d) insert new link - %s",
						level, RedisModule_StringPtrLen(linkstr, NULL));
		SetRBNode(ctx, linkstr, key, val);
		return linkstr;
	}

	RedisModuleString *left = GetLeft(ctx, node);
	RedisModuleString *right = GetRight(ctx, node);
	
	Seqn node_key;
	assert(GetKey(ctx, node, node_key) == 0);
	RedisModule_Log(ctx, "debug", "(level %d) insert at  node %u %u %u",
					level, node_key.arr[0], node_key.arr[1], node_key.arr[2]);
	int cmp = cmpseqnums(key, node_key);
	if (cmp <= 0){
		RedisModule_Log(ctx, "debug", "insert into left");
		left = rbtree_insert(ctx, tree, left, key, val, level+1);
		SetLeft(ctx, node, left);
	} else if (cmpseqnums(key, node_key) > 0){
		RedisModule_Log(ctx, "debug", "insert into right");
		right = rbtree_insert(ctx, tree, right, key, val, level+1);
		SetRight(ctx, node, right);

		Seqn mk;
		assert(GetMaxKey(ctx, right, mk) == 0);
		assert(SetMaxKey(ctx, node, mk) == 0);
	}

	if (IsRedNode(ctx, right) && !IsRedNode(ctx, left)){
		RedisModule_Log(ctx, "debug", "(level %d) right leaning 3-node - rotate left", level);
		node = RotateLeft(ctx, node);
	}

	left = GetLeft(ctx, node);
	RedisModuleString *leftleft = GetLeft(ctx, left);
	right = GetRight(ctx, node);

	if (IsRedNode(ctx, left) && IsRedNode(ctx, leftleft)){
		RedisModule_Log(ctx, "debug", "(level %d) left and left.left are red - rotate right", level);
		node = RotateRight(ctx, node);
	}

	left = GetLeft(ctx, node);
	right = GetRight(ctx, node);
	if (IsRedNode(ctx, left) && IsRedNode(ctx, right)){
		RedisModule_Log(ctx, "debug", "(level %d) convert 4-node to 2-node", level);
		FlipNodeAndChildNodes(ctx, node);
	}

	SetNodeCount(ctx, node, 1 + GetNodeCount(ctx, left) + GetNodeCount(ctx, right));
	return node;
}

int RBTreeInsert(RedisModuleCtx *ctx, RedisModuleString *tree, Seqn key, Value val){
	RedisModuleString *root = GetRoot(ctx, tree);
	
	RedisModule_Log(ctx, "debug", "insert %s into tree %s",
					RedisModule_StringPtrLen(val.descr, NULL),
					RedisModule_StringPtrLen(tree, NULL));
	RedisModuleString *node = rbtree_insert(ctx, tree, root, key, val);
	if (node == NULL){
		RedisModule_Log(ctx, "error", "unable to insert id = %lld", val.id);
		return -1;
	}
	SetRoot(ctx, tree, node);
	SetRedNode(ctx, node, false);
    return 0;
}

int RBTreeLookup(RedisModuleCtx *ctx, RedisModuleString *tree, Seqn key, vector<Value> &lookup){
	RedisModuleString *node = GetRoot(ctx, tree);
	lookup.clear();
	RedisModule_Log(ctx, "debug", "lookup %u-%u-%u", key.arr[0], key.arr[1], key.arr[2]);
	while (node != NULL){
		Seqn current_key;
		assert(GetKey(ctx, node, current_key) == 0);
		int cmp = cmpseqnums(key, current_key);
		if (cmp <= 0){
			if (cmp == 0){
				Point p;
				spfc_decode(current_key, p);
				Value v;
				assert(GetValue(ctx, node, v) == 0);
				v.longitude = (double)p.arr[0]/(double)LONG_LAT_SCALE_FACTOR - 180.0;
				v.latitude = (double)p.arr[1]/(double)LONG_LAT_SCALE_FACTOR - 90.0;
				RedisModule_Log(ctx, "debug", "found %s %lld", RedisModule_StringPtrLen(v.descr, NULL), v.id);
				lookup.push_back(v);
			}
			RedisModule_Log(ctx, "debug", "left child node");
			node = GetLeft(ctx, node);
		} else {
			RedisModule_Log(ctx, "debug", "right child node");
			node = GetRight(ctx, node);
		}
	}
	return 0;
}

RedisModuleString* minimum(RedisModuleCtx *ctx, RedisModuleString *h){
	while (GetLeft(ctx, h) != NULL)
		h = GetLeft(ctx, h);
	return h;
}

RedisModuleString* MoveRedLeft(RedisModuleCtx *ctx, RedisModuleString *h){
	FlipNodeAndChildNodes(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, GetRight(ctx, h)))){
		SetRight(ctx, h, RotateRight(ctx, GetRight(ctx, h)));
		h = RotateLeft(ctx, h);
	}
	return h;
}

RedisModuleString* MoveRedRight(RedisModuleCtx *ctx, RedisModuleString *h){
	FlipNodeAndChildNodes(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h))))
		h = RotateRight(ctx, h);
	return h;
}


RedisModuleString* delete_min(RedisModuleCtx *ctx, RedisModuleString *h, int level=0){
	if (GetLeft(ctx, h) == NULL){
		RedisModule_Log(ctx, "debug", "delete node: %s", RedisModule_StringPtrLen(h, NULL));
		DeleteRBNode(ctx, h);
		return NULL;
	}
	
	Value hvalue;
	GetValue(ctx, h, hvalue);
	RedisModule_Log(ctx, "debug", "delete_min (level %d) node %s",
					level, RedisModule_StringPtrLen(hvalue.descr, NULL));
	
	if (!IsRedNode(ctx, GetLeft(ctx, h)) && !IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h))))
		h = MoveRedLeft(ctx, h);
	SetLeft(ctx, h, delete_min(ctx, GetLeft(ctx, h), level+1));
	return balance(ctx, h);
}

int RBTreeDeleteMin(RedisModuleCtx *ctx, RedisModuleString *tree){
	RedisModuleString *root = GetRoot(ctx, tree);
	if (root == NULL)
		return -1;
	if (!IsRedNode(ctx, GetLeft(ctx, root)) && !IsRedNode(ctx, GetRight(ctx, root)))
		SetRedNode(ctx, root, true);
	root = delete_min(ctx, root);
	if (root != NULL)
		SetRedNode(ctx, root, false);
	SetRoot(ctx, tree, root);
	return 0;
}

RedisModuleString* delete_max(RedisModuleCtx *ctx, RedisModuleString *h, int level=0){
	if (IsRedNode(ctx, GetLeft(ctx, h)))
		h = RotateRight(ctx, h);
	if (GetRight(ctx, h) == NULL)
		return NULL;
	if (!IsRedNode(ctx, GetRight(ctx, h)) && !IsRedNode(ctx, GetLeft(ctx, GetRight(ctx, h))))
		h = MoveRedRight(ctx, h);
	SetRight(ctx, h, delete_max(ctx, GetRight(ctx, h), level+1));
	return balance(ctx, h);
}

int RBTreeDeleteMax(RedisModuleCtx *ctx, RedisModuleString *tree){
	RedisModuleString *root = GetRoot(ctx, tree);
	if (root == NULL)
		return -1;
	if (!IsRedNode(ctx, GetLeft(ctx, root)) && !IsRedNode(ctx, GetRight(ctx, root)))
		SetRedNode(ctx, root, true);
	root = delete_max(ctx, root);
	if (root != NULL)
		SetRedNode(ctx, root, false);
	SetRoot(ctx, tree, root);
	return 0;
}

RedisModuleString* delete_key(RedisModuleCtx *ctx, RedisModuleString *h, Seqn key, long long id, int level = 0){
	if (h == NULL)
		return NULL;
	
	Seqn hkey;
	GetKey(ctx, h, hkey);

	Value hvalue;
	GetValue(ctx, h, hvalue);
	
	RedisModule_Log(ctx, "debug", "delete (level %d) key %u %u %u %s %lld",
					level, hkey.arr[0], hkey.arr[1], hkey.arr[2],
					RedisModule_StringPtrLen(hvalue.descr, NULL), hvalue.id);
	if (cmpseqnums(key, hkey) < 0 || (cmpseqnums(key, hkey) == 0 && id != hvalue.id)){
		if (GetLeft(ctx, h) != NULL && !IsRedNode(ctx, GetLeft(ctx, h))
			                            && !IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h)))){
			h = MoveRedLeft(ctx, h);
		}
		SetLeft(ctx, h, delete_key(ctx, GetLeft(ctx, h), key, id, level+1));
	} else {
		if (IsRedNode(ctx, GetLeft(ctx, h))){
			h = RotateRight(ctx, h);
			GetKey(ctx, h, hkey);
			GetValue(ctx, h, hvalue);
		}
		if ((cmpseqnums(key, hkey) == 0 || id == hvalue.id) && GetRight(ctx, h) == NULL){
			RedisModule_Log(ctx, "debug", "delete (level %d) delete node %s %lld",
							level, RedisModule_StringPtrLen(hvalue.descr, NULL), hvalue.id);
			assert(DeleteRBNode(ctx, h) == 0);
			return NULL;
		}

		if (GetRight(ctx, h) != NULL && !IsRedNode(ctx, GetRight(ctx, h))
			                                 &&!IsRedNode(ctx, GetLeft(ctx, GetRight(ctx, h)))){
			h = MoveRedRight(ctx, h);
			GetKey(ctx, h, hkey);
			GetValue(ctx, h, hvalue);
		}
		if (cmpseqnums(key, hkey) == 0 || id == hvalue.id){
			if (id == hvalue.id){
				Value hvalue;
				assert(GetValue(ctx, h, hvalue) == 0);
				RedisModule_Log(ctx, "debug", "delete (level %d) node %lld %s", level,
								hvalue.id, RedisModule_StringPtrLen(hvalue.descr, NULL));
			
				RedisModuleString *xnode = minimum(ctx, GetRight(ctx, h));
				Seqn xkey;
				assert(GetKey(ctx, xnode, xkey) == 0);
				assert(SetKey(ctx, h, xkey) == 0);
				Value xvalue;
				assert(GetValue(ctx, xnode, xvalue) == 0);
				assert(SetValue(ctx, h, xvalue) == 0);
			
				Seqn mkey;
				assert(GetMaxKey(ctx, GetRight(ctx, h), mkey) == 0);
				assert(SetMaxKey(ctx, h, mkey) == 0);

				RedisModule_Log(ctx, "debug", "delete (level %d) replace with %lld %s", level,
								xvalue.id, RedisModule_StringPtrLen(xvalue.descr, NULL));
				
				SetRight(ctx, h , delete_min(ctx, GetRight(ctx, h), level+1));
				SetNodeCount(ctx, h, 1 + GetNodeCount(ctx, GetLeft(ctx, h)) + GetNodeCount(ctx, GetRight(ctx, h)));
			}
		} else {
			SetRight(ctx, h, delete_key(ctx, GetRight(ctx, h), key, id, level+1));
		}
	}
	return balance(ctx, h);
}

int RBTreeDeleteKey(RedisModuleCtx *ctx, RedisModuleString *tree, Seqn key, long long eventid){
	RedisModuleString *root = GetRoot(ctx, tree);
	if (root == NULL)
		return 0;
	
	if (!IsRedNode(ctx, GetLeft(ctx, root)) && !IsRedNode(ctx, GetRight(ctx, root)))
		SetRedNode(ctx, root, true);

	RedisModule_Log(ctx, "debug", "delete id = %lld, key = %u %u %u",
					eventid, key.arr[0], key.arr[1], key.arr[2]);

	root = delete_key(ctx, root, key, eventid);
	if (root != NULL)
		SetRedNode(ctx, root, false);

	SetRoot(ctx, tree, root);
	return 0;
}

int RBTreeClear(RedisModuleCtx *ctx, RedisModuleString *tree){
	queue<RedisModuleString*> q;

	RedisModuleString *root = GetRoot(ctx, tree);
	if (tree != NULL)
		q.push(root);

	int count = 0;
	while (!q.empty()){
		RedisModuleString *node = q.front();
		count++;
		
		RedisModuleString *left = GetLeft(ctx, node);
		if (left != NULL)
			q.push(left);
		RedisModuleString *right = GetRight(ctx, node);
		if (right != NULL)
			q.push(right);
		q.pop();
		assert(DeleteRBNode(ctx, node) == 0);
	}
	SetRoot(ctx, tree, NULL);
	return count;
}

/* in-order traversal */
int printtree(RedisModuleCtx *ctx, RedisModuleString *node, int level = 0){
	if (node == NULL) return 0;


	/* visit left */
	printtree(ctx, GetLeft(ctx, node), level+1);

	Seqn key;
	GetKey(ctx, node, key);
	
	Value val;
	GetValue(ctx, node, val);

	Point p;
	spfc_decode(key, p);
	
	double longitude = (double)p.arr[0]/(double)LONG_LAT_SCALE_FACTOR - 180.0;
	double latitude  = (double)p.arr[1]/(double)LONG_LAT_SCALE_FACTOR - 90.0;
	time_t t1 = static_cast<time_t>(p.arr[2]);
	
		
	char scratch[64];
	strftime(scratch, 64, "%m-%d-%Y %H:%M",localtime(&t1));
	RedisModule_Log(ctx, "info", "(level %d) \"%s\" red=%d, uid=%d %s %.6f/%.6f",
					level, RedisModule_StringPtrLen(val.descr, NULL),
					IsRedNode(ctx, node), val.id, scratch, latitude, longitude);

	/* visit right */
	printtree(ctx, GetRight(ctx, node), level+1);
	
	return 0;
}

int RBTreePrint(RedisModuleCtx *ctx, RedisModuleString *tree){
	RedisModuleString *root = GetRoot(ctx, tree);
	RedisModule_Log(ctx, "info", "log tree %s", RedisModule_StringPtrLen(root, NULL));
	return printtree(ctx, root);
}

int RBTreeSize(RedisModuleCtx *ctx, RedisModuleString *tree){
	RedisModuleString *root = GetRoot(ctx, tree);
	if (root == NULL)
		return 0;
	return GetNodeCount(ctx, root);
}

int RBTreeQuery(RedisModuleCtx *ctx, RedisModuleString *node, const Region qr, Seqn &next,
				vector<Value> &results, int level=0){
	int rc = 0;
	if (node == NULL)
		return 0;

	Seqn key;
	GetKey(ctx, node, key);
	RedisModule_Log(ctx, "debug", "query (level %d), key %u %u %u", level, key.arr[0], key.arr[1], key.arr[2]);
	if (cmpseqnums(next, key) <= 0){
		RedisModule_Log(ctx, "debug", "query (level %d) left branch", level);
		if (RBTreeQuery(ctx, GetLeft(ctx, node), qr, next, results, level+1) < 0)
			return -1;
	}

	if (contains(qr, key)){
		Point p;
		spfc_decode(key, p);

		Value val;
		val.longitude = (double)p.arr[0]/(double)LONG_LAT_SCALE_FACTOR - 180.0;
		val.latitude = (double)p.arr[1]/(double)LONG_LAT_SCALE_FACTOR  - 90.0;
		GetValue(ctx, node, val);
		RedisModule_Log(ctx, "debug", "query (level %d) found: %s",
						level, RedisModule_StringPtrLen(val.descr, NULL));
		results.push_back(val);
	}

	next = key;
	Seqn prev = next;
	RedisModule_Log(ctx, "debug", "next match: %u %u %u", next.arr[0], next.arr[1], next.arr[2]);
	if (!contains(qr, next) && !next_match(qr, prev, next)){
		RedisModule_Log(ctx, "debug", "query (level %d) no next match", level);
		return 0;
	}
	RedisModule_Log(ctx, "debug", "query (level %d) next match: %u %u %u",
					level, next.arr[0], next.arr[1], next.arr[2]);
	
	Seqn maxkey;
	GetMaxKey(ctx, node, maxkey);
	if (cmpseqnums(next, maxkey) <= 0){
		RedisModule_Log(ctx, "debug", "(level %d) query - right", level);
		if (RBTreeQuery(ctx, GetRight(ctx, node), qr, next, results, level+1) < 0)
			return -1;
	}

	return 0;
}

int RBTreeQuery(RedisModuleCtx *ctx, RedisModuleString *tree, const Region qr, vector<Value> &results){
	RedisModuleString *root = GetRoot(ctx, tree);
	if (root == NULL)
		return -1;

	Seqn prev, next;
	if (next_match(qr, prev, next)){
		RedisModule_Log(ctx, "debug", "query tree %s prev %u %u %u",
						RedisModule_StringPtrLen(root, NULL), prev.arr[0], prev.arr[1], prev.arr[2]);
		RedisModule_Log(ctx, "debug", "query tree %s next %u-%u-%u",
						RedisModule_StringPtrLen(root, NULL), next.arr[0], next.arr[1], next.arr[2]);
		if (RBTreeQuery(ctx, root, qr, next, results) < 0)
			return -1;
	}
	
	return 0;
}

int RBTreeQuery2(RedisModuleCtx *ctx, RedisModuleString *node, const Region qr, Seqn &next,
				vector<Result> &results, int level=0){
	int rc = 0;
	if (node == NULL)
		return 0;

	Seqn key;
	GetKey(ctx, node, key);
	RedisModule_Log(ctx, "debug", "query (level %d), key %u %u %u", level, key.arr[0], key.arr[1], key.arr[2]);
	if (cmpseqnums(next, key) <= 0){
		RedisModule_Log(ctx, "debug", "query (level %d) left branch", level);
		if (RBTreeQuery2(ctx, GetLeft(ctx, node), qr, next, results, level+1) < 0)
			return -1;
	}

	if (contains(qr, key)){
		Value val;
		GetValue(ctx, node, val);
		RedisModule_Log(ctx, "debug", "query (level %d), found %lld", level, val.id);
		Result r;
		r.key = key;
		r.id = val.id;
		results.push_back(r);
	}

	next = key;
	Seqn prev = next;
	RedisModule_Log(ctx, "debug", "next match: %u %u %u", next.arr[0], next.arr[1], next.arr[2]);
	if (!contains(qr, next) && !next_match(qr, prev, next)){
		RedisModule_Log(ctx, "debug", "query (level %d) no next match", level);
		return 0;
	}
	RedisModule_Log(ctx, "debug", "query (level %d) next match: %u %u %u",
					level, next.arr[0], next.arr[1], next.arr[2]);
	
	Seqn maxkey;
	GetMaxKey(ctx, node, maxkey);
	if (cmpseqnums(next, maxkey) <= 0){
		RedisModule_Log(ctx, "debug", "(level %d) query - right", level);
		if (RBTreeQuery2(ctx, GetRight(ctx, node), qr, next, results, level+1) < 0)
			return -1;
	}

	return 0;
}

int RBTreeQuery2(RedisModuleCtx *ctx, RedisModuleString *tree, const Region qr, vector<Result> &results){
	RedisModuleString *root = GetRoot(ctx, tree);
	if (root == NULL)
		return -1;

	Seqn prev, next;
	if (next_match(qr, prev, next)){
		RedisModule_Log(ctx, "debug", "query tree %s prev %u %u %u",
						RedisModule_StringPtrLen(root, NULL), prev.arr[0], prev.arr[1], prev.arr[2]);
		RedisModule_Log(ctx, "debug", "query tree %s next %u-%u-%u",
						RedisModule_StringPtrLen(root, NULL), next.arr[0], next.arr[1], next.arr[2]);
		if (RBTreeQuery2(ctx, root, qr, next, results) < 0)
			return -1;
	}
	
	return 0;
}


/* args: mytree longitude latitude date-start time-start date-end time-end title */
/*  Dates in the form MM-DD-YYYY  e.g. 12-04-2019 */
/*  Times in the form HH-MM       e.g. 18:30      */
/* returns integer id value for new entry */
extern "C" int RBTreeInsert_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 9)
		return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	RedisModuleString *tree_name = argv[1];
	RedisModuleString *long_str = argv[2];
	RedisModuleString *lat_str = argv[3];
	RedisModuleString *date_start_str = argv[4];
	RedisModuleString *time_start_str = argv[5];
	RedisModuleString *date_end_str = argv[6];
	RedisModuleString *time_end_str = argv[7];
	RedisModuleString *descr = argv[8];

	// assign id number for new entry 
	RedisModuleCallReply *reply = RedisModule_Call(ctx, "INCRBY", "cl", "event:id", 10);
	if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER){
		RedisModule_ReplyWithError(ctx, "ERR unable to get id");
		return REDISMODULE_ERR;
	}

	long long event_id = RedisModule_CallReplyInteger(reply);
	
	RedisModule_Log(ctx, "debug", "insert - assign id %lld", event_id);
	
	double x, y;
	if (ParseLongLat(long_str, lat_str, x, y) < 0){
		RedisModule_ReplyWithError(ctx, "ERR long./lat. args cannot be parsed");
		return REDISMODULE_ERR;
	}

	if (x < -180.0 || x > 180.0 || y < -90.0 || y > 90.0){
		RedisModule_ReplyWithError(ctx, "ERR bad long/lat values");
		return REDISMODULE_ERR;
	}

	time_t start, end;
	if (ParseDatetime(date_start_str, time_start_str, start) < 0){
		RedisModule_ReplyWithError(ctx, "ERR start date/time strings cannot be parsed");
		return REDISMODULE_ERR;
	}
	if (ParseDatetime(date_end_str, time_end_str, end) < 0){
		RedisModule_ReplyWithError(ctx, "ERR end date/time strings cannot be parsed");
		return REDISMODULE_ERR;
	}
	if (start > end){
		RedisModule_ReplyWithError(ctx, "ERR end time cannot be greater than start time");
		return REDISMODULE_ERR;
	}
	
	Point pnt;
	pnt.arr[0] = static_cast<uint32_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[1] = static_cast<uint32_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[2] = static_cast<uint32_t>(start);
	
	Seqn k;
	spfc_encode(pnt, k);
	Value v;
	v.id = event_id;
	v.dur.start = start;
	v.dur.end = end;
	v.descr = descr;
	if (RBTreeInsert(ctx, tree_name, k, v) < 0){
		RedisModule_ReplyWithError(ctx, "ERR unable to insert properly");
		return REDISMODULE_ERR;
	}
	RedisModule_ReplyWithLongLong(ctx, event_id);
	return REDISMODULE_OK;
}

/* args: mytree */
extern "C" int RBTreePrint_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	RedisModuleString *tree_name = argv[1];
	if (RBTreePrint(ctx, tree_name) < 0){
		RedisModule_ReplyWithError(ctx, "ERR unable to print tree to log");
		return REDISMODULE_ERR;
	}
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

/* args: mytree long. lat. date time event_id */
extern "C" int RBTreeDeleteKey_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc  != 7)
		return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);

	RedisModuleString *tree = argv[1];
	RedisModuleString *longstr = argv[2];
	RedisModuleString *latstr = argv[3];
	RedisModuleString *datestr = argv[4];
	RedisModuleString *timestr = argv[5];
	RedisModuleString *eventidstr = argv[6];
	RedisModule_Log(ctx, "debug", "delete nodes %s (%lld) for long. =  %s lat. = %s date = %s time = %s",
					RedisModule_StringPtrLen(tree, NULL),
					RedisModule_StringPtrLen(eventidstr, NULL),
					RedisModule_StringPtrLen(longstr, NULL),
					RedisModule_StringPtrLen(latstr, NULL),
					RedisModule_StringPtrLen(datestr, NULL),
					RedisModule_StringPtrLen(timestr, NULL));

	long long eventid;
	if (RedisModule_StringToLongLong(eventidstr, &eventid) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "unable to parse eventid value");
		return REDISMODULE_ERR;
	}

	double x, y;
	if (ParseLongLat(longstr, latstr, x, y) < 0){
		RedisModule_ReplyWithError(ctx, "bad longitude latitude values");
		return REDISMODULE_ERR;
	}

	if (x < -180.0 || x > 180.0 || y < -90.0 || y > 90.0){
		RedisModule_ReplyWithError(ctx, "out of range longitude/latitude values");
		return REDISMODULE_ERR;
	}
	
	time_t t1;
	if (ParseDatetime(datestr, timestr, t1) < 0){
		RedisModule_ReplyWithError(ctx, "bad date time strings");
		return REDISMODULE_ERR;
	}

	Point p;
	p.arr[0] = static_cast<uint32_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	p.arr[1] = static_cast<uint32_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	p.arr[2] = static_cast<uint32_t>(t1);

	Seqn key;
	spfc_encode(p, key);

	if (RBTreeDeleteKey(ctx, tree, key, eventid) < 0){
		RedisModule_ReplyWithError(ctx, "unable to delete");
		return REDISMODULE_OK;
	}

	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

/* args: mytree */
extern "C" int RBTreeSize_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);
	RedisModuleString *tree = argv[1];
	long long sz = (long long)RBTreeSize(ctx, tree);
	RedisModule_ReplyWithLongLong(ctx, sz);
	return REDISMODULE_OK;
}

/* delete all entries before specificed date-time */
/* args: mytree date time */
extern "C" int RBTreePurge_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 4)
		return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	RedisModuleString *tree = argv[1];
	RedisModuleString *datestr = argv[2];
	RedisModuleString *timestr = argv[3];

	RedisModule_Log(ctx, "debug", "Purge nodes in %s before %s %s",
					RedisModule_StringPtrLen(tree, NULL),
					RedisModule_StringPtrLen(datestr, NULL),
					RedisModule_StringPtrLen(timestr, NULL));
	
	time_t t1;
	if (ParseDatetime(datestr, timestr, t1) < 0){
		RedisModule_ReplyWithError(ctx, "error parsing time values");
		return REDISMODULE_ERR;
	}

	Region qr;
	qr.lower[0] = qr.lower[1] = qr.lower[2] = 0;
	qr.upper[0] = qr.upper[1] = numeric_limits<uint32_t>::max();
	qr.upper[2] = static_cast<uint32_t>(t1);

	vector<Result> results;

	if (RBTreeQuery2(ctx, tree, qr, results) < 0){
		RedisModule_ReplyWithError(ctx, "error completing range query");
		return REDISMODULE_ERR;
	}

	long long n_deletes = (long long)results.size();

	int original_size = RBTreeSize(ctx, tree);
	
	for (Result r : results){
		RBTreeDeleteKey(ctx, tree, r.key, r.id);
	}

	RedisModule_Log(ctx, "debug", "number deleted - %lld tree size %d to %d",
					n_deletes, original_size, RBTreeSize(ctx, tree));
	RedisModule_ReplyWithLongLong(ctx, n_deletes);
	
	return REDISMODULE_OK;
}

/* args: mytree */
extern "C" int RBTreeClear_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);
	RedisModule_AutoMemory(ctx);
	long long n = (long long)RBTreeClear(ctx, argv[1]);
	RedisModule_ReplyWithLongLong(ctx, n);
	return REDISMODULE_OK;
}

/* args: mytree lower_longitude upper_longitude   -180.0 <= long. <= 180.0
                lower_latitude  upper_latitude    -90.0 <= lat. <= 90.0
				start_date start_time                     HH:MM
                end_date end_time                         HH:MM
*/
extern "C" int RBTreeQuery_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 10)
		return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);

	RedisModuleString *tree = argv[1];

	double x1, y1;
	if (ParseLongLat(argv[2], argv[4], x1, y1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR bad lower long/lat limit");
		return REDISMODULE_ERR;
	}

	double x2, y2;
	if (ParseLongLat(argv[3], argv[5], x2, y2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR bad upper long/lat limit");
		return REDISMODULE_ERR;
	}

	if (x1 < -180.0 || x1 > 180.0 || x2 < -180.0 || x2 > 180.0){
		RedisModule_ReplyWithError(ctx, "ERR out of range longitude values");
		return REDISMODULE_ERR;
	}

	if (y1 < -90.0 || y1 > 90.0 || y2 < -90.0 || y2 > 90.0){
		RedisModule_ReplyWithError(ctx, "ERR out of range latitude values");
		return REDISMODULE_ERR;
	}

	RedisModuleString *datestr1 = argv[6];
	RedisModuleString *timestr1 = argv[7];
	
	time_t t1;
	if (ParseDatetime(datestr1, timestr1, t1) < 0){
		RedisModule_ReplyWithError(ctx, "ERR bad lower time limit");
		return REDISMODULE_ERR;
	}

	RedisModuleString *datestr2 = argv[8];
	RedisModuleString *timestr2 = argv[9];

	time_t t2;
	if (ParseDatetime(datestr2, timestr2, t2) < 0){
		RedisModule_ReplyWithError(ctx, "ERR bad upper time limit");
		return REDISMODULE_ERR;
	}
	
	Region qr;
	qr.lower[0] = static_cast<uint32_t>((x1+180.0)*LONG_LAT_SCALE_FACTOR);
	qr.lower[1] = static_cast<uint32_t>((y1+90.0)*LONG_LAT_SCALE_FACTOR);
	qr.lower[2] = static_cast<uint32_t>(t1);

	qr.upper[0] = static_cast<uint32_t>((x2+180.0)*LONG_LAT_SCALE_FACTOR);
	qr.upper[1] = static_cast<uint32_t>((y2+90.0)*LONG_LAT_SCALE_FACTOR);
	qr.upper[2] = static_cast<uint32_t>(t2);

	const char *fmt = "title: %s %.6f/%.6f %s to %s";
	const char *datetimefmt = "%m-%d-%Y %H:%M";
	
	vector<Value> results;
	if (RBTreeQuery(ctx, tree, qr, results) < 0){
		RedisModule_ReplyWithError(ctx, "ERR unable to complete query");
		return REDISMODULE_ERR;
	}

	char starttime[64];
	char endtime[64];

	RedisModule_ReplyWithArray(ctx, results.size());
	for (Value r : results){
		strftime(starttime, sizeof(starttime), datetimefmt, localtime(&(r.dur.start)));
		strftime(endtime, sizeof(endtime),  datetimefmt, localtime(&(r.dur.end)));
		RedisModuleString *resp = RedisModule_CreateStringPrintf(ctx, fmt,
										 RedisModule_StringPtrLen(r.descr, NULL),
										 r.longitude, r.latitude, starttime, endtime);
		RedisModule_ReplyWithString(ctx, resp);
	}
	
	return REDISMODULE_OK;
}


/* lookup keys -  args: tree longitude latitude start-date start-time */
extern "C" int RBTreeLookup_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 6)
		return RedisModule_WrongArity(ctx);

	RedisModule_AutoMemory(ctx);
	RedisModuleString *tree = argv[1];

	double x, y;
	if (ParseLongLat(argv[2], argv[3], x, y) < 0){
		RedisModule_ReplyWithError(ctx, "ERR bad long/lat values");
		return REDISMODULE_ERR;
	}

	if (x < -180.0 || x > 180.0 || y < -90.0 || y > 90.0){
		RedisModule_ReplyWithError(ctx, "ERR out of range long/lat values");
		return REDISMODULE_ERR;
	}
	
	time_t start;
	if (ParseDatetime(argv[4], argv[5], start) < 0){
		RedisModule_ReplyWithError(ctx, "ERR bad date/time");
		return REDISMODULE_ERR;
	}

	Point pnt;
	pnt.arr[0] = static_cast<uint32_t>((x+180.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[1] = static_cast<uint32_t>((y+90.0)*LONG_LAT_SCALE_FACTOR);
	pnt.arr[2] = static_cast<uint32_t>(start);

	Seqn k;
	spfc_encode(pnt, k);

	vector<Value> vals;
	if (RBTreeLookup(ctx, tree, k, vals) < 0){
		RedisModule_ReplyWithError(ctx, "ERR unable to complete lookup");
		return REDISMODULE_ERR;
	}

	const char *lookup_fmt = "%s (%lld) %.6lf/%.6lf %s to %s";
	
	RedisModule_ReplyWithArray(ctx, vals.size());
	for (Value v : vals){
		RedisModuleString *resp = RedisModule_CreateStringPrintf(ctx, lookup_fmt,
									  RedisModule_StringPtrLen(v.descr, NULL),
  									  v.id,  v.longitude, v.latitude,
									  ctime(&v.dur.start), ctime(&v.dur.end));
		RedisModule_ReplyWithString(ctx, resp);
	}
	
	return REDISMODULE_OK;
}


extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (RedisModule_Init(ctx, "reventis", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	locale = localeconv();
	
	if (RedisModule_CreateCommand(ctx, "reventis.insert",  RBTreeInsert_RedisCmd,
								  "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.delete", RBTreeDeleteKey_RedisCmd,
								  "write deny-oom fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.purge", RBTreePurge_RedisCmd,
								  "write", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	if (RedisModule_CreateCommand(ctx, "reventis.print", RBTreePrint_RedisCmd,
								  "readonly", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.size", RBTreeSize_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.clear", RBTreeClear_RedisCmd,
								  "write fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.lookup", RBTreeLookup_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "reventis.query", RBTreeQuery_RedisCmd,
								  "readonly", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	return REDISMODULE_OK;
}

