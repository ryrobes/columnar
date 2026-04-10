/*-------------------------------------------------------------------------
 *
 * columnar_cache.c
 *
 * Storage and management of cached data.
 *
 * Copyright (c) Hydra, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "c.h"
#include "columnar/columnar.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#include <time.h>
#include <unistd.h>


/*
 * Main caching MemoryContext.
 *
 * This MemoryContext is created at initialization, below 
 * TopMemoryContext, where all cache entries are stored.
 */
static MemoryContext columnarCacheContext = NULL;

/*
 * Cache entry.
 *
 * An entry for caching a column.
 */
typedef struct ColumnarCacheEntry ColumnarCacheEntry;
typedef struct ColumnarCacheKey ColumnarCacheKey;

struct ColumnarCacheKey
{
	uint64 relId;
	uint64 stripeId;
	uint64 chunkId;
	uint32 columnId;
};

struct ColumnarCacheEntry
{
	ColumnarCacheKey key;
	dlist_node list_node;
	uint64 readCount;
	uint64 length;
	time_t creationTime;
	time_t lastAccessTime;
	void *store;
};

/*
 * Storage for cache entries.
 */
static HTAB *cacheEntryMap = NULL;
static dlist_head cacheEntryList;

/*
 * Storage for total length allocated.
 */
static uint64 totalAllocationLength = 0;

/*
 * Cache statistics.
 *
 * Hits and misses, and general cache statistics.
 */
static ColumnarCacheStatistics statistics = { 0 };

/*
 * Housekeeping of current chunk in use - so they are not evicted.
 */
typedef struct ColumarCacheChunkGroupInUse
{
	uint64 ownerId;
	uint64 relId;
	uint64 stripeId;
	uint64 chunkId;
} ColumarCacheChunkGroupInUse;

static List * ChunkGroupsInUse = NIL;

static void FreeCacheEntryStore(ColumnarCacheEntry *entry);
static uint64 RemoveCacheEntry(ColumnarCacheEntry *entry);
static ColumnarCacheEntry * ColumnarFindInCache(uint64 relId, uint64 stripeId,
												uint64 chunkId, uint32 columnId,
												bool recordAccess);

/*
 * ColumnarCacheMemoryContext
 *
 * Returns the cache MemoryContext, initializing the cache MemoryContext
 * as a child of TopMemoryContext if it does not exist, also clears any
 * statistics gathered.
 */
MemoryContext
ColumnarCacheMemoryContext(void)
{
	if (columnarCacheContext == NULL)
	{
		HASHCTL info;
		uint32 hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		columnarCacheContext = 
			AllocSetContextCreate(TopMemoryContext, 
								  "Columnar Decompression Cache", 
								  0, (uint64) (columnar_page_cache_size * 1024 * 1024 * .1), 
								  columnar_page_cache_size * 1024 * 1024);
		memset(&statistics, 0, sizeof(ColumnarCacheStatistics));
		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(ColumnarCacheKey);
		info.entrysize = sizeof(ColumnarCacheEntry);
		info.hcxt = columnarCacheContext;
		cacheEntryMap = hash_create("columnar decompression cache map",
									64, &info, hashFlags);
		dlist_init(&cacheEntryList);
		ChunkGroupsInUse = NIL;
	}

	return columnarCacheContext;
}

/*
* ColumnarResetCache
*
* Deletes the caching memory context and sets it to NULL, thus removing the
* cache and all of its entries.
*/
void
ColumnarResetCache(void)
{
	if (columnarCacheContext != NULL)
	{
		MemoryContextDelete(columnarCacheContext);
		columnarCacheContext = NULL;
		cacheEntryMap = NULL;
		ChunkGroupsInUse = NIL;
	}

	totalAllocationLength = 0U;
}

/*
 * ColumnarFindInCache
 *
 * Searches the cache for an entry for a relation ID and a chunk ID.
 * If found, it increments the readCount, and returns the entry.	If
 * none are found, NULL is returned instead.
 */
static ColumnarCacheEntry *
ColumnarFindInCache(uint64 relId, uint64 stripeId, uint64 chunkId, uint32 columnId,
					bool recordAccess)
{
	if (cacheEntryMap == NULL)
	{
		return NULL;
	}

	ColumnarCacheKey key = {
		.relId = relId,
		.stripeId = stripeId,
		.chunkId = chunkId,
		.columnId = columnId,
	};
	ColumnarCacheEntry *entry = hash_search(cacheEntryMap, &key, HASH_FIND, NULL);
	if (entry == NULL)
	{
		return NULL;
	}

	if (recordAccess)
	{
		entry->readCount++;
		entry->lastAccessTime = time(NULL);
	}

	return entry;
}

/*
 * ColumnarInvalidateCacheEntry
 *
 * Searches for a cache entry for a relation ID and a chunk ID.
 * If found, removes the cache entry, and frees memory associated with
 * it. If not found, nothing is done.
 *
 * Returns boolean.
 */
static bool
ColumnarInvalidateCacheEntry(uint64 relId, uint64 stripeId, uint64 chunkId, uint32 columnId)
{
	if (cacheEntryMap == NULL)
	{
		return true;
	}

	ColumnarCacheKey key = {
		.relId = relId,
		.stripeId = stripeId,
		.chunkId = chunkId,
		.columnId = columnId,
	};
	ColumnarCacheEntry *entry = hash_search(cacheEntryMap, &key, HASH_FIND, NULL);
	if (entry == NULL)
	{
		return true;
	}

	RemoveCacheEntry(entry);
	return true;
}

static void
EvictCache(uint64 size)
{
	uint64 lastCount = 0;
	uint64 nextLowestCount = PG_UINT64_MAX;

	while (size > 0)
	{
		dlist_mutable_iter miter;

		dlist_foreach_modify(miter, &cacheEntryList)
		{
			ColumnarCacheEntry *entry = dlist_container(ColumnarCacheEntry, list_node, miter.cur);

			if (entry->readCount != lastCount && entry->readCount < nextLowestCount)
			{
				nextLowestCount = entry->readCount;
			}

			if (entry->readCount == lastCount)
			{
				bool skipCacheEntry = false;
				ListCell *lc;
				foreach(lc, ChunkGroupsInUse)
				{
						ColumarCacheChunkGroupInUse *chunkGroupInUse =
							(ColumarCacheChunkGroupInUse *) lfirst(lc);

						if (chunkGroupInUse->relId == entry->key.relId &&
							chunkGroupInUse->stripeId == entry->key.stripeId &&
							chunkGroupInUse->chunkId == entry->key.chunkId)
						{
							skipCacheEntry = true;
							break;
						}
				}

					if (skipCacheEntry)
						continue;

					uint64 freedSize = RemoveCacheEntry(entry);
					if (size < freedSize)
					{
						return;
					}
					size -= freedSize;
				}
			}

		lastCount = nextLowestCount;
		nextLowestCount = PG_UINT64_MAX;
	}
}

void
ColumnarMarkChunkGroupInUse(uint64 ownerId, uint64 relId, uint64 stripeId, uint32 chunkId)
{
	bool found = false;
	ListCell *lc;

	MemoryContext ctx = MemoryContextSwitchTo(ColumnarCacheMemoryContext());

	foreach(lc, ChunkGroupsInUse)
		{
			ColumarCacheChunkGroupInUse *chunkGroupInUse =
				(ColumarCacheChunkGroupInUse *) lfirst(lc);

			if (chunkGroupInUse->ownerId == ownerId)
			{
				chunkGroupInUse->ownerId = ownerId;
				chunkGroupInUse->stripeId = stripeId;
				chunkGroupInUse->chunkId = chunkId;
				chunkGroupInUse->relId = relId;
				found = true;
			}
		}

	if (!found)
	{
			ColumarCacheChunkGroupInUse *newChunkGroupInUse =
				palloc0(sizeof(ColumarCacheChunkGroupInUse));

			newChunkGroupInUse->ownerId = ownerId;
			newChunkGroupInUse->relId = relId;
			newChunkGroupInUse->stripeId = stripeId;
			newChunkGroupInUse->chunkId = chunkId;

		ChunkGroupsInUse = lappend(ChunkGroupsInUse, newChunkGroupInUse);
	}

	MemoryContextSwitchTo(ctx);
}

/*
 * ColumnarAddCacheEntry
 *
 * Adds a cache entry, or updates an existing entry.
 */
void
ColumnarAddCacheEntry(uint64 relId, uint64 stripeId, uint64 chunkId, 
					  uint32 columnId, void *data)
{
	if (columnar_enable_page_cache == false)
	{
		return;
	}

	MemoryContext oldContext = MemoryContextSwitchTo(ColumnarCacheMemoryContext());

	ColumnarCacheEntry *entry = ColumnarFindInCache(relId, stripeId, chunkId, columnId,
													false);

	if (entry != NULL)
	{
		FreeCacheEntryStore(entry);
		totalAllocationLength -= entry->length;
	}
	else
	{
		ColumnarCacheKey key = {
			.relId = relId,
			.stripeId = stripeId,
			.chunkId = chunkId,
			.columnId = columnId,
		};
		bool found = false;

		entry = hash_search(cacheEntryMap, &key, HASH_ENTER, &found);
		Assert(!found);
		memset(entry, 0, sizeof(ColumnarCacheEntry));
		entry->key = key;
		entry->creationTime = entry->lastAccessTime = time(NULL);
		entry->readCount = 0;

		/* Add the entry into the list. */
		dlist_push_tail(&cacheEntryList, &(entry->list_node));
	}

	uint64 size = ((StringInfo) data)->len;

	entry->store = data;
	entry->length = size;

	totalAllocationLength += size;

	if (totalAllocationLength >= statistics.maximumCacheSize)
	{
		statistics.maximumCacheSize = totalAllocationLength;
	}

	/* If we are over our cache allocation, clear until we are at 90%. */
	if (totalAllocationLength >= (columnar_page_cache_size * 1024 * 1024))
	{
		EvictCache((columnar_page_cache_size * 1024 * 1024 * .1) + 
					(totalAllocationLength - (columnar_page_cache_size * 1024 * 1024)));
	}

	statistics.writes++;

	MemoryContextSwitchTo(oldContext);
}

/*
 * ColumnarRetrieveCache
 *
 * Search for a cache entry, returning NULL if not found.	If found,
 * make a copy in the current memory context and return it.
 */
void *
ColumnarRetrieveCache(uint64 relId, uint64 stripeId, uint64 chunkId, uint32 columnId)
{
	if (columnar_enable_page_cache == false)
	{
		return NULL;
	}

	ColumnarCacheEntry *entry = ColumnarFindInCache(relId, stripeId, chunkId, columnId,
													true);

	if (entry == NULL)
	{
		statistics.misses++;

		return NULL;
	}

	statistics.hits++;

	void *chunkCopy = entry->store;

	return chunkCopy;
}

/*
 * ColumnarCacheLength
 *
 * Returns how large our cache is, used for accounting.
 */
static uint64
ColumnarCacheLength()
{
	uint64 count = 0;

	if (cacheEntryMap == NULL)
	{
		return 0;
	}

	dlist_iter iter;
	dlist_foreach(iter, &cacheEntryList)
	{
		count++;
	}

	return count;
}

ColumnarCacheStatistics *
ColumnarGetCacheStatistics(void)
{
	statistics.endingCacheSize = totalAllocationLength;
	statistics.entries = ColumnarCacheLength();

	return &statistics;
}


#define CACHE_NATTS 6
Datum cache_walk(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(cache_walk);

/*
 * Used for debugging, as this data is only available in a transaction
 * or if clearing the cache is specifically disabled.
 */


Datum cache_evict(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(cache_evict);

/*
 * Also used for debugging, with the same constraints that it would only
 * work in a transaction of if the clearing mechanism is explicitly disabled.
 */
Datum cache_evict(PG_FUNCTION_ARGS)
{
	uint64 relId =PG_GETARG_INT64(0);
	uint64 stripeId = PG_GETARG_INT64(1);
	uint64 chunkId = PG_GETARG_INT16(2);
	uint32 columnId = PG_GETARG_UINT32(3);

	bool result = ColumnarInvalidateCacheEntry(relId, stripeId, chunkId, columnId);

	PG_RETURN_BOOL(result);
}

static void
FreeCacheEntryStore(ColumnarCacheEntry *entry)
{
	StringInfo str = entry->store;

	if (str == NULL)
	{
		return;
	}

	if (str->data)
	{
		pfree(str->data);
	}

	pfree(str);
	entry->store = NULL;
}

static uint64
RemoveCacheEntry(ColumnarCacheEntry *entry)
{
	ColumnarCacheKey key = entry->key;
	uint64 freedSize = entry->length;

	dlist_delete(&(entry->list_node));
	FreeCacheEntryStore(entry);
	totalAllocationLength -= freedSize;
	statistics.evictions++;
	hash_search(cacheEntryMap, &key, HASH_REMOVE, NULL);

	return freedSize;
}
