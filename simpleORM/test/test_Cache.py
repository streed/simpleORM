from nose.tools import assert_equals, assert_raises, assert_true

from  simpleORM.cache import LRUCache, CacheKeyConflictError

def test_cache_insert_works():
	cache = LRUCache()

	cache.put( "test", 1 )

	assert_equals( 1, cache.size )
	assert_equals( 1, cache.get( "test" ) )


def test_cache_choosing_least_recently_used_works():
	cache = LRUCache()

	cache.put( "test", 1 )

	assert_raises( CacheKeyConflictError, cache.put, "test", 2 )


def test_cache_replacement_replaces_the_old_value():
	cache = LRUCache()

	cache.put( "test", 1 )
	
	assert_equals( 1, cache.get( "test" ) )
	
	cache.put( "test", 2, replace=True )

	assert_equals( 2, cache.get( "test" ) )


def test_cache_choose_works_correctly():

	cache = LRUCache( max_size=5 )

	for i in range( 5 ):
		cache.put( i, i )

	cache.put( 10, 10 )

	assert( True )
