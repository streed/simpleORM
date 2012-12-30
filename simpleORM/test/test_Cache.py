from nose.tools import assert_equals, assert_raises, assert_true

from  simpleORM.cache import LRUCache 

def test_cache_insert_works():
	cache = LRUCache()

	cache.put( "test", 1 )

	assert_equals( 1, cache.size )
	assert_equals( 1, cache.get( "test" ) )

def test_cache_choosing_least_recently_used_works():

