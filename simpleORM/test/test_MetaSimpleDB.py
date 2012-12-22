from nose.tools import assert_raises, assert_equals

from simpleORM.base import Base
def assert_not_raises( exception, func, *args, **kwargs ):
	def closure( func, *args, **kwargs ):
		try:
			func( *args, **kwargs )
		except exception:
			raise AssertionError, exception
		else:
			return

	closure( func, *args, **kwargs )

class TestORM( Base ):
	
	_fields = [ "this", "is", "a", "test" ]
	_domain = "test_domain"


def test_that_TypeError_is_not_thrown():	
	assert_not_raises( TypeError, TestORM )


def test_that_each_of_the_fields_becomes_an_attribute():
	test = TestORM()

	assert_equals( True, hasattr( test, "this" ) )
	assert_equals( True, hasattr( test, "is" ) )
	assert_equals( True, hasattr( test, "a" ) )
	assert_equals( True, hasattr( test, "test" ) )


def test_that_all_the_find_methods_are_created():

	test = TestORM()

	assert_equals( True, hasattr( test, "find_by_this" ) )
	assert_equals( True, hasattr( test, "find_by_is" ) )
	assert_equals( True, hasattr( test, "find_by_a" ) )
	assert_equals( True, hasattr( test, "find_by_test" ) )

	assert_equals( True, hasattr( test.find_by_this, "__call__" ))
	assert_equals( True, hasattr( test.find_by_is, "__call__" ))
	assert_equals( True, hasattr( test.find_by_a, "__call__" ))
	assert_equals( True, hasattr( test.find_by_test, "__call__" ))

	assert_equals( "select * from `test_domain` where `this` = 'test'", test.find_by_this( "test" ) )

def test_that_the_chaining_produces_proper_sql():
	test = TestORM()

	sql = test.select( ( "this", "is" ) ).where( "this = 'test'" ).order( "is", asc=False ).limit( 10 )

	assert_equals( "select `this`,`is` from `test_domain` where this = 'test' order by is desc limit 10", sql._builder() )
