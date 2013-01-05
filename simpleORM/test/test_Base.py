from nose.tools import assert_raises, assert_equals

from mocks import SimpleDBMockClass
import boto
boto.__dict__["connect_sdb"] = lambda: SimpleDBMockClass( {} )

from simpleORM.base import Base
from simpleORM.column import IndexColumn, RawColumn, ColumnNoneDefinedError


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
	
	this = IndexColumn( RawColumn( "this" ) )
	_is = RawColumn( "is" )
	a = RawColumn( "a" )
	test = RawColumn( "test" )

	_domain = "test_domain"


def test_that_TypeError_is_not_thrown():	
	assert_not_raises( ColumnNoneDefinedError, TestORM )


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

	#assert_equals( "select * from `test_domain` where `this` = 'test'  limit 200", test.find_by_this( "test" ).to_sql() )

def test_that_the_chaining_produces_proper_sql():
	test = TestORM()

	sql = test.select( ( "this", "is" ) ).where( "this = 'test'" ).order( "is", asc=False ).limit( 10 )

	assert_equals( "select `this`,`is` from `test_domain` where this = 'test' order by is desc limit 10", sql.to_sql() )


def test_that_the_direct_call_to_Builder_from_Base_works_as_expected():
	test = TestORM()

	assert_equals( "select `this` from `test_domain` where '1' = '1'  limit 200", test.select( ( "this", ) ).to_sql() )

def test_that_the_proper_index_is_set_on_the_model():
	test = TestORM()

	assert_equals( "this", test.index )
