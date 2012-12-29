import boto

from nose.tools import assert_equals,assert_true, with_setup

from simpleORM.base import Base
from simpleORM.column import IntColumn, StringColumn, ListColumn
from simpleORM.row import RowConverter

def set_up():
	conn = boto.connect_sdb()

	dom = conn.get_domain( "test_domain" )

	dom.put_attributes( "Sean", { "name": "Sean", "age": 22, "friends": [ "David", "Melissa" ] } )

def tear_down():
	conn = boto.connect_sdb()

	dom = conn.get_domain( "test_domain" )

	rs = dom.select( "select * from `test_domain`" )

	for r in rs:
		dom.delete_item( r )


class TestORM( Base ):
	name = StringColumn( "name" )
	age = IntColumn( "age" )
	friends = ListColumn( "friends" )

	_domain = "test_domain"

	_consistency = True

def test_RowConverter_pulls_the_proper_column_classes_out_of_a_Base_subclass():
	
	test = TestORM()


@with_setup( set_up, tear_down )
def test_RowConverter_converts_the_values_correctly():

	test = TestORM()

	rs = test.find_by_name( "Sean" )

	rs = list( rs )

	assert_true( True, isinstance( rs[0]["age"], int ) )
	assert_true( True, isinstance( rs[0]["name"], str ) )
	assert_true( True, isinstance( rs[0]["friends"], list ) )

	assert_equals( 22, rs[0]["age"] )
	assert_equals( "Sean", rs[0]["name"] )
	assert_true( all( f in [ "David", "Melissa" ] for f in rs[0]["friends"] ) )
