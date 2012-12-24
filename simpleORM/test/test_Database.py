from nose.tools import assert_equals

from simpleORM.database import Database
from simpleORM.base import Base

def test_Database_creates_all_the_table():
	db = Database( test=( "a", "b", "c" ) )

	print db._table

	#assert_equals( { "a": [], "b": [], "c": [] }, db._table["test"] )


class TestItem( Base ):
	_fields = [ "a", "b", "c" ]
	_domain = "test_domain" 

	def __init__( self, a=0, b=0, c=0 ):
		Base.__init__( self )
		self.a = a
		self.b = b
		self.c = c

def test_Database_insert_single_item():
	db = Database( test_domain=( "a", "b", "c" ) )

	item = TestItem( a=123, b=1234, c=12345 )

	print db.insert( item )

	item2 = TestItem( a=3, b=5, c=100 )

	i = db.insert( item2 )

	print db.get( "a" )[i]
