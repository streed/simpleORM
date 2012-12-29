from nose.tools import assert_equals, assert_raises, assert_true

from simpleORM.column import RawColumn, StringColumn, IntColumn, ListColumn, DictColumn
from simpleORM.column import ColumnConvertError, ColumnNoneDefinedError
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
	name = StringColumn( "name" )
	age = StringColumn( "age" )
	_domain = "test_domain"

def test_Column_does_not_throw_exception():
	assert_not_raises( ColumnNoneDefinedError, TestORM )

def test_Column_throws_ColumnConvertError():

	integer = IntColumn( "fake" )

	assert_raises( ColumnConvertError, integer.convert, "abc" )


def test_IntColumn_converts_properly():

	integer = IntColumn( "fake" )

	ret = integer.convert( "123" )

	assert_equals( ret, 123 )
	assert_true( isinstance( ret, int ) )
