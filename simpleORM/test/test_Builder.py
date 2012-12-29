from nose.tools import assert_raises, assert_equals

from simpleORM.base import Base
from simpleORM.builder import Builder
from simpleORM.column import RawColumn

class FakeORM( Base ):
	fake = RawColumn( "fake" )
	_domain = "test_domain"

def test_Builder_returns_default_sql_when_not_initialized():
	fake = FakeORM()
	builder = Builder( fake )

	assert_equals( "select * from `test_domain` where '1' = '1'  limit 200", builder.to_sql() )

def test_Builder_pre_hooks_are_called():
	called = { "t": False }

	def test_pre_hook( sql ):
		called["t"] = True

	fake = FakeORM()
	builder = Builder( fake )

	builder.add_pre_hook( test_pre_hook )

	builder.__enter__()

	assert_equals( True, called["t"] )
