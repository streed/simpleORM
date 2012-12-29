
class ColumnConvertError( Exception ):

	def __init__( self, msg ):
		self.msg = msg

	def __repr__( self ):
		return repr( self.msg )

class ColumnNoneDefinedError( Exception ):
	pass

class RawColumn( object ):

	def __init__( self, name, convert=lambda v: v, constraint=lambda v: True ):
		self._constraint = constraint
		self._convert = convert
		self.name = name

	def convert( self, val ):
		if self._constraint( val ):
			return self._convert( val )
		else:
			raise ColumnConvertError( "Could not convert %s properly" % ( val ) )


class StringColumn( RawColumn ):
	def __init__( self, name, constraint=lambda v: True ):
		super( RawColumn, self ).__init__( name, convert=str, constraint=constraint )

class IntColumn( RawColumn ):

	def __init__( self, name, constraint=lambda v: True ):
		super( RawColumn, self ).__init__( name, convert=int, constraint=constraint )


