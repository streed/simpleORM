
class ColumnConvertError( Exception ):

	def __init__( self, msg ):
		self.msg = msg

	def __repr__( self ):
		print self.msg
		return repr( self.msg )
	def __str__( self ):
		return self.__repr__()

def ColumnFailContraintError( ColumnConvertError ):
	pass


class ColumnNoneDefinedError( Exception ):
	pass


class RawColumn( object ):

	def __init__( self, name, convert=lambda v: v, constraint=lambda v: True ):
		self._constraint = constraint
		self._convert = convert
		self.name = name

	def convert( self, val ):
		if self._constraint( val ):
			try:
				return self._convert( val )
			except ValueError as e:
				raise ColumnConvertError( "Received Error converting column `%s` with `%s`" % ( self.name, e ) )
		else:
			raise ColumnFailContraintError( "Column %s: Constraint error given %s" % ( self.name, val ) )


class StringColumn( RawColumn ):
	def __init__( self, name, constraint=lambda v: True ):
		RawColumn.__init__( self, name, convert=str, constraint=constraint )


class IntColumn( RawColumn ):
	def __init__( self, name, constraint=lambda v: True ):
		RawColumn.__init__( self, name, convert=int, constraint=constraint )


class ListColumn( RawColumn ):
	def __init__( self, name, constraint=lambda v: True ):
		RawColumn.__init__( self, name, convert=list, constraint=constraint )


class DictColumn( RawColumn ):
	def __init__( self, name, constraint=lambda v: True ):
		RawColumn.__init__( self, name, convert=dict, constraint=constraint )
