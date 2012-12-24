
class _Column( object ):

	def __init__( self, name ):
		self._name = name
		self._items = []
		self._refs = {}


	def insert( self, item ):
		if item in self._refs:
			return self._regs[item]
		else:

			self._items.append( getattr( item, self._name ) )
			self._refs[item] = len( self._items ) - 1

		return self._refs[item]


	def items( self ):
		return self._items


	def __repr__( self ):
		return "<Column( %s, %d )>" % ( self._name, len( self._items ) )

class Database( object ):

	def __init__( self, **kwargs ):
		self._table = {}
		self._name, self._column_names = list( kwargs.iteritems() )[0]
		
		for n in self._column_names:
			self._table[n] = _Column( n )

	
	def insert( self, item ):
		_id = -1
		for k in item._fields:
			_id = self._table[k].insert( item )

		return _id

	
	def get( self, column ):
		return self._table[column].items()


	def __repr__( self ):
		return "<Database( ( %s ) )>" % ( ",".join( self._table.keys() ) )
