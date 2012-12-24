from simpleORM.connection import Connection
from uuid import uuid4
from simpleORM.builder import Builder


class _ValueWrapper( object ):

	def __init__( self, value=None ):
		self.value = value
		self._table = None

	def get( self ):
		return self.value

	def set( self, value ):
		self.value = value

	def _set_table( self, table ):
		self._table = table

	def __eq__( self, other ):
		return self.value == other.value and self._table == other._table

	def __repr__( self ):
		return "%s" % self.value


class _MetaSimpleDB( type ):


	"""
		This method sets the fields found in the _fields attribute of the class to None
	"""
	def __init__( cls, name, bases, _dict ):
		if not "_fields" in _dict:
			raise TypeError( "A subclass of simpleORM.Base must have a `_fields` attribute." )

		super( _MetaSimpleDB, cls ).__init__( name, bases, _dict )


	"""
		This will add all the find_by_* methods to the class, for those attributes found in the _fields attribute.
	"""
	def __new__( cls, name, bases, _dict ):
		print "New"
		instance = super( _MetaSimpleDB, cls ).__new__( cls, name, bases, _dict )

		#Create the find_by_*
		for f in instance._fields:
			query = "`%s` = '%%s'" % f

			def find_by( self, val, __query=query ):
				return self.where( __query % val  )

			setattr( instance, "find_by_%s" % f, find_by )

			
			for f in instance._fields:
				value = _ValueWrapper()
				#setattr( instance, "__%s_%s" % ( cls.__class__.__name__, f ), value )
				setattr( instance, f, None )

				print id( value )
	
		return instance


class Base( object ):

	#Let's perform some magic to add in some extra methods.
	#This will also pull in the domain object so that this class can operate.
	__metaclass__ = _MetaSimpleDB

	_connection = Connection()


	_fields = []
	_domain = ""

	#Default consistency is "evententually_consistent" = False
	#Override this to get change the behavior
	_consistency = False


	def __init__( self ):
		self._item = self._connection.new_item( self._domain )
		self._deleted = False
		self._id = str( uuid4() )

	def __hash__( self ):
		return hash( self._id )

	def __equal__( self, other ):

		equal = True

		for f in self._fields:
			equal = equal and ( getattr( self, f ) == getattr( other, f ) )

		return equal

	@classmethod
	def create_domain( cls ):
		dom = cls._connection.create_domain( cls._domain )

		if dom:
			cls._connection._domains[dom.name] = dom
			return True
		else:
			return False


	def delete( self ):
		"""
			PRE: This will delete the row this object manages.
			POST: If the item is deleted this will return True else False
		"""
		if not self._deleted and self._connection.getself._domain( self._domain ).delete_item( self._item ):
			self._deleted = True	

		return self._deleted

	def __call__( self ):
		return Builder( self )


	def _execute( self, query ):
		"""
			PRE: This is called to execute a query directly.
			POST: An iterator is returned from this that will return the data.
		"""
		return self._connection.get_domain( self._domain ).select( query, consistent_read=self._consistency )

	#Wrapper to Builder
	def select( self, outputs ):
		return Builder( self ).select( outputs )

	
	#Wrapper to Builder
	def where( self, where ):
		return Builder( self ).where( where )


	#Wrapper to Builder
	def order( self, column, asc=True ):
		return Builder( self ).order( column, asc=asc )


	#Wrapper to Builder
	def limit( self, lim ):
		return Builder( self ).limit( lim )


	def __repr__( self ):
		return "<Base( %s, %s )>" % ( self._id, ",".join( [ str( getattr( self, f ) ) for f in self._fields ] ) )

