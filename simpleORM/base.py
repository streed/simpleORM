from simpleORM.connection import Connection
from simpleORM.builder import Builder

class _MetaSimpleDB( type ):
	def __init__( cls, name, bases, _dict ):
		#Get the fields from the `cls`
		if "_fields" in _dict:
			fields = cls._fields
			
			#insert all the fields into the _dict
			for f in fields:
				setattr( cls, f, None )
		else:
			raise TypeError( "A subclass of simpleORM.Base must have a `_fields` attribute." )

		super( _MetaSimpleDB, cls ).__init__( name, bases, _dict )

	def __new__( cls, name, bases, _dict ):
		instance = super( _MetaSimpleDB, cls ).__new__( cls, name, bases, _dict )

		for f in instance._fields:
			query = "`%s` = '%%s'" % f

			def find_by( self, val, __query=query ):
				return self().where( __query % val  )

			setattr( instance, "find_by_%s" % f, find_by )

			#Add the getters/setters/

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



