
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
			query = "select * from `%s` where `%s` = '%%s'" % ( instance._domain, f )

			def find_by( self, val, __query=query ):
				return self._execute( __query % val )

			setattr( instance, "find_by_%s" % f, find_by )

		return instance

class Base( object ):

	#Let's perform some magic to add in some extra methods.
	#This will also pull in the domain object so that this class can operate.
	__metaclass__ = _MetaSimpleDB


	_fields = []
	_domain = ""


	def __init__( self ):
		self._query = { 
			"select": "select * from",
			"where": " '1' = '1'",
			"order": "",
			"limit": "limit 200"
		}

		pass

	
	def select( self, outputs ):
		"""
			PRE: The outputs is a tuple containing the columns to select from the domain
			POST: This object is returned.
		"""
		_format = "select %s" % ( "`%s`," * len( outputs ) )
		self._query["select"] = _format[0:-1] % outputs

		return self


	def where( self, where ):
		"""
			PRE: The where clause is written out and is a string.
			POST: This object is returned.
		"""
		self._query["where"] = "where %s" % where

		return self


	def order( self, column, asc=True ):
		"""
			PRE: The order in which to return the data.
			POST: This object is returned.
		"""
		if asc:
			self._query["order"] = "order by %s asc" % column
		else:
			self._query["order"] = "order by %s desc" % column

		return self


	def limit( self, lim ):
		"""
			PRE: The lim is such that 0 < lim <= 2500
			POST: This object is returned.
		"""
		self._query["limit"] = "limit %d" % lim

		return self

	def delete( self ):
		"""
			PRE: This will delete the row this object manages.
			POST: If the item is deleted this will return True else False
		"""
		pass

	def _builder( self ):
		q = self._query

		return "%s from `%s` %s %s %s" % ( q["select"], self._domain, q["where"], q["order"], q["limit"] )

	def _execute( self, query ):
		"""
			PRE: This is called to execute a query directly.
			POST: An iterator is returned from this that will return the data.
		"""
		return query
