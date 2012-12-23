
class Builder( object ):

	_hooks = []

	def __init__( self, obj ):
		self._sql = ""
		self._obj = obj

		self._query = { 
			"select": "*",
			"where": "'1' = '1'",
			"order": "",
			"limit": "200"
		}
	

	def select( self, outputs ):
		"""
			PRE: The outputs is a tuple containing the columns to select from the domain
			POST: This object is returned.
		"""
		_format = "`%s`," * len( outputs )
		self._query["select"] = _format[0:-1] % outputs

		return self


	def where( self, where ):
		"""
			PRE: The where clause is written out and is a string.
			POST: This object is returned.
		"""
		self._query["where"] = "%s" % where

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
		self._query["limit"] = "%d" % lim

		return self

	def _fetch( self ):
		return self._obj._execute( self.to_sql() )


	def to_sql( self ):
		q = self._query
		return "select %s from `%s` where %s %s limit %s" % ( q["select"], self._obj._domain, q["where"], q["order"], q["limit"] )


	def __enter__( self ):
		self.__apply_pre_hooks()
		return self._fetch()


	def __exit__( self, _type, value, traceback ):
		return self.__apply_post_hooks()

	def __apply_pre_hooks( self ):
		pass

	
	def __apply_post_hooks( self ):
		return True


