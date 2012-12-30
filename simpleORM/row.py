from simpleORM.column import RawColumn, IndexColumn

class RowConverter( object ):

	def __init__( self, _self ):
		self._columns = {}
		self._index = None

		for k in dir( _self ):
			attr = getattr( _self, k )

			if isinstance( attr, IndexColumn ):
				self._index = k

			if isinstance( attr, RawColumn ):
				self._columns[k] = attr.convert


	def _make_converter( self, results ):
		for row in results:
			yield self._convert_row( row )


	def _convert_row( self, row ):
		ret = {}	

		if self._index:
			ret["index"] = self._columns[self._index]( row[self._index] )

		for k, v in row.iteritems():
			ret[k] = self._columns[k]( v )

		return ret
		
