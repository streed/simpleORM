from simpleORM.column import RawColumn

class RowConverter( object ):

	def __init__( self, _self ):
		self._columns = {}

		for k in dir( _self ):
			attr = getattr( _self, k )
			if isinstance( attr, RawColumn ):
				self._columns[k] = attr.convert


	def _make_converter( self, results ):
		for row in results:
			yield self._convert_row( row )


	def _convert_row( self, row ):
		ret = {}	

		for k, v in row.iteritems():
			ret[k] = self._columns[k]( v )

		return ret
		
