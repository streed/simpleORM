from collections import namedtuple
from simpleORM.column import RawColumn, IndexColumn

class RowConverter( object ):
	"""
		RowConverter will allow make sure that the returned data will be in the proper data type
		for use in other parts of the application.

		This class is passed the current object with `Column` objects. When this class is
		instantiated it will loop through the current object's dictionary and save the 
		name and the `_convert` method from each of the columns.

		When the `_make_converter` method is called it will yeild through the current results after
		attempting to convert each of the values to the proper column data type.

		If the `_convert_row` fails to convert the proper exception is thrown and the conversion
		process is stopped. 

		The return value from the `_convert_row` will be a NamedTuple that will allow for simple
		attribute access to each of the corrosponding column data.
	"""
	def __init__( self, _self ):
		self._columns = {}
		self._index = None

		for k in dir( _self ):
			attr = getattr( _self, k )

			if isinstance( attr, IndexColumn ):
				self._index = k

			if isinstance( attr, RawColumn ):
				self._columns[k] = attr.convert

		if self._index:
			keys = self._columns.keys() + [ "index" ]
			self._tuple = namedtuple( _self.__class__.__name__, keys, rename=True )
		else:
			self._tuple = namedtuple( _self.__class__.__name__, self._columns.keys(), rename=True )


	def _make_converter( self, results ):
		for row in results:
			yield self._convert_row( row )


	def _convert_row( self, row ):
		ret = {}	

		if self._index:
			ret["index"] = self._columns[self._index]( row[self._index] )

		for k, v in row.iteritems():
			ret[k] = self._columns[k]( v )

		return self._tuple( **ret )
		
