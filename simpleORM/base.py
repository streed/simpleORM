from simpleORM.connection import Connection
from simpleORM.builder import Builder
from simpleORM.column import IndexColumn, RawColumn, ColumnNoneDefinedError
from simpleORM.row import RowConverter

from uuid import uuid4

class _MetaSimpleDB( type ):


	"""
		This will add all the find_by_* methods to the class, for those attributes found in the _fields attribute.
	"""
	def __new__( cls, name, bases, _dict ):
		temp = {}
		_has_columns = False
		for f in _dict:
			if isinstance( _dict[f], IndexColumn ):
				temp["index"] = f

			if isinstance( _dict[f], RawColumn ):
				_has_columns = True
				query = "`%s` = '%%s'" % _dict[f].name

				def find_by( self, val, __query=query ):
					return self._execute( self.where( __query % val  ).to_sql() )

				temp["find_by_%s" % _dict[f].name] = find_by

		if not _has_columns and name != "Base":
			raise ColumnNoneDefinedError( "%s has no defined columns" % ( name ) )

		_dict.update( temp )

		instance = super( _MetaSimpleDB, cls ).__new__( cls, name, bases, _dict )

		return instance


class Base( object ):
	"""
		Base model should be subclassed.

		This model will perform the required actions to add in the `find_by_<column name>` methods.
		
		These models are tied to a specific simpleDB domain by defining a `_domain` class instance
		to be the domain name. The class instance of the `_connection` will be used to query on this
		domain.

		To create a new object model for use in with the SimpleDB the following model is used.
		
		from simpleORM.column import IndexColumn, StringColumn, IntColumn
		class TestObject( Base ):
			name = IndexColumn( StringColumn( "name" ) )
			age = IndexColumn( "age" )

			_domain = "test_domain"


		The above class will have the following methods added to the object on creation.

			test.find_by_name()
			test.find_by_age()

		These methods will create the following query strings:

			select * from `test_domain` where `name` = "<passed parameter>" limit 200
			select * from `test_domain` where `age` = "<passed parameter>" limit 200

		These queries will be sent to the `_execute` method and the result set is returned as
		a `RowConverter`.
	"""
	#Let's perform some magic to add in some extra methods.
	#This will also pull in the domain object so that this class can operate.
	__metaclass__ = _MetaSimpleDB

	_connection = Connection()

	_domain = ""

	#Default consistency is "evententually_consistent" = False
	#Override this to get change the behavior
	_consistency = False


	def __init__( self ):
		self._item = self._connection.new_item( self._domain )
		self._deleted = False
		self._id = str( uuid4() )
		self._converter = RowConverter( self )

	def __hash__( self ):
		return hash( self._id )

	def __equal__( self, other ):

		equal = True

		for f in self._fields:
			equal = equal and ( getattr( self, f ) == getattr( other, f ) )

		return equal


	@classmethod
	def create_domain( cls ):
		"""
			This method will create the domain if it does not exist for the current `_connection`.
		"""
		dom = cls._connection.create_domain( cls._domain )

		if dom:
			cls._connection._domains[dom.name] = dom
			return True
		else:
			return False


	def delete( self ):
		"""
			This will delete the current row that this object points to.
		"""
		if not self._deleted and self._connection.get_domain( self._domain ).delete_item( self._item ):
			self._deleted = True

		return self._deleted

	def __call__( self ):
		return Builder( self )


	def _execute( self, query ):
		"""
			This will execute a Builder's sql query and pass it through the current `_converter`.
		"""
		self._item = self._converter._make_converter( self._connection.get_domain( self._domain ).select( query, consistent_read=self._consistency ) )

		return self._item

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

