
class CacheKeyConflictError( Exception ):
	pass

class Cache( object ):

	def __init__( self, max_size=100, max_age=60000 ):
		self._max_size = max_size
		self._max_age = max_age

		self._collection = {}
		self._cur_size = 0
		self._hits = 0
		self._misses = 0

	@property
	def size( self ):
		return self._cur_size

	def put( self, key, obj ):
		pass

	def get( self, key ):
		pass

	def _choose( self ):
		pass

	def _replace( self, key, obj ):
		pass

#TODO: Make sure to add in the timestamp of the obj put/last get
class LRUCache( Cache ):

	def __init__( self, max_size=100, max_age=60000 ):
		Cache.__init__( self, max_size=max_size, max_age=max_age )

	def put( self, key, obj, replace=False ):

		if self._cur_size > self._max_size:
			entry = self._choose( self )
			
			del self._collection[entry]

		if replace:
			if key in self._collection:
				self._cur_size += 1

			self._collection[key] = obj
		else:
			if key in self._collection:
				raise CacheKeyConflictError( "The Key `%s` already exists in the cache collection" % key )
			else:
				self._collection[key] = obj
				self._cur_size += 1


	def get( self, key ):
		return self._collection.get( key, None )

	def _choose( self ):
		print self._collection.items()

