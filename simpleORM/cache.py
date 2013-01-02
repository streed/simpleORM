import time

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

	def put( self, key, obj, replace=False ):

		if self._cur_size >= self._max_size:
			entry = self._choose()
			
			del self._collection[entry]

		if replace:
			if key in self._collection:
				self._cur_size += 1

			self._collection[key] = { "data": obj, "timestamp": time.time(), "accesses": 0 }
		else:
			if key in self._collection:
				raise CacheKeyConflictError( "The Key `%s` already exists in the cache collection" % key )
			else:
				self._collection[key] = { "data": obj, "timestamp": time.time(), "accesses": 0 }
				self._cur_size += 1

	def get( self, key ):
		obj = self._collection.get( key, None )

		if obj:
			obj["timestamp"] = time.time()
			obj["accesses"] += 1
			return obj["data"]
		else:
			return None

	def _choose( self ):
		pass

	def _replace( self, key, obj ):
		pass

class LRUCache( Cache ):

	def __init__( self, max_size=100, max_age=60000 ):
		Cache.__init__( self, max_size=max_size, max_age=max_age )


	def _choose( self ):
		now = time.time()

		temp = sorted( [ ( k, now - v["timestamp"] ) for k, v in self._collection.iteritems() ], key=lambda v: v[1], reverse=True )

		return temp[0][0]


class MRUCache( Cache ):

	def __init__( self, max_size=100, max_age=60000 ):
		Cache.__init__( self, max_size=max_size, max_age=max_age )


	def _choose( self ):
		now = time.time()

		temp = sorted( [ ( k, now - v["timestamp"] ) for k, v in self._collection.iteritems() ], key=lambda v: v[1] )

		return temp[0][0]

class LFUCache( Cache ):

	def __init__( self, max_size=100, max_age=60000 ):
		Cache.__init__( self, max_size=max_size, max_age=max_age )

	def _choose( self ):
		temp = sorted( [ ( k, v["accesses"] ) for k, v in self._collection.iteritems() ], key=lambda v: v[1] )

		return temp[0][0]
