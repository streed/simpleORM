from collections import namedtuple

class DomainMock( object ):
	pass

class SimpleDBMockClass( object ):
	def __init__( self, dict_items ):
		T = namedtuple( "T", [ "name", "select", "put_attributes", "delete_item" ] )
		def fake_select( query, consistent_read=False ):
			return dict_items
		self._domains = { "test_domain": T( name="test_domain", select=fake_select, put_attributes=lambda a, b: None, delete_item=lambda a: None ) }

	def get_all_domains( self ):
		return [ i[1] for i in self._domains.items() ]

	def get_domain( self, dom ):
		return self._domains[dom]
