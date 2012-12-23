import boto

class Connection( object ):

	_connection = boto.connect_sdb()
	_domains = {}

	def __init__( self ):
		doms = self._connection.get_all_domains()

		for d in doms:
			self._domains[d.name] = d

	def get_domain( self, domain ):
		return self._domains[domain]

	def new_item( self, domain ):
		return self._domains[domain].new_item( domain )

	def get_domain_stats( self, domain ):
		pass
