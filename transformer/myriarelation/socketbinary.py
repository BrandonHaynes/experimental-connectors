import subprocess
import multiprocessing
from itertools import imap, izip, cycle
from functools import partial
from urlparse import urlparse, ParseResult
from myria import MyriaConnection
from .. import SocketBinary
from . import MyriaRelation, MyriaQuery, MyriaSchema, utility
from collections import namedtuple

class MyriaSocketBinary(SocketBinary):
  type = MyriaRelation

  @classmethod
  def import_(cls, source, intermediate, *args, **kwargs):
    connection = MyriaConnection(*args, **kwargs) if args or kwargs else MyriaRelation.DefaultConnection
    schema = MyriaSchema(schema=intermediate.schema).local
    #pool = multiprocessing.Pool(processes=len(intermediate.uris))
    workers = [(id, urlparse('//' + name).hostname) for (id, name) \
                 in connection.workers().items()]

    #print intermediate.uris
    work = ((id, { "dataType": "Socket", "hostname": urlparse(uri).hostname, "port": urlparse(uri).port }) \
             for ((id, name), uri) \
             in izip(workers, intermediate.uris))

    plan = utility.get_plan(schema, work, MyriaRelation._get_qualified_name(source.name), scan_type='ImplicitDimensionBinaryFileScan')

    #TODO
    dimensions = map(lambda d: d[2]+1, intermediate.schema.attributes['dimensions'])
    for fragment in plan['fragments']:
      del fragment['operators'][0]['skip']
      fragment['operators'][0]['isLittleEndian'] = True
      fragment['operators'][0]['dimensions'] = dimensions #[2,3]

    #print plan
    return MyriaQuery.submit_plan(plan, connection, timeout=kwargs.get('timeout', 60))

  @classmethod
  def export(self, relation, *args, **kwargs):
    schema = relation.schema
    port = kwargs["port"]

    plan = utility.get_export_plan(schema, relation.connection.workers(), port, relation, False)
    #print plan
    MyriaQuery.submit_plan(plan, relation.connection, timeout=kwargs.get('timeout', 60))
    return namedtuple('Intermediate', [])()
