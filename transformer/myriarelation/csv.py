import subprocess
import multiprocessing
from itertools import imap, izip, cycle
from functools import partial
from urlparse import urlparse, ParseResult
from myria import MyriaConnection
from .. import CSV
from . import MyriaRelation, MyriaQuery, MyriaSchema, utility

class MyriaCSV(CSV):
  type = MyriaRelation

  @classmethod
  def import_(cls, source, intermediate, *args, **kwargs):
    connection = MyriaConnection(*args, **kwargs) if args or kwargs else MyriaRelation.DefaultConnection
    schema = MyriaSchema(intermediate.schema).local
    pool = multiprocessing.Pool(processes=len(intermediate.uris))
    workers = [(id, urlparse('//' + name).hostname) for (id, name) \
                 in connection.workers().items()]

    #TODO Need to expose identity and username, if it's still needed for cross-cluster transfers
    identity, username = None, None

    #TODO Need to adjust when |myria| > |other|, since we may not be able to argOverwriteTable=True    
    pipes = pool.map(partial(_scatter_uri, identity, username), izip(intermediate.uris, cycle(workers)))
    work = ((id, { "dataType": "URI", "uri": uri }) for (id, uri) in pipes if uri)
    plan = utility.get_plan(schema, work, MyriaRelation._get_qualified_name(source.name))

    return MyriaQuery.submit_plan(plan, connection, timeout=kwargs.get('timeout', 60))

def _scatter_uri(identity, username, (source, (id, host))):
  if source is None:  # No work to do for this worker
    return id, None
  else:
    uri = urlparse(source)
    source = '{}:{}'.format(uri.netloc, uri.path)
    filename = utility._get_filename() if uri.hostname != host else uri.path

    command = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ForwardAgent=yes'] + \
              (['-i', identity] if identity else []) + \
              ['{}{}'.format(username + '@' if username else '', uri.netloc), 
               'scp -C -B -q -o StrictHostKeyChecking=no -o ForwardAgent=yes {} {}:{}'.format(uri.path, host, filename)]

    subprocess.check_call(command) if uri.hostname != host else 0

    return id, ParseResult(scheme='file', netloc=host, path=filename,
                           params='', query='', fragment='').geturl()
