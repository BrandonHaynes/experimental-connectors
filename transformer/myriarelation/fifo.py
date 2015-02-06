import subprocess
import multiprocessing
import pipes
from functools import partial
from itertools import imap, ifilter, repeat
from urlparse import urlparse
import utility
from myria import MyriaConnection
from . import MyriaRelation, MyriaQuery, MyriaSchema
from .. import FIFO

class MyriaFIFO(FIFO):
  type = MyriaRelation

  @classmethod
  def import_(cls, source, intermediate, *args, **kwargs):
    connection = MyriaConnection(*args, **kwargs) if args or kwargs else MyriaRelation.DefaultConnection
    schema = MyriaSchema(intermediate.schema).local
    workers = [(id, urlparse('//' + name).hostname) for (id, name) \
                 in connection.workers().items()]

    print list(_assign_uris(intermediate.uris, workers))

    #TODO Need to expose identity and username, if it's still needed for cross-cluster transfers
    identity, username = None, None

    pool = multiprocessing.Pool(processes=len(intermediate.uris))
    pipes = pool.map(partial(_scatter_uri, identity, username), 
                     _assign_uris(intermediate.uris, workers))
    work = ((id, { "dataType": "URI", "uri": uri }) for (id, uri) in pipes if uri)
    plan = utility.get_plan(schema, work, MyriaRelation._get_qualified_name(source.name))

    return MyriaQuery.submit_plan(plan, connection)

def _assign_uris(uris, workers):
  uris = map(urlparse, uris)
  assignment = {worker[1]: {'value': worker, 'uris': []} for worker in workers}
  
  # Assign work to the same node as workers, where possible
  for uri in ifilter(lambda uri: uri.netloc in workers, uris):
    assignment[uri.netloc]['uris'].append(uri)

  # Assign remaining uris to the worker with the least work
  for uri in ifilter(lambda uri: not uri.netloc in workers, uris):
    # Should be using a heap here
    min(assignment.values(), key=lambda worker: len(worker['uris']))['uris'].append(uri)

  return ((uri.geturl(), worker['value']) \
             for worker in assignment.values() 
             for uri in worker['uris'])

def _scatter_uri(identity, username, (source, (id, host))):
  if source is None:  # No work to do for this worker
    return id, None
  else:
    source = urlparse(source)
    destination = urlparse('//' + host + utility._get_filename(), scheme='file')

    _create_destination_pipe(identity, username, destination)
    _connect_pipe(identity, username, source, destination)

    return id, destination.geturl()

def _create_destination_pipe(identity, username, destination):
  command = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ForwardAgent=yes'] + \
            (['-i', identity] if identity else []) + \
            ['{}{}'.format(username + '@' if username else '', destination.hostname), 
             'mkfifo', destination.path]
  subprocess.check_call(command)

def _connect_pipe(identity, username, source, destination):
  # 1) SSH to source host
  # 2) On source host, cat source pipe to {SSH connection to destination host}
  # 3) On destination host, cat stdin to destination pipe
  command = ['ssh', '-C', '-o', 'StrictHostKeyChecking=no', '-o', 'ForwardAgent=yes', 
               source.hostname,
                 'cat {source_path} | ssh -C -o StrictHostKeyChecking=no -o ForwardAgent=yes {username}{hostname} \
                    "cat > {destination_path}"'.format(source_path=pipes.quote(source.path), 
                                                       hostname=pipes.quote(destination.hostname), 
                                                       destination_path=pipes.quote(destination.path),
                                                       username=username + '@' if username else '')]
  return subprocess.Popen(command)

