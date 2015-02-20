import tempfile
import time
import os
from urlparse import ParseResult
from itertools import izip, izip_longest

def _get_filename(path=tempfile.gettempdir()):
  return os.path.join(path, 'tmp' + str(time.time()))

  # TODO This doesn't guarantee a usable name on a remote system...
  with tempfile.NamedTemporaryFile(suffix='.fifo', delete=True, dir=path) as file:
    #print file.name
    return file.name

def _get_workers(connection):
  keys = ('hostname', 'port', 'id', 'created', 'path')

  #TODO OMG OMG Worst thing ever!
  if 'vega.cs.washington.edu' in connection.hostname:
    hosts = ['vega.cs.washington.edu', 'arcturus.cs.washington.edu', 'aldebaran.cs.washington.edu', 'altair.cs.washington.edu', 'betelgeuse.cs.washington.edu', 'rigel.cs.washington.edu', 'spica.cs.washington.edu', 'castor.cs.washington.edu', 'pollux.cs.washington.edu', 'polaris.cs.washington.edu']
    path = '/disk1/scidb_data_test/00{0}/{0}'
    return [{'hostname': host, 'path': path.format(id) } for id, host in enumerate(hosts)]
  else:
    return [dict(izip(keys, values)) for values in connection.afl.list("'instances'").toarray()]

def _get_worker_uris(workers, filename, scheme='file', include_path=True, ports=None):
  return [ParseResult(scheme=scheme, 
                      netloc=worker['hostname'] + (':' + str(port) if port else ''), 
                     path=os.path.join(worker['path'] if include_path else '', 
                                       filename or ''),
                     params='', query='', fragment='').geturl()
            for worker, port in izip_longest(workers, ports or [])]
