import threading
import multiprocessing
import random
from collections import namedtuple
from functools import partial
import scidbpy
from .. import SocketCSV
from . import SciDBSchema
import utility
import time

class SciDBSocketCSV(SocketCSV):
  type = scidbpy.SciDBArray

  @classmethod
  def export(self, array, *args, **kwargs):
    #port = 8081
    #destination = 'socket:localhost:{}'.format(port)
    workers = utility._get_workers(array.interface)
    ports = [random.randint(50000, 60000) for _ in workers]
    pool = multiprocessing.Pool(processes=len(workers))
    uris = utility._get_worker_uris(workers, None, scheme='tcp', 
                                    include_path=False, ports=ports)

    threading.Thread(target=self._write_pipes, args = (array, uris)).start()
    
    return namedtuple('Intermediate', ['source', 'uris', 'schema'])(
                      array, uris, SciDBSchema(array))

  @staticmethod
  def _write_pipes(array, uris):
    name = 'socket:' + ','.join(uris)
    array.interface.query("save(scan({}), '{}', -1, '{}')"
                          .format(array.name, name, 'csv+'))

  @classmethod
  def import_(cls, source, intermediate, *args, **kwargs):
    interface = scidbpy.connect(kwargs['url'])
    workers = utility._get_workers(interface)
    pool = multiprocessing.Pool(processes=len(workers))
    name = source.name.replace(':', '_')
    schema = SciDBSchema(source.schema).local

    try:
      result = interface.query("create_array({}, {})".format(name, schema))
    except Exception:
      pass

    pool.map(partial(_load, name, schema, kwargs['url'], 
                            kwargs['hostname'], kwargs['port']), workers)
    pool.close()
    pool.join()

    #map(lambda w: interface.query('reshape({}_{}, {})'.format(name, w['id'], schema.replace('*', '9999999')), workers)
    #vstack = reduce('concat({}, {})'.format, ('{}_{}'.format(name, w['id']) for w in workers))
    #interface.query('store({}, {})'.format(name, vstack))

def _load(name, schema, url, hostname, port, worker):
  interface = scidbpy.connect(url)
  local_name = '{}_{}'.format(name, worker['id'])

  try:
    interface.query('create temp array {}{}'.format(local_name, schema))
  except Exception:
    pass
  return interface.query("load({}, '{}@{}', {}, 'text')"
                           .format(local_name, hostname, port, worker['id']))
