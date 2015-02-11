import threading
import multiprocessing
import random
from collections import namedtuple
import scidbpy
from .. import SocketCSV
from . import SciDBSchema
import utility

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
    interface = scidbpy.connect(kwargs["url"])
    #print "load(create_array({}, {}), '{}@{}', -1, '{}')".format(source.name, source.schema, kwargs["hostname"], kwargs["port"], 'text')
    
    #result = interface.query("create_array({}, {})".format(source.name, SciDBSchema(source.schema).local))
    #print "*****RESULT = " + result
    print "-----"
    print "-----"
    #sprint result
    print "-----"
    print "-----"
    return interface.query("load({}, '{}@{}')"
                          .format(source.name, kwargs["hostname"], kwargs["port"]))