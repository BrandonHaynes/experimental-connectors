import threading
import multiprocessing
import random
from collections import namedtuple
import scidbpy
from .. import SocketBinary
from . import SciDBSchema
import utility
import time

class SciDBSocketBinary(SocketBinary):
  type = scidbpy.SciDBArray

  @classmethod
  def export(self, array, *args, **kwargs):
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
    types = '({})'.format(",".join(source.schema.types))
    array.interface.query("save(scan({}), '{}', -1, '{}')"
                          .format(array.name, name, types))

  @classmethod
  def import_(cls, source, intermediate, *args, **kwargs):
    interface = scidbpy.connect(kwargs["url"])
    name = source.name.replace(':', '_')

    types = '({})'.format(",".join(source.schema.types))
    print types

    time.sleep(10)
    return interface.query("load({}, '{}@{}', -1, '{}')"
                          .format(name, kwargs["hostname"], kwargs["port"], types))
