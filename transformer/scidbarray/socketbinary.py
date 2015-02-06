import threading
import multiprocessing
import random
from collections import namedtuple
import scidbpy
from .. import SocketBinary
from . import SciDBSchema
import utility

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
    array.interface.query("save(scan({}), '{}', -1, '{}')"
                          .format(array.name, name, '(double)'))
