from collections import namedtuple
import scidbpy
from .. import CSV
from . import SciDBSchema
import utility

class SciDBCSV(CSV):
  type = scidbpy.SciDBArray

  @classmethod
  def export(cls, array, *args, **kwargs):
    filename = utility._get_filename() #path='/home/bhaynes/scidb') #TODO
    workers = utility._get_workers(array.interface)
    uris = utility._get_worker_uris(workers, filename)

    array.interface.query("save(scan({}), '{}', -1, '{}')"
                          .format(array.name, filename, 'csv+'))

    return namedtuple('Intermediate', ['source', 'uris', 'schema'])(
                      array, uris, SciDBSchema(array))
