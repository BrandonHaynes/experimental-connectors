from collections import namedtuple
import scidbpy
from .. import SerialCSV
from . import SciDBSchema
import utility

class SciDBSerialCSV(SerialCSV):
  type = scidbpy.SciDBArray

  @classmethod
  def export(cls, array, *args, **kwargs):
    #TODO
    filename = utility._get_filename() #TODO path='/home/bhaynes/scidb')
    #filename = utility._get_filename(path=kwargs.pop('path', None))
    workers = utility._get_workers(array.interface)
    uris = utility._get_worker_uris(workers[:1], filename)

    import time
    start = time.time()

    array.interface.query("save(scan({}), '{}', -2, '{}')"
                            .format(array.name, filename, 'csv+'))

    print time.time() - start

    return namedtuple('Intermediate', ['source', 'uris', 'schema'])(
                      array, uris, SciDBSchema(array))