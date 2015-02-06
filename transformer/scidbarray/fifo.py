import subprocess
import multiprocessing
import pipes
import threading
from collections import namedtuple
from itertools import izip, repeat
import scidbpy
import utility
from . import SciDBSchema
from .. import FIFO

def _create_pipe((pipename, worker)):
  command = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ForwardAgent=yes', 
               worker['hostname'], 'mkfifo -m 666 {}'.format(pipes.quote(pipename))]
  subprocess.check_call(command)

class SciDBFIFO(FIFO):
  type = scidbpy.SciDBArray

  @classmethod
  def export(self, array, *args, **kwargs):
    pipename = utility._get_filename() #path='/home/bhaynes/scidb') #TODO
    workers = utility._get_workers(array.interface)
    uris = utility._get_worker_uris(workers, pipename)
    pool = multiprocessing.Pool(processes=len(workers))

    pool.map(_create_pipe, izip(repeat(pipename), workers))
    threading.Thread(target=self._write_pipes, args = (array, pipename)).start()
    
    return namedtuple('Intermediate', ['source', 'uris', 'schema'])(
                      array, uris, SciDBSchema(array))

  @staticmethod
  def _write_pipes(array, pipename):
    array.interface.query("save(scan({}), '{}', -1, '{}')"
                            .format(array.name, pipename, 'csv+'))