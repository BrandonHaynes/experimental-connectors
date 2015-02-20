import subprocess
import socket
import tempfile
from urlparse import urlparse
from functools import partial

def get_plan(schema, work, relation, query='', scan_type='FileScan'):
   return \
      { "fragments": map(partial(get_fragment, [0], schema, relation, scan_type), work),
        "logicalRa": query, 
        "rawQuery":  query }

def get_export_plan(schema, workers, port, relation, csvFormat, query='', scan_type='DbQueryScan', sync_type='SocketDataOutput', littleEndian=True):
  # workers is a list of ids
  return \
      { "fragments": map(partial(get_export_fragment, [0], schema, relation, scan_type, port, sync_type, csvFormat, littleEndian), workers),
        "logicalRa": query, 
        "rawQuery":  query }

def get_export_fragment(taskid, schema, relation, scan_type, port, sync_type, csvFormat, littleEndian, workerid):
  return { "overrideWorkers": [workerid],
            "operators":[
               {
                  "opId": __increment(taskid),
                  "opType": scan_type, # Look in operators to determine type for reading from db

                  "schema": schema.local,
                  "sql": 'SELECT * FROM "{}"'.format(relation.name),
               },
               {
                  "argChild": taskid[0]-1,

                  "opId": __increment(taskid),
                  "opType": sync_type,

                  "port": port,
                  "csvFormat": csvFormat,
                  "isLittleEndian": littleEndian,
                  "numDims": 1
                }
             ]
          }

def get_fragment(taskid, schema, relation, scan_type, (worker, datasource)):
  return { "overrideWorkers": [worker],
            "operators":[
               {
                  "opId": __increment(taskid),
                  "opType": scan_type,

                  "schema": schema,
                  "source": datasource,
                  "skip": 1
               },
               {
                  "argChild": taskid[0]-1,
                  "argOverwriteTable": True,

                  "opId": __increment(taskid),
                  "opType": "DbInsert",

                  "relationKey": relation
                }
             ]
          }

def _copy_local(uri):
  if uri.hostname != socket.gethostname():
    filename = _get_filename()
    source = '{}:{}'.format(uri.hostname, uri.path)
    command = ['scp', '-C', '-B', '-q', '-o', 'StrictHostKeyChecking=no', '-o', 'ForwardAgent=yes', 
               source, filename]
    subprocess.check_call(command)
    return urlparse('//' + socket.gethostname() + filename, scheme='file')
  else:
    return uri

def _get_filename():
    # TODO This doesn't guarantee a usable name on a remote system...
    with tempfile.NamedTemporaryFile(suffix='.fifo', delete=True) as file:
      return file.name

def __increment(id):
  id[0] += 1
  return id[0] - 1
