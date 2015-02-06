from urlparse import urlparse
from myria import MyriaConnection
from . import MyriaRelation, MyriaQuery, MyriaSchema, utility
from .. import SerialCSV

class MyriaSerialCSV(SerialCSV):
  type = MyriaRelation

  @classmethod
  def import_(cls, source, intermediate, *args, **kwargs):
    connection = MyriaConnection(*args, **kwargs) if args or kwargs else MyriaRelation.DefaultConnection
    schema = MyriaSchema(intermediate.schema).local

    uri = utility._copy_local(urlparse(intermediate.uris[0]))

    with open(uri.path) as descriptor:
      descriptor.readline()
      response = connection.upload_fp(MyriaRelation._get_qualified_name(kwargs.get('name', source.name)),
                                      MyriaSchema(intermediate.schema).local,
                                      descriptor)

    return MyriaQuery(response['queryId'], connection, *args, **kwargs)
