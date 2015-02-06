from time import strptime
from dateutil.parser import parse
from itertools import izip
from myria import MyriaConnection
from schema import MyriaSchema

class MyriaRelation(object):
  DefaultConnection = MyriaConnection(hostname='localhost', port=8753)

  def __init__(self, relation, connection=DefaultConnection, *args, **kwargs):
    self.name = relation if isinstance(relation, basestring) else relation.name
    self.components = self._get_name_components(self.name)
    self.connection = connection
    self.qualified_name = self._get_qualified_name(self.components)

  def toJson(self):
    return self.connection.download_dataset(self.qualified_name)

  @property 
  def schema(self):
    return MyriaSchema(json=self._metadata['schema'])

  @property
  def createdDate(self):
    return parse(self._metadata['created'])

  def __len__(self):
    return int(self._metadata['numTuples'])

  @property
  def _metadata(self):
    if 'metadata' not in self.__dict__:
      self.metadata = self.connection.dataset(self.qualified_name)
    return self.metadata

  @staticmethod
  def _get_name(qualified_name):
    return ':'.join([qualified_name['userName'], qualified_name['programName'], qualified_name['relationName']])

  @staticmethod
  def _get_name_components(name):
    components = name.split(':')
    default_components = ['public', 'adhoc'][:max(3 - len(components), 0)]
    return default_components + components[:3]

  @staticmethod
  def _get_qualified_name(name_or_components):
    if isinstance(name_or_components, basestring):
      return MyriaRelation._get_qualified_name(MyriaRelation._get_name_components(name_or_components))
    else:
      return dict(izip(('userName', 'programName', 'relationName'), name_or_components[:3]))
