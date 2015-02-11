from itertools import repeat
from .. import Schema

class MyriaSchema(Schema):
  def __init__(self, *args, **kwargs):
    if 'schema' in kwargs:
      super(MyriaSchema, self).__init__(schema.default, 
                                        schema.attributes['args'] + args, 
                                        **dict(schema.attributes.items() + kwargs.items()))
    elif 'json' in kwargs:
      super(MyriaSchema, self).__init__(zip(kwargs['json']['columnNames'], 
                                            map(lambda t: self._reverse_type_map[t], 
                                                kwargs['json']['columnTypes']), 
                                            repeat(False)), 
                                        args, **kwargs)
    else:
      super(MyriaSchema, self).__init__([])

  @property
  def local(self):
    if any(self.nullable):
        raise ArgumentException('Nullable attribute {} not supported'.format(name))

    return {'columnNames': self.names,
            'columnTypes': [self.type_map[type] for type in self.types] }

  @property
  def type_map(self):
      return {'int64':  'LONG_TYPE', 
              'int32':  'INT_TYPE', 
              'uint64': 'LONG_TYPE', 
              'uint32': 'INT_TYPE', 
              'float':  'FLOAT_TYPE',
              'string': 'STRING_TYPE',
              'double': 'DOUBLE_TYPE'} #TODO: What is Myria's story for nans?

  @property
  def _reverse_type_map(self):
      return {'LONG_TYPE': 'int64', 
              'INT_TYPE': 'int32', 
              'FLOAT_TYPE': 'float',
              'DOUBLE_TYPE': 'double',
              'STRING_TYPE': 'string'}
