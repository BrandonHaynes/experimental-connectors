class Schema(object):
  def __init__(self, default, *args, **kwargs):
    print default
    default = list(default)
    names, types, nullable = zip(*default)

    self.default = default
    self.names = names
    self.types = types
    self.nullable = nullable
    self.attributes = kwargs
    self.attributes['args'] = args

  @property
  def type_map(self):
      return {'int64':  'LONG_TYPE', 
              'int32':  'INT_TYPE', 
              'uint64': 'LONG_TYPE', 
              'uint32': 'INT_TYPE', 
              'float':  'FLOAT_TYPE',
              'double': 'DOUBLE_TYPE'} #TODO: What is Myria's story for nans?

  @property
  def _reverse_type_map(self):
      return {'LONG_TYPE': 'int64', 
              'INT_TYPE': 'int32', 
              'FLOAT_TYPE': 'float',
              'DOUBLE_TYPE': 'double'}
