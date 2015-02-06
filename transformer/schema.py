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