from .. import Schema
from itertools import chain, imap, izip

class SciDBSchema(Schema):
  def __init__(self, *args, **kwargs):
    if 'array' in kwargs:
      super(SciDBSchema, self).__init__(chain(imap(lambda name: (name, 'int64', False), array.dim_names),
                                              array.sdbtype.full_rep), 
                                          attributes=array.sdbtype.full_rep, 
                                          dimensions=izip(array.datashape.dim_names, array.datashape.dim_low, 
                                                          array.datashape.dim_high, array.datashape.chunk_size, 
                                                          array.datashape.chunk_overlap))
    elif 'schema' in kwargs:
      super(SciDBSchema, self).__init__(zip(schema.local['json']['columnNames'], 
                                            schema.local['json']['columnTypes'], 
                                            repeat(False)), 
                                        args, **kwargs)
    else:
      super(SciDBSchema, self).__init__([])

  @property
  def local(self):
    if any(self.nullable):
        raise ArgumentException('Nullable attribute {} not supported'.format(name))

    return {'columnNames': self.names,
            'columnTypes': [self.type_map[type] for type in self.types] }
