import scidbpy 
from .. import Schema
from itertools import chain, imap, izip, repeat

class SciDBSchema(Schema):
  def __init__(self, source, *args, **kwargs):
    if isinstance(source, scidbpy.SciDBArray):
      super(SciDBSchema, self).__init__(chain(imap(lambda name: (name, 'int64', False), array.dim_names),
                                              array.sdbtype.full_rep), 
                                          attributes=array.sdbtype.full_rep, 
                                          dimensions=izip(array.datashape.dim_names, array.datashape.dim_low, 
                                                          array.datashape.dim_high, array.datashape.chunk_size, 
                                                          array.datashape.chunk_overlap))
    else:
      super(SciDBSchema, self).__init__(source.default)

  @property
  def local(self):
    if any(self.nullable):
        raise ArgumentException('Nullable attribute {} not supported'.format(name))

    return {'columnNames': self.names,
            'columnTypes': self.types }
