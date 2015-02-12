import scidbpy 
from .. import Schema
from itertools import chain, imap, izip, repeat

class SciDBSchema(Schema):
  def __init__(self, source, *args, **kwargs):
    if isinstance(source, scidbpy.SciDBArray):
      super(SciDBSchema, self).__init__(chain(imap(lambda name: (name, 'int64', False), source.dim_names),
                                              source.sdbtype.full_rep), 
                                          attributes=source.sdbtype.full_rep, 
                                          dimensions=izip(source.datashape.dim_names, source.datashape.dim_low, 
                                                          source.datashape.dim_high, source.datashape.chunk_size, 
                                                          source.datashape.chunk_overlap))
    else:
      super(SciDBSchema, self).__init__(source.default)

  @property
  def local(self):
    chunk_size = 2**24
    overlap = 0
    print '---'
    print zip(self.names, self.types)
    print '---'
    attributes = map(':'.join, zip(self.names, self.types))
    dimensions= 'i=0:*'

    return '<{attributes}> [{dimensions},{chunk_size},{overlap}]'.format(attributes=','.join(attributes), 
                                                                         dimensions=dimensions,
                                                                         chunk_size=chunk_size,
                                                                         overlap=overlap)
