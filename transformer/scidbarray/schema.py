from .. import Schema
from itertools import chain, imap, izip

class SciDBSchema(Schema):
  def __init__(self, array, *args, **kwargs):
    super(SciDBSchema, self).__init__(chain(imap(lambda name: (name, 'int64', False), array.dim_names),
    	                                    array.sdbtype.full_rep), 
                                      attributes=array.sdbtype.full_rep, 
                                      dimensions=izip(array.datashape.dim_names, array.datashape.dim_low, 
                                                      array.datashape.dim_high, array.datashape.chunk_size, 
                                                      array.datashape.chunk_overlap))