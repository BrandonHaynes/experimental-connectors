import types
from formats import CSV

default_strategy = CSV

def transform(source, target, strategy=default_strategy, *args, **kwargs):
  return strategy.execute(source, target, *args, **kwargs)


class Transformable(): # Don't inherit from object to allow runtime mixins
  def transform(self, target, strategy=default_strategy, *args, **kwargs):
    return transform(self, target, *args, **kwargs)

  @classmethod
  def mixin(cls, base):
  	if not hasattr(base, 'transform'):
  		setattr(base, 'transform', transform)
  	else:
  		raise AttributeError("Class {} has an existing attribute 'transform' and cannot be mixed (use ConversionStrategy.execute(base, target, ...) directly)".format(base))
