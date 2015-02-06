import inspect, importlib
from collections import defaultdict
from itertools import ifilter
from functools import partial

class ConversionStrategy(object):
  class MetaClass(type):
    def __init__(cls, classname, bases, attributes):
      super(cls.MetaClass, cls).__init__(classname, bases, attributes)
      cls.MetaClass.register(cls)

    def register(handler, type=None):
      for cls in inspect.getmro(handler)[:-2]:
        handler.handlers[cls].add(handler)

  __metaclass__ = MetaClass
  handlers = defaultdict(set)
  type = None

  @classmethod
  def execute(cls, source, target, *args, **kwargs):
    handlers = map(partial(cls.__get_handler, cls), [type(source), target])

    intermediate = handlers[0].export(source, *args, **kwargs)
    return         handlers[1].import_(source, intermediate, *args, **kwargs)

  @classmethod
  def import_(source, intermediate, *args, **kwargs): raise NotImplementedError

  @classmethod
  def export(cls, source): raise NotImplementedError

  @staticmethod
  def __get_handler(key, type, search=True):
    handler = next(ifilter(lambda handler: handler.type == type, 
                   ConversionStrategy.handlers[key]), None)
    if handler is None and search:
      handler = ConversionStrategy.__import_handlers(key, type)
    elif handler is None:
      raise NameError('Unable to find strategy for class ''%s'' (tried importing %s)' % (type.__name__, ConversionStrategy.__import_names(key, type)))
    return handler

  @staticmethod
  def __import_handlers(key, type):
    map(ConversionStrategy.__tryimport, ConversionStrategy.__import_names(key, type))
    return ConversionStrategy.__get_handler(key, type, False)

  @staticmethod
  def __import_names(key, type):
    return map(lambda format: format.format(type.__name__, key.__name__).lower(),
               ['{0}{1}', '{0}.{1}', 'transformer.{0}.{1}'])

  @staticmethod
  def __tryimport(module):
    try: importlib.import_module(module, '..')
    except ImportError: pass