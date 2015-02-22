from sys import argv
import timeit
import uuid
import scidbpy 
from myria import MyriaConnection
from transformer import Transformable
from transformer.myriarelation import MyriaRelation
from transformer import CSV, SerialCSV, FIFO, SocketCSV, SocketBinary, SocketBoostArchive #, FIFOBinary, Binary, SocketCSVImplicit

def convert(array, strategy, coordinator):
    relation = array.transform(MyriaRelation, strategy, hostname=coordinator, port=8753, ssl=False, \
timeout=9600) 
    relation.waitForCompletion(timeout=9600)
    return relation

def convertBack(relation, strategy):
    array = relation.transform(scidbpy.SciDBArray, strategy, port=9999, hostname='localhost', url='http://{}:8080'.format(coordinator))
    return array

def test(array, strategy, coordinator):
    relation = convert(array, strategy, coordinator)
    print relation.name

if __name__ == '__main__':
    Transformable.mixin(scidbpy.SciDBArray)
    Transformable.mixin(MyriaRelation)

    coordinator = 'localhost'
    exponents = (int(argv[1]), int(argv[1])+1) if len(argv) > 1 else (4,5)
    sizes = [1000000 * 2**x for x in xrange(*exponents)]
    strategies = [SocketBinary] #SocketCSV] #SocketBinary, SocketCSV] #Binary #Binary #SocketCSV #FIFO #SocketBinary

    for size in sizes:
      for strategy in strategies:
        print 'Size: ' + str(size)
        print 'Strategy: ' + str(strategy)

        array = scidbpy.connect('http://{}:8080'.format(coordinator)).random(size, chunk_size=min(size/8,2**16), persistent=False)
        print array.name

        query = ''

        def test():
          global query
          query = convert(array, strategy, coordinator)

        print 'Begin SciDB->Myria'
        print timeit.timeit('test()', setup='from __main__ import test', number=1)

        def testRoundtrip():
            query = convert(array, strategy, coordinator)
            relation = MyriaRelation(query.name)
            newarray = convertBack(relation, strategy)
            #print array.interface.wrap_array(relation.name.replace(':', '_'))

        def testBack():
            convertBack(MyriaRelation(query.name), strategy)

        print 'Begin Myria->SciDB'
        print timeit.timeit('testBack()', setup='from __main__ import testBack', number=1)
  
        print 'Begin SciDB->Myria->SciDB'
        print timeit.timeit('testRoundtrip()', setup='from __main__ import testRoundtrip', number=1)
