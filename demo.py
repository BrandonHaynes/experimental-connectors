import uuid
import scidbpy 
from myria import MyriaConnection
from transformer import Transformable
from transformer.myriarelation import MyriaRelation
from transformer import CSV, SerialCSV, FIFO, SocketCSV

if __name__ == '__main__':
    Transformable.mixin(scidbpy.SciDBArray)
    Transformable.mixin(MyriaRelation)

    #array = scidbpy.connect('http://localhost:8080').randint((10, 10))

    relation = MyriaRelation("Test")
    relation.transform(scidbpy.SciDBArray, SocketCSV, hostname='localhost', port=9999, url=None)

    # TODO:
    # Can we automate "ssh-agent && ssh-add ~/.ssh/uwdb_ec2.rsa"?
    # Had to explicitly copy ~/.ssh/id_rsa to ec2 /home/bhaynes/.ssh

    #MyriaRelation.DefaultConnection =  MyriaConnection(hostname='localhost', port=8753)

    # Stars cluster -> Stars cluster

    #array = scidbpy.connect('http://vega.cs.washington.edu:8080').wrap_array('waveform_signal_subtable')
    ##relation = array.transform(MyriaRelation, FIFO, hostname='rest.myria.cs.washington.edu', port=1776, ssl=True, timeout=960)

    # Stars cluster -> EC2
    #ec2_hostname = 'ec2-54-145-132-143.compute-1.amazonaws.com'
    #array = scidbpy.connect('http://vega.cs.washington.edu:8080').randint((10, 10))
    #relation = array.transform(MyriaRelation, FIFO, hostname=ec2_hostname, port=8753, ssl=False, timeout=480)

    ##relation.waitForCompletion(timeout=2000)
    ##print relation.name
    #print relation.toJson()
