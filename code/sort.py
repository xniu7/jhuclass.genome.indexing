import sys
from bwt import reverseBwt
from pyspark import SparkConf, SparkContext
from radix import radixSort
from segment import segSort
from default import defaultSort
from partition import partitionSort
from datetime import datetime

# reverse bwt to original reads.
def check(bwt, reads_num):
    bwt = ''.join(bwt.collect())
    print reverseBwt(bwt, reads_num).replace('$','\n')
    
# config spark context, set master, name and memory size
def getSC(master, name):
    conf = (SparkConf()
             .setMaster(master)
             .setAppName(name)
             #.set("spark.executor.memory", "1g")
             )
    sc = SparkContext(conf = conf)
    
    sc.addPyFile('default.py')
    sc.addPyFile('segment.py')
    sc.addPyFile('radix.py')
    sc.addPyFile('partition.py')

    return sc

# select a sort method
def sort(sort_name, reads, cores_num, length=70):
    if (sort_name=='radix'):
        bwt = radixSort(reads)
    elif (sort_name=='segment'):
        bwt = segSort(reads)
    elif (sort_name=='partition'):
        bwt = partitionSort(reads, cores_num)
    else:
        bwt = defaultSort(reads, cores_num)
    return bwt

if __name__ == "__main__":
    if len(sys.argv) < 6:
        print >> sys.stderr, "Usage: <sort> <master> <cores_num> <input> <output>"
        exit(-1)
    start_time = datetime.now()
    
    sc = getSC(sys.argv[2], sys.argv[1]+sys.argv[3]+sys.argv[4])

    # drop lines with '>'
    reads = sc.textFile(sys.argv[4],int(sys.argv[3])).filter(lambda s: '>' not in s)
    # sort suffixes
    bwt = sort(sys.argv[1],reads, int(sys.argv[3]))
    # output bwt
    bwt.saveAsTextFile(sys.argv[5])
    # reverse bwt to reads
    #check(bwt, reads.count())
    
    finish_time = datetime.now()
    print finish_time-start_time
