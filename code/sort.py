import sys
from bwt import reverseBwt
from pyspark import SparkConf, SparkContext
from radix import radixSort
from segment import segSort
from default import defaultSort
from datetime import datetime

# reverse bwt to original reads.
def check(suffixes, reads_num):
    output = suffixes.collect()
    bwt = ''.join(output)
    print reverseBwt(bwt, reads_num).replace('$','\n')
    
# config spark context, set master, name and memory size
def getSC(master, name):
    conf = (SparkConf()
             .setMaster(master)
             .setAppName(name)
             #.set("spark.executor.memory", "1g")
             )
    return SparkContext(conf = conf)

# select a sort method
def sort(sort_name, reads, length=70):
    if (sort_name=='radix'):
        suffixes = radixSort(reads)
    elif (sort_name=='segment'):
        suffixes = segSort(reads)
    else:
        suffixes = defaultSort(reads)
    return suffixes

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print >> sys.stderr, "Usage: <sort> <master> <input> <output>"
        exit(-1)
    start_time = datetime.now()
    
    sc = getSC(sys.argv[2], sys.argv[1]+sys.argv[3])

    # drop lines with '>'
    reads = sc.textFile(sys.argv[3]).filter(lambda s: '>' not in s)
    # sort suffixes
    suffixes = sort(sys.argv[1],reads)
    # output bwt
    suffixes.saveAsTextFile(sys.argv[4])
    # reverse bwt to reads
    #check(suffixes, reads.count())
    
    finish_time = datetime.now()
    print finish_time-start_time
