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
def sort(sort_name, reads, cores_num):
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
        print >> sys.stderr, "Usage: <sort> <master> <threads_num> <input> <output>"
        exit(-1)
    start_time = datetime.now()
    
    sort_method = sys.argv[1]
    master_address = sys.argv[2]
    threads_number = sys.argv[3]
    input_path = sys.argv[4]
    output_path = sys.argv[5]
    
    sc = getSC(master_address, sort_method+threads_number+input_path)

    # drop lines with '>'
    reads = sc.textFile(input_path,int(threads_number)).filter(lambda s: '>' not in s)
    # sort suffixes
    bwt = sort(sort_method,reads, int(threads_number))
    # output bwt
    bwt.saveAsTextFile(output_path)
    # reverse bwt to reads
    #check(bwt, reads.count())
    
    finish_time = datetime.now()
    print finish_time-start_time
