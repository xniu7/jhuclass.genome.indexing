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
def sort(sort_name, reads, threads_number):
    if (sort_name=='radix'):
        bwt = radixSort(reads)
    elif (sort_name=='segment'):
        bwt = segSort(reads)
    elif (sort_name=='partition'):
        bwt = partitionSort(reads, threads_number)
    else:
        bwt = defaultSort(reads, threads_number)
    return bwt

# RDD does not support communications among lines, 
# because each line is independent during processing.
# Thus we first collect RDD (RDD->List), then parallelize List (List->RDD)
def getReads(lines, file_type):
    #concatinate lines begin with '>'
    if file_type == 'fasta' :
        # drop lines with '>'
        #reads = lines.filter(lambda line: '>' not in line)
        reads = []
        read = ''
        lines = lines.collect()
        for line in lines : 
            if '>' not in line:
                read += line
            else :
                if len(read)>0: reads.append(read) 
                read = ''
        if len(read)>0: reads.append(read) 
    #choose the second line of every four lines            
    elif file_type == 'fastq' :
        # drop lines with '@' '+'
        reads = lines.collect()[1::4]
    else :
        reads = lines.collect()
    return reads

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print >> sys.stderr, "Usage: <sort> <master> <threads_num> <file_type> <input> <output>"
        exit(-1)
    start_time = datetime.now()
    
    sort_method = sys.argv[1]
    master_address = sys.argv[2]
    threads_number = sys.argv[3]
    file_type = sys.argv[4]
    input_path = sys.argv[5]
    output_path = sys.argv[6]
    
    sc = getSC(master_address, sort_method+threads_number+input_path)

    lines = sc.textFile(input_path,int(threads_number))
    # collect RDD (RDD->List)
    reads = getReads(lines,file_type)
    # parallelize List (List->RDD)
    reads = sc.parallelize(reads,int(threads_number))
    # sort suffixes
    bwt = sort(sort_method,reads, int(threads_number))
    # output bwt
    bwt.saveAsTextFile(output_path)
    # reverse bwt to reads
    check(bwt, reads.count())
    
    finish_time = datetime.now()
    print finish_time-start_time
