import sys
from pyspark import SparkConf, SparkContext

import re

from bwt import reverseBwt
from radix import radixSort
from segment import segSort
from default import defaultSort
from partition import partitionSort
    
# config spark context, set master, name and memory size
def getSC(master, name):
    conf = (SparkConf()
             .setMaster(master)
             .setAppName(name)
             #.set("spark.executor.memory", "1g")
             .set("spark.akka.frameSize", "512")
             )
    sc = SparkContext(conf = conf)
    
    sc.addPyFile('default.py')
    sc.addPyFile('segment.py')
    sc.addPyFile('radix.py')
    sc.addPyFile('partition.py')
    sc.addPyFile('bwt.py')

    return sc

# select a sort method
def sort(sort_name, reads, threads_number, output_path, prefixes):
    if (sort_name=='radix'):
        bwt = radixSort(reads)
    elif (sort_name=='segment'):
        bwt = segSort(reads)
    elif (sort_name=='partition'):
        bwt = partitionSort(reads, threads_number, output_path, prefixes)
    else:
        bwt = defaultSort(reads, threads_number, output_path, prefixes)
    return bwt

# RDD does not support communications among lines, 
# because each line is independent during processing.
# Thus we first collect RDD (RDD->List), then parallelize List (List->RDD)
def collectReads(lines, file_type):
    if file_type == 'fasta' :
        reads = []
        read = ''
        lines = lines.collect()
        #concatinate lines begin with '>'
        for line in lines : 
            if '>' not in line:
                read += line
            else :
                if len(read)>0: reads.append(read) 
                read = ''
        if len(read)>0: reads.append(read) 
    elif file_type == 'fastq' :
        #choose the second line of every four lines
        reads = lines.collect()[1::4]
    else :
        reads = lines.collect()
    return reads
    
def filterReads(lines, file_type):
    if file_type == 'fasta' :
        reads = lines.filter(lambda line: '>' not in line)
    elif file_type == 'fastq' :
        reads = lines.filter(lambda line: re.match('^[ACGTN]*$', line))
    else :
        reads = lines
    return reads
    
def getReads(lines, file_type, collect, reads_output_path):
    if collect:
        # collect RDD (RDD->List)
        reads = collectReads(lines,file_type)
        # parallelize List (List->RDD)
        reads = sc.parallelize(reads,int(threads_number))
    else :
        reads = filterReads(lines,file_type)
        
    # output reads
    # reads.saveAsTextFile(reads_output_path)
    return reads

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print >> sys.stderr, "Usage: <sort> <master> <threads_num> <file_type> <input> <bwt_output_path>"
        exit(-1)
            
    sort_method = sys.argv[1]
    master_address = sys.argv[2]
    threads_number = sys.argv[3]
    file_type = sys.argv[4]
    input_path = sys.argv[5]
    reads_output_path = ""
    bwt_output_path = sys.argv[6]
    
    sc = getSC(master_address, sort_method+threads_number+input_path)

    lines = sc.textFile(input_path,int(threads_number))

    reads = getReads(lines,file_type, False, reads_output_path).cache()
    
    prefixes='$ACGNT'
    #prefixes = ['$','AA','CA','GA','NA','TA','AC','CC','GC','NC','TC','AG','CG','GG','NG','TG','AN','CN','GN','NN','TN','AT','CT','GT','NT','TT']
    # sort suffixes
    bwt = sort(sort_method,reads, int(threads_number), bwt_output_path, prefixes)
