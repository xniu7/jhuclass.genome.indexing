import sys
from bwt import reverseBwt
from pyspark import SparkConf, SparkContext
from collections import defaultdict
from datetime import datetime

# initial state of BPR, parallel workers can communicate by these variables
BPR = {'noBucket':False,'bucketNum':0,'updatedBprNum':0,'num_reads':0}

# update suffix array by bucket pointer
def updateSA((indexes,bpr),bucketL = 4,read_length=71):
    sameBprLength = 0
    if len(indexes)!=1:
        min = bpr - len(indexes)
        # a list of indexes with same bpr
        sameBprs = defaultdict(list)
        for index in indexes:
            if index+bucketL not in BPR.keys() : offsetBpr=sys.maxint
            else : offsetBpr = BPR[index+bucketL]
            # sameBprs is a {bpr1:[index1,index2], bpr2:[index3]}
            sameBprs[offsetBpr].append(index)
        keylist = sameBprs.keys()
        keylist.sort()
        for key in keylist:
            # allocate new bucket pointer to a new list of indexes
            yield (sameBprs[key],min+len(sameBprs[key]))
            min += len(sameBprs[key])
    # if only one index, do not need to split bucket
    else:
        yield (indexes,bpr)   

# update bucket pointer by suffix array
def updateBpr((indexes,bpr),read_length=71):
    # total length of reads
    length = read_length*BPR['num_reads']
    if(len(indexes)!=1):
        # record how many bucket is needed to be splitted
        BPR['bucketNum'] += 1
    for index in indexes:
        BPR[index] = bpr
    # record the how many indexes' bpr has been updated
    BPR['updatedBprNum'] += len(indexes)
    # if all indexes' bpr has been updated, output new bpr
    if BPR['updatedBprNum']==length :
        # if no bucket is needed to be splitted, finish recursion
        if BPR['bucketNum']==0: BPR['noBucket']=True
        # reset
        BPR['updatedBprNum']=0
        BPR['bucketNum']=0
        yield BPR    

# transform (id, read) to [(suffix1, pos1), (suffix2, pos2), ...]
def transform((line_id,read), bucketL = 4, length=71):
    for i in xrange(0, len(read)-bucketL+1):
        yield (read[i:i+bucketL], line_id*length+i)
    for i in xrange(len(read)-bucketL+1, len(read)):
        yield (read[i:]+''.join('$' for _ in xrange(0,i-(len(read)-bucketL))), line_id*length+i)

# config spark context, set master, name and memory size
def getSC(master, name):
    conf = (SparkConf()
             .setMaster(master)
             .setAppName(name)
             #.set("spark.executor.memory", "1g")
             )
    return SparkContext(conf = conf)

# each read has a id (id, read)
def getReads(sc, filePath, read_length=70):
    reads = sc.textFile(sys.argv[2]).filter(lambda s: ('>' not in s) and len(s)==read_length).map(lambda s: s+'$')
    readsC = reads.collect()
    readsID = [(line_id,readsC[line_id]) for line_id in xrange(0,len(readsC))]
    reads = sc.parallelize(readsID)
    return reads

# SA is a list of tuples, each tuple contains an index and a bucket pointer
def initSA(reads):
    SA=[]
    bpr=0
    
    suffixes = reads.flatMap(transform)
    buckets = suffixes.groupByKey().sortByKey()
    output = buckets.collect()
    
    for (comPre,indexes) in output:
        for i in xrange(0,len(indexes)):
            bpr+=1
        SA.append((indexes,bpr-1))
    
    return sc.parallelize(SA)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: sort <master> <input> <output>"
        exit(-1)
    start_time = datetime.now()
    
    sc =  getSC(sys.argv[1], 'pbpr'+sys.argv[2])
    
    reads = getReads(sc,sys.argv[2])
    BPR['num_reads'] = reads.count()
    readsC = reads.collect()

    SAsc = initSA(reads)
    
    # recursively update bucket pointer and suffix array
    while(True):
        BPR = SAsc.flatMap(updateBpr).collect()[0]
        SAsc = SAsc.flatMap(updateSA)
        if(BPR['noBucket']==True): break
    SA = SAsc.collect()
    
    bwt = ''.join(readsC[indexes[0]/71][1][indexes[0]%71-1] for indexes, pos in SA )    

    sc.parallelize(bwt).saveAsTextFile(sys.argv[3])
    
    # reverse bwt to reads
    #print reverseBwt(bwt, BPR['num_reads']).replace('$','\n')
    
    finish_time = datetime.now()
    print finish_time-start_time
    
