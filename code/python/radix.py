# transform read to (suffix, gap, preChar)
# if length=5
# ACGT => [(ACGT$,0,$),(CGT$,1,a),(GT$,2,C),(T$,3,G),($,4,T)]
def transform(read,length=71):
    read=read+'$'
    for i in xrange(0, len(read)):
        yield (read[i:],length-len(read)+i, read[i-1:i] if i>0 else '$')

# given a bucket group, yield new tuples
# 4 represent T                 3 represent G
# (4, [(GT,0,C), (TT,0,C)]) => [(3,(G,0,C)),(4,(T,0,C)))]
def bucket_map(element):
    if len(element)!=2: 
        index = 0
        value = [element]
    else : 
        index = element[0]
        value = element[1]
    for (suffix,blank_length,preChar) in value:
        if(blank_length>0):
            blank_length-=1
            index=0
        else:
            if(suffix[-1]=='$'):
                index=0
            elif(suffix[-1]=='A'):
                index=1
            elif(suffix[-1]=='C'):
                index=2
            elif(suffix[-1]=='G'):
                index=3
            elif(suffix[-1]=='T'):
                index=4
            elif(suffix[-1]=='N'):
                index=5  
            suffix = suffix[:-1]
        yield (index,(suffix,blank_length,preChar))

# get pre character from [(suffix, gap, preChar)]
def getPreChar(element):
    value=element[1]
    for (suffix,blank_length,preChar) in value:
        yield preChar

# sort from last char to first char
def radixSort(reads,length=71):
    suffixes = reads.flatMap(transform)
    for i in xrange(0,length):
        suffixes = suffixes.flatMap(bucket_map).groupByKey().sortByKey()
    return suffixes.flatMap(getPreChar)