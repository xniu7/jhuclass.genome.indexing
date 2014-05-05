# encode each segment by base6
def str2ints(suffix,base=6,length=100,seg=10):
    keys = []
    for i in xrange(0, length/seg if length%seg==0 else length/seg +1):
        keys.append(0)
        for j in xrange(0, seg):
            if len(suffix)< i*seg+j+1: break
            if suffix[i*seg+j]=='A':
                keys[i] += 1*base**(seg-j-1)
            elif suffix[i*seg+j]=='C':
                keys[i] += 2*base**(seg-j-1)
            elif suffix[i*seg+j]=='G':
                keys[i] += 3*base**(seg-j-1)
            elif suffix[i*seg+j]=='T':
                keys[i] += 4*base**(seg-j-1)
            elif suffix[i*seg+j]=='N':
                keys[i] += 5*base**(seg-j-1)
    return keys

# transform read to base6 encoded suffix
# AAAAAAAAACAAAAAAAAAG => (60466172,60466173)
# transform encoded suffix to another form, (n number,(n-1 number, ... (fist number, pre char)))
# (60466172,60466173) =>(60466173,(60466172,$))
def transform(read,seg=10):
    read=read+'$'
    for i in xrange(0, len(read)):
        keys = str2ints(read[i:])
        value = read[i-1:i] if i>0 else '$'
        for j in xrange(0,seg):
            value = (keys[j],value)
        yield value

# sort suffix from last number to first one
def segSort(reads, seg=10):
    suffixes = reads.flatMap(transform)
    for i in xrange(0,seg) :
        suffixes = suffixes.sortByKey().map(lambda (k,v):v)
    return suffixes