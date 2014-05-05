# read => a list of (suffix, pre char)
# ACGT => [(ACGT$,$), (CGT$,A), (GT$,C), (T$,G), ($,T)]
def transform(read):
    suffix = []
    read=read+'$'
    for i in xrange(0, len(read)):
        yield (read[i:],read[i-1:i] if i>0 else '$')

# sort (suffix, preChar) tuples by suffix
def defaultSort(reads):
    suffixes = reads.flatMap(transform)
    suffixes = suffixes.sortByKey().map(lambda (k,v):v)
    return suffixes