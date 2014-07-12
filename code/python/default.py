# read => a list of (suffix, pre char)
# ACGT => [(ACGT$,$), (CGT$,A), (GT$,C), (T$,G), ($,T)]
def transform(read):
    read=read+'$'
    for i in xrange(0, len(read)):
        yield (read[i:],read[i-1:i] if i>0 else '$')

# closure
def generate_transform_func(prefix):
    def transform(read):
        read=read+'$'
        for i in xrange(0, len(read)):
            if len(read)-i<len(prefix) or read[i:i+len(prefix)]!=prefix: continue
            yield (read[i:],read[i-1:i] if i>0 else '$')
    return transform

# sort (suffix, preChar) tuples by suffix
def defaultSort(reads, threads_num, output_path, prefixes):
    for prefix in prefixes:
        if '$' == prefix:
            bwt = reads.map(lambda read:read[-1])
        else:
            transform = generate_transform_func(prefix)
            suffixes = reads.flatMap(transform)
            suffixes = suffixes.sortByKey(True,threads_num)
            bwt = suffixes.map(lambda (k,v):v)            
        bwt.saveAsTextFile(output_path+'/'+prefix)
    return 0
