from collections import defaultdict

partitionL=3
readL=70

def radix_pass(strs, ordr, depth):
    """ Given a collection of same-length strings and depth, return a
        permutation that stably sorts the strings according to character
        at that depth """
    buckets = defaultdict(list)
    for i in ordr:
        buckets[strs[i][0][depth]].append(i)
    return [x for sublist in [buckets[c] for c in '$ACGTN'] for x in sublist]

# read => a list of (partition,(suffix, pre char)), the length of suffixs should be fixed.
# ACGT => [(AC, (GT$,$)), (CG, (T$$,A)), (GT, ($$$,C)), (T$, ($$$,G)), ($$, ($$$,T))]
def transform(read):
    for i in xrange(0, len(read)+1):
        partition = read[i:partitionL+i] if (partitionL+i < len(read)) else read[i:]
        suffix = read[partitionL+i:] if (partitionL+i < len(read)) else ''
        suffix += ''.join(['$' for _ in xrange(0,readL-partitionL-len(suffix))])
        
        yield (partition, (suffix, read[i-1:i] if i>0 else '$'))

# sort suffixes in each partition by a lsd radix sort.
def sort((partition, suffixes)):
    lsd = range(len(suffixes))
    for depth in xrange(readL-partitionL-1,-1,-1):
        lsd = radix_pass(suffixes,lsd,depth)
        
    yield ''.join([suffixes[pos][1] for pos in lsd])


def partitionSort(reads):
    suffixes = reads.flatMap(transform)
    partitions = suffixes.groupByKey().sortByKey()
    bwt = partitions.flatMap(sort)
    return bwt
