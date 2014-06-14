from collections import defaultdict

def radix_pass(strs, ordr, depth):
    """ Given a collection of same-length strings and depth, return a
        permutation that stably sorts the strings according to character
        at that depth """
    buckets = defaultdict(list)
    for i in ordr:
        buckets[strs[i][0][depth]].append(i)
    return [x for sublist in [buckets[c] for c in '$ACGNT'] for x in sublist]

# closure
def generate_transform_func(prefix, partition_length):
    # read => a list of (partition,(suffix, pre char)), the length of suffixs should be fixed.
    # ACGT => [(AC, (GT$,$)), (CG, (T$$,A)), (GT, ($$$,C)), (T$, ($$$,G)), ($$, ($$$,T))]
    def transform(read):
        read=read+'$'
        suffix_length = len(read)-len(prefix)-partition_length
        for i in xrange(0, len(read)):
            if len(read)-i<len(prefix) or read[i:i+len(prefix)]!=prefix: continue
            partition = read[len(prefix)+i:len(prefix)+partition_length+i] \
                if (len(prefix)+partition_length+i <= len(read)) else read[len(prefix)+i:]
            
            suffix = read[len(prefix)+partition_length+i:] \
                if (len(prefix)+partition_length+i <= len(read)) else ''
            suffix += '$'*(suffix_length-len(suffix)) if suffix_length-len(suffix)>0 else ''
    
            yield (partition, (suffix, read[i-1] if i>0 else '$'))    
    return transform

# sort suffixes in each partition by a lsd radix sort.
def sort((partition, suffixes)):
    if len(suffixes)==0 or len(suffixes[0])!=2 : return
    lsd = range(len(suffixes))
    suffix_length = len(suffixes[0][0])
    for depth in xrange(suffix_length-1,-1,-1):
        lsd = radix_pass(suffixes,lsd,depth)
        
    yield '\n'.join([suffixes[pos][1] for pos in lsd])

def partitionSort(reads,threads_num,output_path):
    prefixes = ['$','A$','C$','G$','N$','T$','AA','CA','GA','NA','TA','AC','CC','GC','NC','TC','AG','CG','GG','NG','TG','AN','CN','GN','NN','TN','AT','CT','GT','NT','TT']
    #prefixes = '$ACGNT'
    for prefix in prefixes:
        transform = generate_transform_func(prefix,10)
        suffixes = reads.flatMap(transform)
        partitions = suffixes.groupByKey(threads_num)
        if partitions.count()>0:  partitions=partitions.sortByKey(True,threads_num)
        bwt = partitions.flatMap(sort)
        bwt.saveAsTextFile(output_path+'/'+prefix)
    return 0
