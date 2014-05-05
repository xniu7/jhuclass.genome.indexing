def rotations(t):
    ''' Return list of rotations of input string t '''
    tt = t * 2
    return [ tt[i:i+len(t)] for i in xrange(0, len(t)) ]

def bwm(t):
    ''' Return lexicographically sorted list of t\'s rotations '''
    return sorted(rotations(t))

def bwtViaBwm(t):
    ''' Given T, returns BWT(T) by way of the BWM '''
    return ''.join(map(lambda x: x[-1], bwm(t)))    
    
def firstCol(tots):
    ''' Return map from character to the range of rows prefixed by
        the character. '''
    first = {}
    totc = 0
    for c, count in sorted(tots.iteritems()):
        first[c] = (totc, totc + count)
        totc += count
    return first

def rankBwt(bw):
    ''' Given BWT string bw, return parallel list of B-ranks.  Also
        returns tots: map from character to # times it appears. '''
    tots = dict()
    ranks = []
    for c in bw:
        if c not in tots: tots[c] = 0
        ranks.append(tots[c])
        tots[c] += 1
    return ranks, tots

def reverseBwt(bw,reads_num):
    ''' Make T from BWT(T) '''
    ranks, tots = rankBwt(bw)
    first = firstCol(tots)
    t=''
    for i in xrange(reads_num-1,-1,-1):
        rowi = i # start in first row
        t = '$' + t # start with rightmost character
        while bw[rowi] != '$':
            c = bw[rowi]
            t = c + t # prepend to answer
            # jump to row that starts with c of same rank
            rowi = first[c][0] + ranks[rowi]
    return t

def compareOriBwt(ori_path,bwt_path):
    ''' Given BWT and original reads size, Make T from BWT(T). Then 
        compare T with original T ''' 
    num_lines = sum(1 for line in open(ori_path))
    f_bwt = open(bwt_path,'r')
    for line in f_bwt: ''
    bwt_reverse_t = reverseBwt(line.replace('\n',''), num_lines).replace('$','\n')
    f_ori = open(ori_path, 'r')
    ori_t = ''.join(f_ori)    
    rep = ori_t.replace(ori_t,'')
    if len(rep)==0: print 'Successfully transform!'
    else : print 'T-catransform error.'
    
if __name__=="__main__":
    t = 'abaaba$'
    b = bwtViaBwm(t)
    b = 'TTAAG$TAG$CAGG$'
    reads = reverseBwt(b,3).replace('$','\n')
    compareOriBwt("/Users/niuxiang/Dropbox/documents/classes/fsda/project/data/input","/Users/niuxiang/Dropbox/documents/classes/fsda/project/data/output")
