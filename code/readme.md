There are two types of scripts
 * .py
   * sort.py: main program of parallel sorting, containing three sorting methods (radix sort, segment sort and default sort). Ex: `pyspark sort.py radix/segmet/default local input output`
     * default.py: default sort method
     * radix.py: radix sort method
     * segment.py: segment sort method
   * pbpr.py: main program of parallel bucket pointer refinement. Ex: `pyspark pbpr.py local input output`
   * bwt.py: a reverse process of bwt to reads (not parallel) based on https://raw.githubusercontent.com/xniu7/comp-genomics-class/master/notebooks/CG_BWT_Reverse.ipynb
 * .sh
   * prepare.sh: after uploading all codes to clusters, run this script to config amazon credentials and synchronize files among master and slaves. Ex. `bash prepare.sh`
   * test.sh: test program on a ec2 cluster. Ex. `bash test.sh radix/segment/default/pbpr ec2://xxxx 100`
