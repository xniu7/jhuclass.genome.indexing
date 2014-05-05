#!/bin/bash
# run parallel bucket pointer refinement sorting
if [ $1 = "pbpr" ]; then
	../spark/bin/pyspark pbpr.py spark://$2:7077 s3n://fsda/data/$3/input s3n://fsda/data/$3/output
# run radix sort, segment sort or default sort
else
	../spark/bin/pyspark sort.py $1 spark://$2:7077 s3n://fsda/data/$3/input s3n://fsda/data/$3/output
fi
