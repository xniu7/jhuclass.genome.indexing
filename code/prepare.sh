# create amazon credentials
echo export AWS_SECRET_ACCESS_KEY=****** >> /root/spark/conf/spark-env.sh
echo export AWS_ACCESS_KEY_ID=****** >> /root/spark/conf/spark-env.sh

# synchronize code and enviroment among master and slaves
/root/spark-ec2/copy-dir /root/code/
/root/spark-ec2/copy-dir /root/spark/conf/spark-env.sh

# get spark url
cat /root/spark-ec2/cluster-url
