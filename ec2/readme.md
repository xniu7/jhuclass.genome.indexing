There are three scripts:
 * connect.sh: 
   * launch cluster with 5 slaves: `bash connect.sh launch -tc3.8xlarge -s5` 
   * destroy cluster: `bash connect.sh destroy`
   * stop cluster: `bash connect.sh stop`
   * start cluster: `bash connect.sh start`
 * get.sh:
   * get code from cluster: `bash get.sh ec2://xxx`
 * put.sh
   * upload code to cluster: `bash put.sh ec2://xxx`
