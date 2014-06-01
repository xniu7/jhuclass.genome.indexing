import boto.ec2
import sys

def delete(region,group_names):
    conn = boto.ec2.connect_to_region(region)
    groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
    print "Deleting security groups " + str(group_names)
    # Delete individual rules to remove dependencies
    for group in groups:
        for rule in group.rules:
            for grant in rule.grants:
                group.revoke(ip_protocol=rule.ip_protocol, from_port=rule.from_port, to_port=rule.to_port, src_group=grant)
    #Deleting group
    for group in groups:
        conn.delete_security_group(group.name)

if __name__=="__main__":
    if len(sys.argv) < 3:
        # region:{'us-east-1', 'us-west-1', 'us-west-2'}
        # group_names: test_master,test_slaves
        print >> sys.stderr, "Usage: <region> <group_names>"
        exit(-1)
    region = sys.argv[1]
    group_names = sys.argv[2].split(',')
    delete(region,group_names)