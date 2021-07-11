import configparser
import boto3
import json
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                         = config.get('AWS', 'KEY')
SECRET                      = config.get('AWS', 'SECRET')
REGION                      = config.get('AWS', 'REGION')

DWH_ROLE_NAME               = config.get('ROLE', 'DWH_ROLE_NAME')
DWH_CLUSTER_TYPE            = config.get('CLUSTER', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES               = config.get('CLUSTER', 'DWH_NUM_NODES')
DWH_NODE_TYPE               = config.get('CLUSTER', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER      = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB                      = config.get('CLUSTER', 'DWH_DB')
DWH_DB_USER                 = config.get('CLUSTER', 'DWH_DB_USER')
DWH_DB_PASSWORD             = config.get('CLUSTER', 'DWH_DB_PASSWORD')
DWH_PORT                    = config.get('CLUSTER', 'DWH_PORT')

ec2 = boto3.resource('ec2',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
)

s3 = boto3.client('s3',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
)

iam = boto3.client('iam',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
)

redshift = boto3.client('redshift',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET
)

def teardown_infrastructure():
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)
        pass
    
    try:
        iam.detach_role_policy(RoleName=DWH_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except Exception as e:
        print(e)
        pass
    
    try:
        iam.delete_role(RoleName=DWH_ROLE_NAME)
    except Exception as e:
        print(e)
        pass
    
def get_redshift_cluster_status():
    try:
        props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    except Exception as e:
        print(e)
        return "Cluster doesn't exist."
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def update_config_file():
    config.set('CLUSTER', 'DWH_HOST', 'not_specified')
    config.set('ROLE', 'DWH_ROLE_ARN', 'not_specified')

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

if __name__ == "__main__":
    teardown_infrastructure()
    update_config_file()
    cluster_status = get_redshift_cluster_status()
    print(cluster_status)
    print('Infrastructure Removed...')