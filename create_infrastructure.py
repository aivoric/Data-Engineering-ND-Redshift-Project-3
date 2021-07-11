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

def create_role():
    try:
        print("Creating role...")
        dwhRole = iam.create_role(
            Path='/'
            , RoleName=DWH_ROLE_NAME
            , Description="A test IAM role"
            , AssumeRolePolicyDocument = json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole'
                    , 'Effect': 'Allow'
                    , 'Principal': {'Service': 'redshift.amazonaws.com'}}]
                    , 'Version': '2012-10-17'  
            })
        )
        return dwhRole

    except Exception as e:
        print(e)
        
def attach_policy_to_role():
    print("Attaching policy to role...")
    response = iam.attach_role_policy(RoleName=DWH_ROLE_NAME
        , PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    print(f"Status code: {response_code}")

def get_role_arn():
    roleArn = iam.get_role(RoleName=DWH_ROLE_NAME)['Role']['Arn']
    return roleArn
    
def create_redshift_cluster():
    try:
        print("Creating redshift cluster...")
        response = redshift.create_cluster(        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            DBName=DWH_DB,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            Port=int(DWH_PORT),
            IamRoles=[get_role_arn()]
        )
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        print(f"Status code: {response_code}")
    except Exception as e:
        print(e)
        
def get_redshift_cluster_status():
    props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def open_incoming_tcp_port():
    try:
        print("Opening incoming TCP port...")
        props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        vpc = ec2.Vpc(id=props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(f"DefaultSg: {defaultSg}")
        
        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,  
            CidrIp='0.0.0.0/0', 
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
        
def update_config_file():
    try:
        props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        config.set('CLUSTER', 'DWH_HOST', props['Endpoint']['Address'])
        config.set('ROLE', 'DWH_ROLE_ARN', props['IamRoles'][0]['IamRoleArn'])

        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
    except Exception as e:
        print(e)
        pass

if __name__ == "__main__":
    create_role()
    attach_policy_to_role()
    create_redshift_cluster()
    cluster_status = get_redshift_cluster_status()
    print(cluster_status)
    open_incoming_tcp_port()
    update_config_file()
    print('Infrastructure Ready...')