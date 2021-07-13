import configparser
import boto3
import json
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)

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

class InfrastructureManager():
    """
    1. Creates infrastructure (AWS role, Redshift cluster)
    2. Tears down infrastructure
    3. Updates config file based on infrastructure status
    4. Contains various helper methods to get infrastructure information
    """
    def __init__(self):
        self.ec2 = boto3.resource('ec2', region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

        self.s3 = boto3.client('s3', region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

        self.iam = boto3.client('iam', region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

        self.redshift = boto3.client('redshift', region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
        
        self.cluster_status = self.set_cluster_status()
        self.cluster_arn = ""
        self.cluster_host = ""
        
    def set_cluster_status(self):
        """
        Updates the current status of the Redshift cluster.
        """
        try:
            cluster_status = self.redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
            self.cluster_status = cluster_status
        except Exception as e:
            logging.warning("Redshift Cluster doesn't exist or has been deleted.")
            self.cluster_status = "No Cluster"
        
    def print_cluster_information(self):
        """
        Prints all the main information about the cluster to the console.
        """
        try:
            props = self.redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", 
                        "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
            x = [(k, v) for k,v in props.items() if k in keysToShow]
            print(pd.DataFrame(data=x, columns=["Key", "Value"]))
        except Exception as e:
            logging.warning("Redshift Cluster doesn't exist.")
            
    def get_role_arn(self):
        """
        Returns the Arn of the created AWS IAM Role.
        """
        roleArn = self.iam.get_role(RoleName=DWH_ROLE_NAME)['Role']['Arn']
        return roleArn

    def create_role(self):
        """
        Creates an IAM Role.
        """
        try:
            logging.info('Creating role...')
            self.iam.create_role(
                RoleName=DWH_ROLE_NAME
                , Description="Role for accessing S3 from Redshift."
                , AssumeRolePolicyDocument = json.dumps({
                    'Statement': [{
                        'Action': 'sts:AssumeRole'
                        , 'Effect': 'Allow'
                        , 'Principal': {'Service': 'redshift.amazonaws.com'}}]
                        , 'Version': '2012-10-17'  
                })
            )
            logging.info('Role created!')

        except Exception as e:
            logging.warning(f"Failed to create a role.")
            
    def attach_policy_to_role(self):
        """
        Attaches a S3ReadOnly policy to the created IAM role.
        """
        try:
            logging.info("Attaching policy to role...")
            response = self.iam.attach_role_policy(RoleName=DWH_ROLE_NAME
                , PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            if response_code == 200: logging.info('Policy attached!')
        except Exception as e:
            logging.warning(f"Failed to attach policy to role.")
            
    def create_redshift_cluster(self):
        logging.info(f"Cluster status: {self.cluster_status}")
        if self.cluster_status == 'available':
            logging.info("Redshift cluster already exists. Skipping creation.")
            return
            
        try:
            logging.info("Creating redshift cluster...")
            response = self.redshift.create_cluster(        
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                DBName=DWH_DB,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                Port=int(DWH_PORT),
                IamRoles=[self.get_role_arn()]
            )
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            if response_code == 200: logging.info('Cluster creation started...')
            
            while self.cluster_status != 'available':
                time.sleep(30)
                self.set_cluster_status()
                logging.info(f"Cluster status: {self.cluster_status}. Waiting...")
            
            logging.info('Cluster created! Cluster info:')
            self.print_cluster_information()
            
        except Exception as e:
            logging.warning(f"Failed to start cluster creation.")
    
    def open_incoming_tcp_port(self):
        """
        Opens Redshift to the entire world!
        """
        try:
            logging.info("Opening incoming TCP port...")
            props = self.redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            vpc = self.ec2.Vpc(id=props['VpcId'])
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
            logging.warning(f"{e}")
    
    def update_config_file(self):
        """
        Updates the dwh.cfg file with Redshift host and Role Arn information.
        """
        try:
            logging.info("Updating config file with cluster settings...")
            props = self.redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            
            self.cluster_host = props['Endpoint']['Address']
            self.cluster_arn = props['IamRoles'][0]['IamRoleArn']
            
            config.set('CLUSTER', 'DWH_HOST', self.cluster_host)
            config.set('ROLE', 'DWH_ROLE_ARN', self.cluster_arn)

            with open('dwh.cfg', 'w') as configfile:
                config.write(configfile)
            logging.info("Config file has been updated!")
        except Exception as e:
            logging.warning(f"{e}")

    def reset_config_file(self):
        """
        Resets dwh.cfg file to remove the Redshift host and Role Arn information.
        """
        try:
            logging.info("Resettinc config file...")
            
            config.set('CLUSTER', 'DWH_HOST', 'Not specified')
            config.set('ROLE', 'DWH_ROLE_ARN', 'Not specified')

            with open('dwh.cfg', 'w') as configfile:
                config.write(configfile)
            logging.info("Config file has been updated!")
        except Exception as e:
            logging.warning(f"{e}")
            
    def drop_redshift(self):
        """
        Tearsdown Redshift cluster.
        """
        try:
            logging.info("Deleting redshift cluster...")
            self.redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  
                SkipFinalClusterSnapshot=True)
            
            while self.cluster_status != 'No Cluster':
                time.sleep(30)
                self.set_cluster_status()
                logging.info(f"Cluster status: {self.cluster_status}. Waiting...")
            
            logging.info('Cluster deleted!')
            
        except Exception as e:
            logging.warning(f"Failed to start cluster deletion.")
            
    def drop_role_policy(self):
        """
        Removes the S3 Read policy from the IAM Role.
        """
        try:
            logging.info("Detaching policy from role...")
            self.iam.detach_role_policy(RoleName=DWH_ROLE_NAME, 
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
            logging.info("Policy detached!")
        except Exception as e:
            logging.warning(f"Failed to start cluster deletion.")
            
    def drop_role(self):     
        """
        Removes the IAM Role.
        """   
        try:
            logging.info("Deleting role...")
            self.iam.delete_role(RoleName=DWH_ROLE_NAME)
            logging.info("Role deleted!")
        except Exception as e:
            logging.warning(f"Failed to delete role.")

def main():
    infrastructure = InfrastructureManager()

if __name__ == "__main__":
    main()