from infrastructure import InfrastructureManager
import logging

logging.basicConfig(level=logging.INFO)

def main():
    infrastructure = InfrastructureManager()
    
    logging.info('Starting infrastructure creation...')
    infrastructure.create_role()
    infrastructure.attach_policy_to_role()
    infrastructure.create_redshift_cluster()
    infrastructure.open_incoming_tcp_port()
    infrastructure.update_config_file()
    logging.info('Finished infrustructure creation!')
    
if __name__ == "__main__":
    main()