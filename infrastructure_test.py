from infrastructure import InfrastructureManager
import logging

logging.basicConfig(level=logging.INFO)

def main():
    infrastructure = InfrastructureManager
    
    logging.info('Starting infrastructure creation...')
    infrastructure.create_role()
    infrastructure.attach_policy_to_role()
    infrastructure.create_redshift_cluster()
    infrastructure.open_incoming_tcp_port()
    infrastructure.update_config_file()
    logging.info('Finished infrustructure creation!')
    
    logging.info('Starting infrastructure teardown...')
    infrastructure.drop_redshift()
    infrastructure.drop_role_policy()
    infrastructure.drop_role()
    infrastructure.reset_config_file
    logging.info('Finished infrustructure teardown!')

if __name__ == "__main__":
    main()