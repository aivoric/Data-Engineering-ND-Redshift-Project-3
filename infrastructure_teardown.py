from infrastructure import InfrastructureManager
import logging

logging.basicConfig(level=logging.INFO)

def main():
    infrastructure = InfrastructureManager()
        
    logging.info('Starting infrastructure teardown...')
    infrastructure.drop_redshift()
    infrastructure.drop_role_policy()
    infrastructure.drop_role()
    infrastructure.reset_config_file
    logging.info('Finished infrustructure teardown!')

if __name__ == "__main__":
    main()