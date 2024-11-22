import logging

def setup_logger():
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.DEBUG
    )
    return logging.getLogger("MigrationLogger")
