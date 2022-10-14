import argparse
from config import config
from DataRecorder import DataRecorder

    
def main():
    # Construct Parser
    parser = argparse.ArgumentParser()
    
    # Add arguments to the parser
    parser.add_argument("-c", "--config", metavar="Config File", default="database.ini", dest="CONFIG_FILE", help="Path to the config file")
    
    # Parse Arguments
    args = vars(parser.parse_args())
    
    # Parse CONFIG_FILE
    psql_config = config(args["CONFIG_FILE"], "postgresql")
    db_config = config(args["CONFIG_FILE"], "database")
    socket_config = config(args["CONFIG_FILE"], "socket")
    recorder_config = config(args["CONFIG_FILE"], "recorder")
    
    # Setup DataRecorder
    data_recorder = DataRecorder(psql_config, db_config, socket_config, recorder_config)
    data_recorder.setup_server()
    data_recorder.setup_socket()
    
    # Start Recording
    data_recorder.start_recording()
    
    
if __name__ == '__main__':
    main()
