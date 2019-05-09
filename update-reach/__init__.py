import logging

import azure.functions as func
from src.reach_tools import *
from src import config

def main(msg: func.QueueMessage) -> None:

    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))
