# Code is based on this website
# https://www.bogotobogo.com/python/python_network_programming_client_client.php
# run_client.py
# This file mainly deal listening to network and keyboard, and take care of
# timeout event.
import socket
import datetime
import time
import sys
import threading
from client_config import CONFIG as client_config
from clients.client import Client, ZeroMQclient
#from states.follower import Follower
from boards.memory_board import MemoryBoard

from states.characters import Follower

if __name__ == '__main__':
    client_id = sys.argv[1]
    ZeroMQclient('localhost\t'+ str(client_config[client_id]), client_id,
                 MemoryBoard())
    while True:
        time.sleep(10)
