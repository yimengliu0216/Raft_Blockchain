# Code is based on this website
# https://www.bogotobogo.com/python/python_network_programming_server_client.php
# run_server.py
# This file mainly deal listening to network and keyboard, and take care of
# timeout event.
import socket
import datetime
import time
import sys
import threading
from server_config import CONFIG as server_config
from servers.server import Server, ZeroMQServer
#from states.follower import Follower
from blocks.block import BlockChain
from boards.memory_board import MemoryBoard
import logging

from states.characters import Follower

if __name__ == '__main__':
    server_id = int(sys.argv[1])
    logging.basicConfig(filename='server_'+str(server_id)+'.log', level=logging.DEBUG)
    observers = [Server('localhost\t'+str(server_config[i]), Follower(),
                        BlockChain(), MemoryBoard(), [], True)
                 for i in server_config if i!=server_id]
    #for this_item in observers:
    #    this_item._state.set_server(this_item)
    ZeroMQServer('localhost\t'+ str(server_config[server_id]), Follower(),
                 BlockChain(), MemoryBoard(), observers)
    #ZeroMQServer(this_server)
    while True:
        time.sleep(10)
