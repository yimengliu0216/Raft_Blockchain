import zmq
import threading
import time
import pickle
import socket
import logging
from blocks.block import BlockChain, Block

class Server(object):

    def __init__(self, name, state, log, messageBoard, neighbors, is_neighbor=False):
        self._ip_addr, self._port = name.split('\t')
        self._port = int(self._port)
        self._name = name
        self._state = state
        self._log = log
        self._messageBoard = messageBoard
        self._neighbors = neighbors
        self._is_neighbor = is_neighbor
        self.logger = logging.getLogger()
        #self._id = id_number
        self._num_trans_in_block = 2
        self.init_cur_trans = []
        self.added_odd_trans = False
        self.init_block_chain()

        self._total_nodes = len(neighbors) + 1

        self._commitIndex = max(-1, self._log.size()-1)
        self._currentTerm = 0
        self.currentTermChanged = False

        self._lastApplied = 0

        self._lastLogIndex = max(-1, self._log.size()-1)
        self._lastLogTerm = -1

        self._state.set_server(self)
        self._messageBoard.set_owner(self)
        self._log.set_commitIndex(self._commitIndex)

    def send_message(self, message):
        #for n in self._neighbors:
        #    message._receiver = n._full_address
        #message._receiver = self._full_address
        self.post_message(message)

    def send_message_response(self, message):
        raise Exception('send_message_response function is abondoned, try use \
                        send_message instead')
        n = [n for n in self._neighbors if n._full_address == message.receiver]
        if(len(n) > 0):
            n[0].post_message(message)

    def post_message(self, message):
        self._messageBoard.post_message(message)
        #self._messageBoard.print_board()

    def on_message(self, message):
        state, response = self._state.on_message(message)

        self._state = state

    def init_block_chain(self):
        with open('clients/input_100.txt') as file_in:
            all_init_trans = []
            for line in file_in:
                line = line.strip()
                all_init_trans.append(line)
                #sender, receiver, amount = line.split()
            
            i = 0
            
#            while(i <= len(all_init_trans)-self._num_trans_in_block):
#                    block_trans = all_init_trans[i:i+self._num_trans_in_block]
#                    block = Block(-1, block_trans)
#                    self._log.add_block(block)
#                    i += self._num_trans_in_block
#            print(len(all_init_trans))
            if(len(all_init_trans) % 2 == 0):
                while(i <= len(all_init_trans)-self._num_trans_in_block):
                    block_trans = all_init_trans[i:i+self._num_trans_in_block]
                    block = Block(-1, block_trans, self._is_neighbor)
                    self._log.add_block(block)
                    i += self._num_trans_in_block
            else:
                while(i < len(all_init_trans)-self._num_trans_in_block):
#                    print(len(all_init_trans))
                    block_trans = all_init_trans[i:i+self._num_trans_in_block]
                    block = Block(-1, block_trans, self._is_neighbor)
                    self._log.add_block(block)
                    i += self._num_trans_in_block
                    
                    if(i == len(all_init_trans)-1):
                        self.init_cur_trans = all_init_trans[i:i+1]
#                        sprint(self.init_cur_trans) 
#                        block_trans = all_init_trans[i:i+1]
#                        block = Block(-1, block_trans)
#                        self._log.add_block(block)

    def print_block_chain(self):
        self._log.print_block_chain()

    def print_client_balance(self, client_name):
        amount = self._log.get_balance(client_name)
        print(amount)

class ZeroMQServer():
    def __init__(self, name, state, log, messageBoard, neighbors):
        #super(ZeroMQServer, self).__init__(name, state, log, messageBoard,
        #                                   neighbors, server_id, port)
        global this_server
        this_server = Server(name, state, log, messageBoard,
                             neighbors)

        class SubscribeThread(threading.Thread):
            def run(thread):
                global this_server
                this_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                this_socket.bind((this_server._ip_addr, this_server._port))
                this_socket.listen(10)

                while True:
                    this_server.logger.info('start to listen the network')
                    clientsocket,addr = this_socket.accept()
                    data = clientsocket.recv(4096)
                    message = pickle.loads(data)
                    clientsocket.close()
                    this_server.logger.info('received a message')
                    #print(message._sender)
                    threadLock.acquire()
                    this_server.on_message(message)
                    threadLock.release()

        class PublishThread(threading.Thread):
            def run(thread):
                global this_server

                while True:
                    threadLock.acquire()
                    message = this_server._messageBoard.get_message()
                    threadLock.release()
                    #print(message)
                    if not message:
                        time.sleep(0.01)
                        continue # sleep wait
                    this_server.logger.info('fetch a message from board')
                    if message._receiver is not None:
                        #print(message._receiver)
                        receiver_name, receiver_port = \
                            message._receiver.split('\t')
                        receiver_port = int(receiver_port)
                        try:
                            this_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            this_socket.connect((receiver_name, receiver_port))
                            data = pickle.dumps(message)
                            this_socket.sendall(data)
                            this_socket.close()
                        except:
                            this_server.logger.info('fail sending to a receiver'
                                                    + str(receiver_port))
                        continue

                    for n in this_server._neighbors:
                        this_server.logger.info('send to all neighbors')
                        try:
                            this_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            this_socket.connect((n._ip_addr, n._port))
                            data = pickle.dumps(message)
                            this_socket.sendall(data)
                            this_socket.close()
                        except:
                            this_server.logger.info('fail sending to neighbor'+
                                                    str(n._port))

        class TimeThread(threading.Thread):
            def run(thread):
                global this_server

                while True:
                    time.sleep(0.01)
                    cur_time = time.time()
                    threadLock.acquire()
                    state = this_server._state.check_timeout(cur_time)
                    this_server._state = state
                    threadLock.release()

        class KeyboardThread(threading.Thread):
            def run(thread):
                global this_client

                while True:
                    cmd = input()
                    threadLock.acquire()
                    cmd_tokens = cmd.split()
                    if len(cmd_tokens) == 0:
                        continue
                    elif cmd_tokens[0] == 'pcb': #print current balance
                        if len(cmd_tokens) > 1:
                            this_server.print_client_balance(cmd_tokens[1])
                        pass
                    elif cmd_tokens[0] == 'pb': #print blockchain
                        this_server.print_block_chain()
                        pass
                    threadLock.release()

        self.subscribeThread = SubscribeThread()
        self.publishThread = PublishThread()
        self.timeThread = TimeThread()
        self.keyboardThread = KeyboardThread()

        threadLock = threading.Lock()
        self.subscribeThread.daemon = True
        self.subscribeThread.start()
        self.publishThread.daemon = True
        self.publishThread.start()
        self.timeThread.daemon = True
        self.timeThread.start()
        self.keyboardThread.daemon = True
        self.keyboardThread.start()
        #print('done')
