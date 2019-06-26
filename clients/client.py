import zmq
import threading
import time
import pickle
import socket
import random
import json
import os
from server_config import CONFIG as config_server
from messages.client_request import ClientRequestMessage

class Client(object):

    def __init__(self, name, client_id, messageBoard):
        self._ip_addr, self._port = name.split('\t')
        self._port = int(self._port)
        self._name = name
        self._id = client_id
        self._messageBoard = messageBoard
        self._servers = config_server
        self._cur_leader = 'localhost\t'+str(list(config_server.values())[0])
        self.pending_txn = []
        self._txn_id = 0

    def send_message(self, message):
        #for n in self._neighbors:
        #    message._receiver = n._full_address
        #message._receiver = self._full_address
        self.post_message(message)

    def send_message_response(self, message):
        raise Exception('send_message_response function is abandoned, try use \
                        send_message instead')
        n = [n for n in self._neighbors if n._full_address == message.receiver]
        if(len(n) > 0):
            n[0].post_message(message)

    def post_message(self, message):
        self._messageBoard.post_message(message)
        #self._messageBoard.print_board()

    def on_message(self, message):
        self._cur_leader = message.sender
        to_remove = []
        for _ in self.pending_txn:
            if _.data.split('\t')[0] == message.data['txn_id']:
                to_remove.append(_)
        for _ in to_remove:
            self.pending_txn.remove(_)
        if message.data['response']:
            print(message.data['txn_id'],' succeed')
        else:
            print(message.data['txn_id'],' failed')

    def new_receiver(self, message):
        new_receiver = 'localhost\t'+str(
            random.sample(list(config_server.values()),
                          1)[0])
        while new_receiver == this_client._cur_leader:
            new_receiver = 'localhost\t'+str(
                random.sample(list(config_server.values()),
                              1)[0])
        message.update_receiver(new_receiver)

    def write_txn_id(self):
        with open('saved_states/'+self._name+\
                  '_txn_id.json','w') as file_out:
            json.dump(self._txn_id, file_out)

    def read_txn_id(self):
        if not os.path.exists('saved_states/'+self._name+\
                              '_txn_id.json'):
            return 0
        try:
            with open('saved_states/'+self._name+\
                      '_txn_id.json') as file_in:
                return json.load(file_in)
        except:
            #self._server.logger.info('unable to load existing terms')
            return 0

    def get_new_id(self):
        self._txn_id = self.read_txn_id()
        new_id = self._name.replace('\t', ' ') + '_'+str(self._txn_id)
        self._txn_id += 1
        self.write_txn_id()
        return new_id


class ZeroMQclient():
    def __init__(self, name, client_id, messageBoard):
        #super(ZeroMQclient, self).__init__(name, state, log, messageBoard,
        #                                   neighbors, client_id, port)
        global this_client
        this_client = Client(name, client_id, messageBoard)

        class SubscribeThread(threading.Thread):
            def run(thread):
                global this_client
                this_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                this_socket.bind((this_client._ip_addr, this_client._port))
                this_socket.listen(10)

                while True:
                    #print('start to listen the network')
                    clientsocket,addr = this_socket.accept()
                    data = clientsocket.recv(4096)
                    message = pickle.loads(data)
                    clientsocket.close()
                    #print('received a  message')
                    #print(message._sender)
                    threadLock.acquire()
                    this_client.on_message(message)
                    threadLock.release()

        class PublishThread(threading.Thread):
            def run(thread):
                global this_client

                while True:
                    threadLock.acquire()
                    message = this_client._messageBoard.get_message()
                    threadLock.release()
                    #print(message)
                    if not message:
                        time.sleep(0.01)
                        continue # sleep wait
                    print('fetch a message from board')
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
                            print('fail sending to a receiver', receiver_port)
                            this_client.new_receiver(message)
                            this_client.send_message(message)
                        continue

        class TimeThread(threading.Thread):
            def run(thread):
                global this_client
                timeout = 20

                while True:
                    time.sleep(0.01)
                    cur_time = time.time()
                    threadLock.acquire()
                    for message in this_client.pending_txn:
                        if message.timestamp + timeout < int(time.time()):
                            print('timeout, resending ', message.data)
                            this_client.new_receiver(message)
                            this_client.send_message(message)
                            message.update_timestamp()
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
                    elif cmd_tokens[0] == 'send' and len(cmd_tokens) ==3:
                        #print(this_client._cur_leader)
                        cur_leader = this_client._cur_leader
                        send_data = this_client.get_new_id()+'\t'+ \
                            this_client._id + cmd[4:]
                        message = ClientRequestMessage(this_client._name,
                                                       cur_leader,
                                                       send_data)
                        this_client.send_message(message)
                        print('txn id:', send_data.split('\t')[0])
                        this_client.pending_txn.append(message)
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
