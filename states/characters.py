
import time
import random

import os
import json
from collections import defaultdict

from messages.base import BaseMessage
from messages.request_vote import RequestVoteMessage
from messages.request_vote import RequestVoteResponseMessage
from messages.response import ResponseMessage
from messages.append_entries import AppendEntriesMessage

from blocks.block import Block


class State(object):

    def set_server(self, server):
        self._server = server

    def on_message(self, message):
        """This method is called when a message is received,
        and calls one of the other corrosponding methods
        that this state reacts to."""

        if message.type == message.ClientRequest:
            return self.on_client_command(message)
        
        _type = message.type
        self._server.logger.info('message type'+str(_type))
        self._server.currentTermChanged = False

        if(message.term > self._server._currentTerm):
            self._server.currentTermChanged = True
            self._server._currentTerm = message.term
        # If the message.term < ours, we need to tell
        # them this so they don't get left behind.
        elif(message.term < self._server._currentTerm):
            self._send_response_message(message, yes=False)
            return self, None

        if(_type == BaseMessage.AppendEntries):
            return self.on_append_entries(message)
        elif(_type == BaseMessage.RequestVote):
            return self.on_vote_request(message)
        elif(_type == BaseMessage.RequestVoteResponse):
            #print('vote received')
            return self.on_vote_received(message)
        elif(_type == BaseMessage.Response):
            return self.on_response_received(message)

    
    def on_append_entries(self, message):
        """This is called when there is a request to
        append an entry to the log."""
        return self, None
    
    def on_vote_request(self, message):
        """This is called when there is a vote request."""
        return self, None

    def on_vote_received(self, message):
        """This is called when this node recieves a vote."""
        return self, None

    def on_response_received(self, message):
        """This is called when a response is sent back to the Leader"""
        return self, None

    def on_client_command(self, message):
        """This is called when there is a client request."""
        print(message.data)
        return self, None

    def on_leader_timeout(self, message):
        """This is called when the leader timeout is reached."""
        return self, None
    
    def _nextTimeout(self):
        self._currentTime = time.time()
        return self._currentTime + 0.001* random.randrange(self._timeout,
                                                            2 * self._timeout)

    def _send_response_message(self, msg, yes=True, txn_id=None,
                               reply_heartbeat=False):
        response = ResponseMessage(self._server._name, msg.sender, msg.term, {
            "response": yes,
            "currentTerm": self._server._currentTerm,
            'txn_id':txn_id,
            'reply_heartbeat': reply_heartbeat
        })
        self._server.send_message(response)

    def to_follower():
        follower = Follower()
        follower.set_server(self._server)
        return follower

    def to_candidate():
        candidate = Candidate()
        candidate.set_server(self._server)
        return candidate

    def to_leader():
        leader = Leader()
        leader.set_server(self._server)
        return leader
    
#    def check_timeout(self, cur_time):
#        pass


class Voter(State):

    def __init__(self):
        self._last_vote = None
        self._voted_terms = []

    def on_vote_request(self, message):
        #print(self._type)
        #print('vote')
        voted_terms = self.read_voted_terms()
        #print('voted terms:', voted_terms)
        #if(self._last_vote is None and
        #   message.data["lastLogIndex"] >= self._server._lastLogIndex):
        if(message.term not in voted_terms and
           (message.term > self._server._currentTerm or 
            (message.term == self._server._currentTerm and
             message.data["lastLogIndex"] >= self._server._lastLogIndex))):
            #print('vote yes')
            self._last_vote = message.sender
            self._send_vote_response_message(message)
        else:
            #print('vote no')
            self._send_vote_response_message(message, yes=False)

        return self, None

    def _send_vote_response_message(self, msg, yes=True):
        voteResponse = RequestVoteResponseMessage(
            self._server._name,
            msg.sender,
            msg.term,
            {"response": yes})
        self._server.send_message(voteResponse)

    def write_voted_terms(self):
        existing_terms = self.read_voted_terms()
        for new_term in self._voted_terms:
            if new_term not in existing_terms:
                existing_terms.append(new_term)
        #print(existing_terms)
        with open('saved_states/'+self._server._name+\
                  '_voted_terms.json','w') as file_out:
            json.dump(existing_terms, file_out)

    def read_voted_terms(self):
        if not os.path.exists('saved_states/'+self._server._name+\
                              '_voted_terms.json'):
            return []
        try:
            with open('saved_states/'+self._server._name+\
                      '_voted_terms.json') as file_in:
                return json.load(file_in)
        except:
            self._server.logger.info('unable to load existing terms')
            return []
        
#    def check_timeout(self, cur_time):
#        pass


class Follower(Voter):

    def __init__(self, timeout=500):
        Voter.__init__(self)
        self._timeout = timeout
        self._timeoutTime = self._nextTimeout()
        self._cur_leader = None

    def on_append_entries(self, message):
        
        self._timeoutTime = self._nextTimeout()

        if(message.term < self._server._currentTerm):
            if len(message.data['entries']) == 0:
                self._send_response_message(message, yes=False,
                                            reply_heartbeat=True)
            else:
                self._send_response_message(message, yes=False)

            return self, None

        if(message.data != {}):
            log = self._server._log
            data = message.data
            self._cur_leader = message.sender
            
            if(self._server.added_odd_trans == True):
                self._server.init_cur_trans = []
            

            # Check if the leader is too far ahead in the log.
            '''
            if(data["leaderCommit"] != self._server._commitIndex):
                # If the leader is too far ahead then we
                #   use the length of the log - 1
                self._server._commitIndex = min(data["leaderCommit"],
                                                log.size() - 1)
                self._server._log.set_commitIndex(self._server._commitIndex)
            '''

            # Can't possibly be up-to-date with the log
            # If the log is smaller than the preLogIndex
            if(log.size() <= data["prevLogIndex"]):
                self._server.logger.info('not enough log entries '
                                         +str(log.size())+' '+str(data['prevLogIndex']))
                if len(message.data['entries']) == 0:
                    self._send_response_message(message, yes=False,
                                                reply_heartbeat=True)
                else:
                    self._send_response_message(message, yes=False)
                return self, None

            # We need to hold the induction proof of the algorithm here.
            #   we make sure that the prevLogIndex term is always
            #   equal to the server.
            if(log.size() > 0 and
               log.get_term(data['prevLogIndex'])!=data["prevLogTerm"]):

                # There is a conflict we need to resync so delete everything
                #   from this prevLogIndex and forward and send a failure
                #   to the server.
                #log = log[:data["prevLogIndex"]]
                #self._server.logger.info('send response')
                self._server.logger.info('prevLogIndex do not match')
                log.keepUpTo(data['prevLogIndex'])
                if len(message.data['entries']) == 0:
                    self._send_response_message(message, yes=False,
                                                reply_heartbeat=True)
                else:
                    self._send_response_message(message, yes=False)
                self._server._log = log
                self._server._lastLogIndex = data["prevLogIndex"]
                self._server._lastLogTerm = data["prevLogTerm"]
                return self, None
            # The induction proof held so lets check if the commitIndex
            #   value is the same as the one on the leader
            else:
                # Make sure that leaderCommit > 0 and that the
                #   data is different here
                if data['leaderCommit'] > self._server._commitIndex:
                    self._server._commitIndex = min(data['prevLogIndex']+1,
                                                data['leaderCommit'])
                    self._server._log.set_commitIndex(self._server._commitIndex)
                log.keepUpTo(data['prevLogIndex']+1)
                if(len(data["entries"]) > 0):
                    for e in data["entries"]:
                        self._server.logger.info('append new entry')
                        log.add_block(e)
                    self._server._lastLogIndex = log.size() - 1
                    self._server._lastLogTerm = log.get_term(-1)
                    #self._commitIndex = log.size() - 1
                    self._server._log = log
                    self._send_response_message(message)

            return self, None
        
        else:
            return self, None

    # to deal with timeout
    def check_timeout(self, cur_time = -1):
#        from .candidate import Candidate
        if cur_time > self._timeoutTime:
            #print('follower timeout recerving message')
            self._server.logger.info('follower timeout recerving message')
            candidate = Candidate()
            candidate.set_server(self._server)
            self._timeoutTime = self._nextTimeout()
            return candidate
        return self

    def on_client_command(self, message):
        if self._cur_leader is not None:
            self._server.logger.info('send client command to current leader')
            message.update_receiver(self._cur_leader)
            self._server.send_message(message)
        return self, None
    

class Candidate(Voter):

    def set_server(self, server):
        self._server = server
        
        self._votes = {}
        self._start_election()
        self._timeout = 500
        self._timeoutTime = self._nextTimeout()

    #def on_vote_request(self, message):
        #print('vote')
    #    return self, None

    def on_vote_received(self, message):
#        from .leader import Leader
        self._server.logger.info('vote received from '+ message.sender)
        self._timeoutTime = self._nextTimeout()
        if message.sender not in self._votes:
            self._votes[message.sender] = message

            if(len(self._votes.keys()) > (self._server._total_nodes - 1) / 2):
                self._server.logger.info('become leader')
                print('become leader')
                leader = Leader()
                leader.set_server(self._server)
                #print(leader._nextIndexes)

                return leader, None
        return self, None

    def _start_election(self):
        self._server.logger.info('start election')
        self._votes = {}
        self._server._currentTerm += 1
        election = RequestVoteMessage(
            self._server._name,
            None,
            self._server._currentTerm,
            {
                "lastLogIndex": self._server._lastLogIndex,
                "lastLogTerm": self._server._lastLogTerm,
            })

        self._server.send_message(election)
        #self._server._messageBoard.print_board()
        self._votes[self._server._name] = election
        self._voted_terms.append(self._server._currentTerm)
        self.write_voted_terms()
        self._last_vote = self._server._name

    # to deal with timeout
    def check_timeout(self, cur_time= -1):
        if cur_time > self._timeoutTime:
            self._server.logger.info('candidate timeout receiving message')
            self._start_election()
            self._timeoutTime = self._nextTimeout()
        return self

    def return_to_follower(self):
        follower = Follower
#        follower.set_server(self._server)
        return follower

    def on_append_entries(self, message):
        return self.return_to_follower(), None


class Leader(State):

    def __init__(self):
        self._nextIndexes = defaultdict(int)
        self._matchIndex = defaultdict(int)
        self._cur_trans = []
        #self._pending_trans = []
        self._time_count = 0

    def set_server(self, server):
        #print(self._server._neighbors)
        self._server = server
        
#        print(self._server._currentTerm)
#        if (self._server._currentTerm <= 4):
#            self._cur_trans = self._server.init_cur_trans
#        else:
#            self._cur_trans = []
            
        self._cur_trans = self._server.init_cur_trans     
#        print(self._cur_trans)
        
        self._send_heart_beat()
        self._time_count = 0

        for n in self._server._neighbors:
            self._nextIndexes[n._name] = self._server._lastLogIndex + 1
            self._matchIndex[n._name] = 0

    def on_response_received(self, message):
        # Was the last AppendEntries good?
        if(not message.data["response"]):
            # No, so lets back up the log for this node
            self._nextIndexes[message.sender] = \
                min(self._nextIndexes[message.sender],
                    self._server._log.size()-1)
            if not message.data['reply_heartbeat']:
                self._nextIndexes[message.sender] -= 1

            # Get the next log entry to send to the client.
            previousIndex = max(0, self._nextIndexes[message.sender] - 1)
            previous = self._server._log.get(previousIndex)
            self._server.logger.info(str(self._nextIndexes[message.sender]))
            current = self._server._log.get(self._nextIndexes[message.sender])

            # Send the new log to the client and wait for it to respond.
            appendEntry = AppendEntriesMessage(
                self._server._name,
                message.sender,
                self._server._currentTerm,
                {
                    "leaderId": self._server._name,
                    "prevLogIndex": previousIndex,
                    "prevLogTerm": previous.get_term(),
                    "entries": [current],
                    "leaderCommit": self._server._commitIndex,
                })
            self._server.send_message(appendEntry)
        
        else:
            # The last append was good so increase their index.
            self._server.logger.info('append an entry to follower')
            self._nextIndexes[message.sender] += 1

            # Are they caught up?
            if(self._nextIndexes[message.sender] > self._server._lastLogIndex+1):
                self._nextIndexes[message.sender] = self._server._lastLogIndex+1

            self._matchIndex[message.sender]=self._nextIndexes[message.sender]-1
            commit_count = 1
            for _ in self._matchIndex:
                #print(self._matchIndex[_], self._nextIndexes[_])
                if self._matchIndex[_] > self._server._commitIndex:
                    commit_count += 1
            if commit_count > (self._server._total_nodes - 1) / 2:
                self._server.logger.info('commitIndex add 1')
                self._server._commitIndex += 1
                self._server._log.set_commitIndex(self._server._commitIndex)
                commit_log = self._server._log.get(self._server._commitIndex)
                log_data = commit_log.txns
                for txn in log_data:
                    #print(txn)
                    if len(txn.split('\t')) == 1:
                        continue
                    txn_id = txn.split('\t')[0]
                    receiver = txn_id.split('_')[0].replace(' ', '\t')
                    response = ResponseMessage(
                        self._server._name, receiver, None, {
                            "response": True,
                            "currentTerm": self._server._currentTerm,
                            'txn_id':txn_id})
                    self._server.send_message(response)

        return self, None

    def _send_heart_beat(self):
        #print('send heart beat')
        message = AppendEntriesMessage(
            self._server._name,
            None,
            self._server._currentTerm,
            {
                "leaderId": self._server._name,
                "prevLogIndex": self._server._lastLogIndex,
                "prevLogTerm": self._server._lastLogTerm,
                "entries": [],
                "leaderCommit": self._server._commitIndex,
            })
        self._server.send_message(message)
        

    def check_timeout(self, cur_time= -1):
        #print(self._nextIndexes)
        self._time_count += 1
        if self._time_count == 10:
            self._send_heart_beat()
            self._time_count = 0
        return self

    def on_vote_request(self, message):
        """This is called when there is a vote request."""
        if self._server.currentTermChanged:
            self._server.logger.info('leader step down')
#            from states.follower import Follower
            follower = Follower()
            follower.set_server(self._server)
            return follower, None
        return self, None

    def on_vote_received(self, message):
        """This is called when this node recieves a vote."""
        return self, None

    def in_cur_txn(self, txn_id):
        for this_txn in self._cur_trans:
            if this_txn.split('\t')[0] == txn_id:
                return True
        return False

    def send_new_entry(self, new_entry):
        self._server._log.add_block(new_entry)
        message = AppendEntriesMessage(
            self._server._name,
            None,
            self._server._currentTerm,
            {
                "leaderId": self._server._name,
                "prevLogIndex": self._server._lastLogIndex,
                "prevLogTerm": self._server._lastLogTerm,
                "entries": [new_entry],
                "leaderCommit": self._server._commitIndex,
            })
        self._server.logger.info('try to append new block')
        self._server.send_message(message)

    def get_balance(self, client_name):
        balance = self._server._log.get_balance(client_name, True)
        for data in self._cur_trans:
            txn = data.split('\t')[-1]
            sender, receiver, amount = txn.split()
            amount = int(amount)
            if sender == client_name:
                balance -= amount
            if receiver == client_name:
                balance += amount
        return balance

    def not_valid_txn(self, data):
        txn_info = data.split('\t')[-1]
        #print(txn_info)
        sender, receiver, amount = txn_info.split()
        sender_balance = self.get_balance(sender)
        if sender_balance < float(amount):
            return True
        else:
            return False

    def on_client_command(self, message):
               
        data = message.data
        #print(data)
        txn_id, txn_info = data.split('\t')[:2]
        txn_pos = self._server._log.is_exist(txn_id)
        if txn_pos == -1: # not in the current blockchain
            pass
        elif txn_pos <= self._server._commitIndex:
            self._server.logger.info('transtion already in blockchain: '+txn_id)
            self._send_response_message(message, True, txn_id)
            return self, None
        else:
            self._server.logger.info('transtion is pending: '+txn_id)
#            print('transaction is pending')
            return self, None
        if self.in_cur_txn(txn_id):
            self._server.logger.info('transtion is pending: '+txn_id)
#            print('current transaction is pending')
            return self, None
        if self.not_valid_txn(data):
            self._send_response_message(message, False, txn_id)
            return self, None

        self._cur_trans.append(message.data)
        num_trans_a_block = self._server._num_trans_in_block
        while len(self._cur_trans) >= num_trans_a_block:
            block_txns = [txn for txn in self._cur_trans[:num_trans_a_block]]
            #block_txns = [txn.data for txn in block_txns if type(txn) is not str]
            #print(self._server._currentTerm, block_txns)
            new_block = Block(self._server._currentTerm, block_txns)
            self.send_new_entry(new_block)
            #self._server._log.add_block(new_block)
            self._server._lastLogIndex += 1
            self._server._lastLogTerm = self._server._currentTerm
            #self._pending_trans += self._cur_trans[:num_trans_a_block]
            self._cur_trans = self._cur_trans[num_trans_a_block:]
        
        self._server.added_odd_trans = True
        return self, None
