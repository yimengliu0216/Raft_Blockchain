import time

class BaseMessage(object):
    AppendEntries = 0
    RequestVote = 1
    RequestVoteResponse = 2
    Response = 3
    ClientRequest = 4

    def __init__(self, sender, receiver, term, data):
        self._timestamp = int(time.time())

        self._sender = sender
        self._receiver = receiver
        self._term = term
        self._data = data

    def update_timestamp(self):
        self._timestamp = int(time.time())

    def update_receiver(self, new_receiver):
        self._receiver = new_receiver
    
    @property
    def timestamp(self):
        return self._timestamp
    
    @property
    def sender(self):
        return self._sender
    
    @property
    def receiver(self):
        return self._receiver

    @property
    def term(self):
        return self._term
    
    @property
    def data(self):
        return self._data
    
    @property
    def type(self):
        return self._type
