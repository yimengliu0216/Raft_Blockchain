from .base import BaseMessage

class ClientRequestMessage(BaseMessage):

    _type = BaseMessage.ClientRequest

    def __init__(self, sender, receiver, data):
        BaseMessage.__init__(self, sender, receiver, None, data)
