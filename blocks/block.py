import random
from hashlib import sha256

noncelength = 16
random.seed(1)

def blockchain_hash(s):
    return sha256(s.encode()).hexdigest()

class Header:
    
    def __init__(self, currentTerm, hashPrev, hashTxns, is_neighbor):
        self.currentTerm = currentTerm
        self.hashPrev = hashPrev
        self.hashTxns = hashTxns
        self.nonce = self.generate_nonce(is_neighbor)
        self.H = []
        
    def generate_nonce(self, is_neighbor):
        s = '1234567890ABCDEF'
        while (1):
            nonce = ''.join(random.sample(s, noncelength))
            H = blockchain_hash(self.hashTxns + nonce)
            if (H[-1] in ['0', '1', '2']):
                if is_neighbor == False:
                    print('hashofTxns+nonce: ' + H)
                return nonce
            
    def header_hash(self):
        m = ''.join(['{:64x}'.format(self.currentTerm), self.hashPrev,
                     self.hashTxns, self.nonce])
        return blockchain_hash(m)

    def data(self):
        return {'term':self.currentTerm,
                'hashOfPrevBlock':self.hashPrev,
                'hashOfTxns':self.hashTxns,
                'nonce':self.nonce}
#                'hashofTxs+nonce':self.H}

class Block:
    
    def __init__(self, term, txns, is_neighbor=False):
        self.txns = txns
        txns_hash = [blockchain_hash(t) for t in txns]
        txns_hash_tree = ''.join(txns_hash)
        txns_hash_tree_hash = blockchain_hash(txns_hash_tree)
        prev_hash = "NULL"
        self.header = Header(term, prev_hash, txns_hash_tree_hash, is_neighbor)

    def header_data(self):
        return self.header.data()
        
    def set_prev_block(self, prevblock):
        self.header.hashPrev = prevblock.header.header_hash()
        #self.header_data = self.header.data()

    def get_term(self):
        return self.header_data()['term']

class BlockChain:
    
    def __init__(self):
        self.blocklist = []
        self.commitIndex = -1

    def set_commitIndex(self, new_index):
        self.commitIndex = new_index
        
    def add_block(self, block):
        if (len(self.blocklist) > 0):
            block.set_prev_block(self.blocklist[-1])
        self.blocklist.append(block)
        
    def print_block_chain(self):
        end_point = self.commitIndex + 1
        if end_point is None:
            to_print = self.blocklist
        else:
            to_print = self.blocklist[:end_point]
        for block in to_print:
            header_data = block.header_data()
            for header_key in header_data:
                print(header_key+'\t', header_data[header_key])
            for tnx in block.txns:
                print(tnx)

    def size(self):
        return len(self.blocklist)

    def get(self, i):
        if i < len(self.blocklist):
            return self.blocklist[i]
        else:
            raise Exception('out of range for blocklist')

    def get_term(self, i):
        return self.blocklist[i].header_data()['term']

    def keepUpTo(self, i):
        self.blocklist = self.blocklist[:i]

    def get_balance(self, client_name, all_txn = False):
        balance = 100
        if all_txn:
            to_count = self.blocklist
        else:
            to_count = self.blocklist[:self.commitIndex+1]
        for block in to_count:
            for txn in block.txns:
                txn = txn.split('\t')[-1]
                sender, receiver, amount = txn.split()
                amount = int(amount)
                if sender == client_name:
                    balance -= amount
                if receiver == client_name:
                    balance += amount
        return balance

    def is_exist(self, txn_id):
        for i, block in enumerate(self.blocklist):
            for txn in block.txns:
                this_txn_id = txn.split('\t')[0]
                if this_txn_id == txn_id:
                    return i
        return -1