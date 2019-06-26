from .board import Board

class MemoryBoard(Board):

    def __init__(self):
        Board.__init__(self)
        self._board = []

    def post_message(self, message):
        self._board.append(message)

        self._board = sorted(self._board,
                             key=lambda a: a.timestamp, reverse=True)

    def get_message(self):
        if(len(self._board) > 0):
            #print('fetch data')
            fetched_data = self._board.pop()
            #print(fetched_data)
            return fetched_data
        else:
            return None

    def print_board(self):
        print('%d messaged on board' %len(self._board))
        for message in self._board:
            print(message)
