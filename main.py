# Simple Locking Concurrency Control
from collections import deque

class Locking:
  def __init__(self, input_sequence):
    self.lock_table = {}
    self.transaction_table = [[] for i in range(10)]
    self.sequence = deque([])
    self.input_sequence = deque(input_sequence)
    self.queue = deque([])

  
  def exclusive_lock(self, tx, item, type):
    if item not in self.lock_table.keys():
      self.lock_table[item] = tx
      print("Transaction " + str(tx) + " acquired exclusive lock on " + item)
      self.sequence.append({'tx': tx, 'item': item, 'type': 'xlock'})
      print("Transaction " + str(tx) + " " + type + " " + item)
      self.sequence.append({'tx': tx, 'item': item, 'type': type})
      if item not in self.transaction_table[tx]:
        self.transaction_table[tx].append(item)
      return True
    else:
      print("Transaction " + str(tx) + " failed. Item " + str(item) + " is locked by " + str(self.lock_table[item]) + ".", end = " ")
      if self.transaction_table[tx] != []:
        print("Rolling back transaction " + str(tx) + ".")
        self.sequence.append({'tx': tx, 'item': item, 'type': type})
        self.rollback(tx)
      else:
        print("Adding transaction " + str(tx) + " to queue.")
        self.queue.append({'tx': tx, 'item': item, 'type': type})
        # self.input_sequence.append({'tx': tx, 'item': item, 'type': type})
      return False

  
  def release_lock(self, tx, item):
    if ((item, tx) in self.lock_table.items()):
      del self.lock_table[item]
      
  def commit(self, tx):
    for item in self.transaction_table[tx]:
      self.release_lock(tx, item)
    self.sequence.append({'tx': tx, 'item': None, 'type': 'commit'})
    self.transaction_table[tx] = []
    print("Transaction " + str(tx) + " committed.")

  def rollback(self, tx):
    for item in self.transaction_table[tx]:
      self.release_lock(tx, item)
    tx_sequence = []
    for i in range(len(self.sequence)):
      if self.sequence[i]['tx'] == tx and self.sequence[i]['type'] != 'aborted' and self.sequence[i]['type'] != 'xlock':
        tx_sequence.append(self.sequence[i])
    for i in range(len(self.input_sequence)):
      if self.input_sequence[i]['tx'] == tx:
        tx_sequence.append(self.input_sequence[i])
        self.input_sequence.remove(self.input_sequence[i])
    for i in range(len(tx_sequence)):
      self.input_sequence.append(tx_sequence[i])
    self.sequence.append({'tx': tx, 'item': None, 'type': 'rollback'})
    self.transaction_table[tx] = []
    print("Transaction " + str(tx) + " rolled back.")
    
  def print_sequence(self):
    print("--- Sequence ---")
    print("Type\tTransaction\tItem")
    for i in range(len(self.sequence)):
      if self.sequence[i]['type'] == 'aborted':
        continue
      else:
        if self.sequence[i]['item'] == None:
          print("Transaction " + str(self.sequence[i]['tx']) + " " + self.sequence[i]['type'])
        else:
          print(self.sequence[i]['type'] + "\t" + str(self.sequence[i]['tx']) + "\t\t" + self.sequence[i]['item'])
  
  def print_seq(self):
    print(self.sequence)

  def run(self):
    while len(self.input_sequence) > 0:
      status = True
      flag = True
      if len(self.queue) > 0:
        if self.queue[0]['item'] not in self.lock_table.keys():
          self.input_sequence.appendleft(self.queue.popleft())
      item = self.input_sequence.popleft()
      if item['item'] in self.lock_table.keys():
        if item['tx'] == self.lock_table[item['item']]:
          self.sequence.append(item)
          print("Transaction " + str(item['tx']) + " " + item['type'] + " " + item['item'])
        else:
          status = self.exclusive_lock(item['tx'], item['item'], item['type'])
      else:
        status = self.exclusive_lock(item['tx'], item['item'], item['type'])
      for i in range(len(self.input_sequence)):
        if self.input_sequence[i]['tx'] == item['tx']:
          flag = False
          break
      if flag and status:
        self.commit(item['tx'])
      
      # elif item['type'] == 'commit':
      #   self.sequence.append(item)
      #   self.commit(item['tx'])
    self.print_sequence()


input_string = input("Enter input string (delimited by ;): ")
input_list = input_string.split(";")
sequence = []
for input in input_list:
  input = input.strip()
  if input == "":
    continue
  try: 
    if input[0] == "R":
      sequence.append({"type": "read", "tx": int(input[1]), "item": input[3]})
    elif input[0] == "W":
      sequence.append({"type": "write", "tx": int(input[1]), "item": input[3]})
    # elif input[0] == "C":
      # sequence.append({"type": "commit", "tx": int(input[1])})
  except:
    print("Invalid input string")
    exit()

locking = Locking(sequence)
locking.run()