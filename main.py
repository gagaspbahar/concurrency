# Simple Locking Concurrency Control
from collections import deque

class Locking:
  def __init__(self, input_sequence):
    self.lock_table = {}
    self.transaction_table = [[] for i in range(10)]
    self.sequence = deque([])
    self.input_sequence = deque(input_sequence)

  
  def exclusive_lock(self, tx, item, type):
    print(self.lock_table)
    if item not in self.transaction_table[tx]:
      self.transaction_table[tx].append(item)
    if item not in self.lock_table.keys():
      self.lock_table[item] = tx
      self.sequence.append({'tx': tx, 'item': item, 'type': 'xlock'})
      self.sequence.append({'tx': tx, 'item': item, 'type': type})
    else:
      print(self.lock_table)
      print("Transaction " + str(tx) + " failed. Item " + str(item) + " is locked by " + str(self.lock_table[item]) + ". Rolling back..")
      self.rollback(tx)
  
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
      if self.sequence[i]['tx'] == tx:
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
      item = self.input_sequence.popleft()
      print(item)
      if item['item'] in self.lock_table.keys():
        if item['tx'] == self.lock_table[item['item']]:
          self.sequence.append(item)
        else:
          self.exclusive_lock(item['tx'], item['item'], item['type'])
      else:
        self.exclusive_lock(item['tx'], item['item'], item['type'])
      flag = False
      for i in range(len(self.input_sequence)):
        if self.input_sequence[i]['tx'] == item['tx']:
          flag = True
          break
      if not flag:
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