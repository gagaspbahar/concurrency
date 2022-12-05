from collections import deque

class MVCC:
    def __init__(self, input_sequence):
        self.version_table = {}
        self.transaction_table = [[] for i in range(10)]
        self.sequence = deque([])
        self.input_sequence = deque(input_sequence)
        self.counter = 0
        # default counter: i
        self.transaction_counter = [i for i in range(10)]

    def getMaxVersionIndexByWrite(self, item):
        max_w_timestamp = self.version_table[item][0]['timestamp'][1]
        max_index = 0
        for i in range(len(self.version_table[item])):
            if self.version_table[item][i]['timestamp'][1] > max_w_timestamp:
                max_w_timestamp = self.version_table[item][i]['timestamp'][1]
                max_index = i
        return max_index

    def read(self, tx, item):
        if item not in self.version_table.keys():
            if (('tx', tx) not in self.version_table.items()):
                self.version_table[item] = []
                self.version_table[item].append({'tx': tx, 'timestamp': (
                    self.transaction_counter[tx], 0), 'version': 0})
                self.sequence.append({'tx': tx, 'item': item, 'action': 'read', 'timestamp': (
                    self.transaction_counter[tx], 0), 'version': 0})
                print("Transaction " + str(tx) + " read " + item + " at version 0" + ". Timestamp " + item + " now: (" + str(self.transaction_counter[tx]) + ", " + str(0) + ").")
                self.counter += 1
            else:
                max_index = self.getMaxVersionIndexByWrite(item)
                max_w_timestamp = self.version_table[item][max_index]['timestamp'][1]
                max_r_timestamp = self.version_table[item][max_index]['timestamp'][0]
                max_version = self.version_table[item][max_index]['version']

                if self.transaction_counter[tx] > max_r_timestamp:
                    self.version_table[item][max_index]['timestamp'] = (
                        self.transaction_counter[tx], max_w_timestamp)
                print("Transaction " + str(tx) + " read " + item + " at version " + str(max_version) +
                      ". Timestamp " + item + " now: " + str(self.version_table[item][max_index]['timestamp']) + ".")
                self.counter += 1
        else:
            max_index = self.getMaxVersionIndexByWrite(item)
            max_w_timestamp = self.version_table[item][max_index]['timestamp'][1]
            max_r_timestamp = self.version_table[item][max_index]['timestamp'][0]
            max_version = self.version_table[item][max_index]['version']

            if self.transaction_counter[tx] > max_r_timestamp:
                self.version_table[item][max_index]['timestamp'] = (
                    self.transaction_counter[tx], max_w_timestamp)
            print("Transaction " + str(tx) + " read " + item + " at version " + str(max_version) +
                  ". Timestamp " + item + " now: " + str(self.version_table[item][max_index]['timestamp']) + ".")
            self.counter += 1

    def write(self, tx, item):
        if item not in self.version_table.keys():
            self.version_table[item] = []
            self.version_table[item].append({'tx': tx, 'timestamp': (
                self.transaction_counter[tx], self.transaction_counter[tx]), 'version': 0})
            self.sequence.append({'tx': tx, 'item': item, 'action': 'write', 'timestamp': (
                self.transaction_counter[tx], self.transaction_counter[tx]), 'version': self.transaction_counter[tx]})
            print("Transaction " + str(tx) + " wrote " + item + " at version " + str(self.transaction_counter[tx]) +
                  ". Timestamp " + item + " now: (" + str(self.transaction_counter[tx]) + ", " + str(self.transaction_counter[tx]) + ").")
            self.counter += 1
        else:
            max_index = self.getMaxVersionIndexByWrite(item)
            max_w_timestamp = self.version_table[item][max_index]['timestamp'][1]
            max_r_timestamp = self.version_table[item][max_index]['timestamp'][0]
            max_version = self.version_table[item][max_index]['version']

            if self.transaction_counter[tx] < max_r_timestamp:
                # rollback
                self.sequence.append({'tx': tx, 'item': item, 'action': 'write', 'timestamp': (
                    max_r_timestamp, self.transaction_counter[tx]), 'version': max_version})
                self.rollback(tx)
            elif self.transaction_counter[tx] == max_w_timestamp:
                self.version_table[item][max_index]['timestamp'] = (
                    max_r_timestamp, self.transaction_counter[tx])
                self.sequence.append({'tx': tx, 'item': item, 'action': 'write', 'timestamp': (
                    max_r_timestamp, self.transaction_counter[tx]), 'version': max_version})
                self.counter += 1
            else:
                self.version_table[item].append({'tx': tx, 'timestamp': (
                    max_r_timestamp, self.transaction_counter[tx]), 'version': self.transaction_counter[tx]})
                print("Transaction " + str(tx) + " wrote " + item + " at version " + str(self.transaction_counter[tx]) + ". Timestamp " + item + " now: " + str((
                    max_r_timestamp, self.transaction_counter[tx])) + ".")
                self.counter += 1

    def rollback(self, tx):
        # for item in self.version_table:
        #   for i in range(len(self.version_table[item])):
        #     if self.version_table[item][i]['tx'] == tx:
        #       del self.version_table[item][i]
        tx_sequence = []
        for i in range(len(self.sequence)):
            if self.sequence[i]['tx'] == tx and self.sequence[i]['action'] != 'aborted':
                tx_sequence.append(
                    {'tx': self.sequence[i]['tx'], 'item': self.sequence[i]['item'], 'action': self.sequence[i]['action']})
        for i in range(len(self.input_sequence)):
            if self.input_sequence[i]['tx'] == tx:
                tx_sequence.append(self.input_sequence[i])
                self.input_sequence.remove(self.input_sequence[i])
        for i in range(len(tx_sequence)):
            self.input_sequence.append(tx_sequence[i])
        self.sequence.append({'tx': tx, 'item': None, 'action': 'rollback'})
        self.transaction_counter[tx] = self.counter
        print("Transaction " + str(tx) + " rolled back. Assigned new timestamp: " + str(self.transaction_counter[tx]) + ".")

    def print_sequence(self):
        for i in range(len(self.sequence)):
            if (self.sequence[i]['action'] == 'rollback'):
                print("Transaction " +
                      str(self.sequence[i]['tx']) + " rolled back.")
            elif (self.sequence[i]['action'] != 'aborted'):
                print(self.sequence[i]['item'], self.sequence[i]['tx'],
                      self.sequence[i]['timestamp'], self.sequence[i]['version'])

    def run(self):
        while len(self.input_sequence) > 0:
            # print(self.input_sequence)
            current = self.input_sequence.popleft()
            if current['action'] == 'read':
                self.read(current['tx'], current['item'])
                self.transaction_table[current['tx']].append(current['item'])
            elif current['action'] == 'write':
                self.write(current['tx'], current['item'])
                self.transaction_table[current['tx']].append(current['item'])
            else:
                print("Invalid action.")


input_string = input("Enter input string (delimited by ;): ")
input_list = input_string.split(";")
sequence = []
for input in input_list:
    input = input.strip()
    if input == "":
        continue
    try:
        if input[0] == "R":
            sequence.append(
                {"action": "read", "tx": int(input[1]), "item": input[3]})
        elif input[0] == "W":
            sequence.append(
                {"action": "write", "tx": int(input[1]), "item": input[3]})
        # elif input[0] == "C":
            # sequence.append({"type": "commit", "tx": int(input[1])})
    except:
        print("Invalid input string")
        exit()

mvcc = MVCC(sequence)
mvcc.run()

# R5(A);R2(B);R1(B);W3(B);W3(C);R5(C);R2(C);R1(A);R4(D);W3(D);W5(B);W5(C)
