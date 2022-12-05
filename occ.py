import math

WRITE_DATA_PLACEHOLDER = "<some value>"

class Transaction:
    def __init__(self, tx_num: int) -> None:
        self.tx_num = tx_num
        self.read_set = []
        self.write_set = []
        self.timestamps = {
            "start": math.inf,
            "validation": math.inf,
            "finish": math.inf
        }

class OCC:
    def __init__(self, sequence_string: str) -> None:
        self.input_sequence = self.parse_sequence(sequence_string)
        self.local_vars = {}
        self.current_timestamp = 0
        self.transactions = {}
        self.rolledback_transaction_nums = []

    def parse_sequence(self, sequence_string: str):
        parsed_sequence = []
        seq = sequence_string.replace(' ', '').split(';')
        for cmd in seq:
            parsed_cmd = {}
            if (cmd[0].upper() == 'R' or cmd[0].upper() == 'W') and '(' in cmd and ')' in cmd:
                prt_open_idx = cmd.index('(')
                prt_close_idx = cmd.index(')')
                if prt_open_idx > prt_close_idx:
                    print("INVALID SEQUENCE (parenthesis)")
                    break
                if cmd[0].upper() == 'R':
                    parsed_cmd["action"] = "read"
                elif cmd[0].upper() == 'W':
                    parsed_cmd["action"] = "write"
                tx_number = cmd[1:prt_open_idx]
                parsed_cmd["tx"] = int(tx_number)
                parsed_cmd["item"] = cmd[prt_open_idx+1:prt_close_idx]
            elif cmd[0].upper() == 'C':
                parsed_cmd["action"] = "commit"
                parsed_cmd["tx"] = int(cmd[1:])
            else:
                print(f"INVALID COMMAND {cmd}")
                break
            parsed_sequence.append(parsed_cmd)
        return parsed_sequence

    def read(self, cmd):
        # baca data dari database (tidak diimplementasikan di tugas ini)
        print("Transaction", cmd["tx"], "read", cmd["item"], "from DB")
        # tambahkan ke read_set tx
        if cmd["item"] not in self.transactions[cmd["tx"]].read_set:
            self.transactions[cmd["tx"]].read_set.append(cmd["item"])
            print(" ", cmd["item"], "has been added to the READ SET of transaction", cmd["tx"])
        print(" ","--READ SET transaction", cmd["tx"], "=", self.transactions[cmd["tx"]].read_set)
        

    def temporal_write(self, cmd):
        # tulis data ke local variable  
        self.local_vars[cmd["item"]] = WRITE_DATA_PLACEHOLDER
        print("Transaction", cmd["tx"], "temporary write", cmd["item"], "to LOCAL VARIABLE")
        # tambahkan ke write_set tx
        if cmd["item"] not in self.transactions[cmd["tx"]].write_set:
            self.transactions[cmd["tx"]].write_set.append(cmd["item"])
            print(" ",cmd["item"], "has been added to the WRITE SET of transaction", cmd["tx"])
        print(" ","--WRITE SET transaction", cmd["tx"], "=", self.transactions[cmd["tx"]].write_set)
        

    def validate(self, cmd):
        tx_num = cmd["tx"]
        self.transactions[tx_num].timestamps["validation"] = self.current_timestamp
        is_valid = True
        for current_tx_num in self.transactions.keys():
            if current_tx_num != tx_num:
                ti_validation_timestamp = self.transactions[current_tx_num].timestamps["validation"]
                ti_finish_timestamp = self.transactions[current_tx_num].timestamps["finish"]
                tj_start_timestamp = self.transactions[current_tx_num].timestamps["start"]
                tj_validation_timestamp = self.transactions[current_tx_num].timestamps["validation"]
                if ti_validation_timestamp < tj_validation_timestamp:
                    if ti_finish_timestamp < tj_start_timestamp:
                        pass
                    elif ti_finish_timestamp != math.inf and (tj_start_timestamp < ti_finish_timestamp and ti_finish_timestamp < tj_validation_timestamp):
                        write_set_ti = self.transactions[current_tx_num].write_set
                        read_set_tj = self.transactions[tx_num].read_set
                        is_element_intersect = False
                        for v in write_set_ti:
                            if v in read_set_tj:
                                is_element_intersect = True
                                break
                        if is_element_intersect:
                            is_valid = False
                            break
                    else:
                        is_valid = False
                        break
        if is_valid:
            self.write(tx_num)
        else:
            # fill rollback list
            self.rolledback_transaction_nums.append(tx_num)
            print(f"Transaction {tx_num} rollback")
                
        
    def write(self, tx_num):
        tx = self.transactions[tx_num]
        for var in tx.write_set:
            written_data = self.local_vars[var]
            print(f"Transaction {tx_num} write {var} = {written_data} to DB")

        self.commit(tx_num)
    
    def commit(self, tx_num):
        print(f"Transaction {tx_num} commited")
        self.transactions[tx_num].timestamps["finish"] = self.current_timestamp
    
    def rollback_all(self):
        while(len(self.rolledback_transaction_nums) > 0):
            rolledback_tx_num = self.rolledback_transaction_nums.pop(0)
            rolledback_tx = self.transactions[rolledback_tx_num]
            # reset transaction parameters
            rolledback_tx.read_set = []
            rolledback_tx.write_set = []
            rolledback_tx.timestamps = {
                "start": self.current_timestamp,
                "validation": math.inf,
                "finish": math.inf
            }
            rolledback_cmd_sequence = []
            for cmd in self.input_sequence:
                if cmd["tx"] == rolledback_tx_num:
                    rolledback_cmd_sequence.append(cmd)
            for rolledback_cmd in rolledback_cmd_sequence:
                # perform action
                if rolledback_cmd["action"] == "read":
                    self.read(rolledback_cmd)
                elif rolledback_cmd["action"] == "write":
                    self.temporal_write(rolledback_cmd)
                elif rolledback_cmd["action"] == "commit":
                    self.validate(rolledback_cmd)
                self.current_timestamp += 1

            self.current_timestamp += 1
                
    
    def run(self):
        # run initial commands
        for cmd in self.input_sequence:
            # setup
            if cmd["tx"] not in self.transactions.keys():
                self.transactions[cmd["tx"]] = Transaction(cmd["tx"])
                self.transactions[cmd["tx"]].timestamps["start"] = self.current_timestamp
            # perform action
            if cmd["action"] == "read":
                self.read(cmd)
            elif cmd["action"] == "write":
                self.temporal_write(cmd)
            elif cmd["action"] == "commit":
                self.validate(cmd)
            self.current_timestamp += 1
        # re-run rolledback transactions
        self.rollback_all()

input_string = input("Enter input string (delimited by ;): ")
occ = OCC(input_string)
print()
occ.run()