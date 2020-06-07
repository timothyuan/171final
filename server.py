import sys
import socket
import queue
import threading
from time import sleep
from random import randint
from hashlib import sha256

def send(s, msg, addr):
    global connections
    # delay for each message
    sleep(2)
    # check if connection is valid
    if connections[addr]:
        s.sendto(msg.encode('utf-8'), addr)

def communication(s):
    global lock, connections, ballot, acceptNum, acceptVal, promises, acceptances, pid, balance
    while True:
        event, addr = s.recvfrom(1024)
        event = event.decode('utf-8')
        event = event.split('/')
        type = event[0]
        with lock:
            if not connections[addr]:
                continue
            if type == 'prepare':
                # prepare/ballotnum/depth
                print('Received prepare:', event)
                bal = (int(event[1]),event[2],int(event[3]))
                if bal >= ballot:
                    # ballot number less than received ballot number
                    print(bal, ballot)
                    ballot = bal
                    reply = 'promise/{}/{}/{}/{}/{}/{}/{}'.format(ballot[0], ballot[1], ballot[2],acceptNum[0], acceptNum[1], acceptNum[2], acceptVal)
                    threading.Thread(target = send, args = [s, reply, addr]).start()
            elif type == 'promise':
                print('Received promise:', event)
                # promise/ballotnum/pid/depth/acceptnum/acceptpid/acceptdepth/acceptval
                bal = (int(event[1]),event[2],int(event[3]))
                if ballot!=bal:
                    # ignore different ballot numbers
                    continue
                promises.append(event)
            elif type == 'accept':
                # accept/ballotnum/depth/acceptVal
                print('Received accept:', event)
                bal = (int(event[1]),event[2],int(event[3]))
                if bal >= ballot:
                    acceptNum = bal
                    acceptVal = event[4]
                    reply = 'accepted/{}/{}/{}'.format(bal[0], bal[1], acceptVal)
                    threading.Thread(target = send, args = [s, reply, addr]).start()
            elif type == 'accepted':
                # accepted/ballotnum/depth/acceptVal
                print('Received accepted:', event)
                acceptances+=1
            elif type == 'decide':
                # decide/ballotnum/acceptVal
                print('Received decide:', event)
                acceptVal = event[2]
                chain.append(acceptVal.split('||'))
                transactions = acceptVal.split('||')[0]
                transactions = transactions[1:-1].split(',')
                print('transactions:', transactions)
                for t in transactions:
                    fields = t.strip()[1:-1].split('-')
                    if fields[1] == pid:
                        balance += int(fields[2])
                promises.clear()
                acceptances = 0
                ballotNum = (0, pid, 0)
                acceptNum = (0, '', 0)
                acceptVal = None

def processing(s):
    global ports, pid, balance, chain, transfers, pending, promises, acceptances, ballot, acceptVal, acceptNum, lock
    while True:
        with lock:
            if len(transfers) == 0 and len(pending) == 0 and acceptVal == None:
                continue

        # timeout before sending prepare messages
        sleep(7)

        # LEADER ELECTION
        while True:

            # move transfers to pending, ensuring transfers added while paxos is running are not lost
            with lock:
                pending.extend(transfers)
                transfers.clear()

            with lock:
                if len(pending) == 0 and acceptVal == None:
                    break
            # random paxos wait
            sleep(randint(0,5))
            with lock:
                # clear promises
                promises.clear()
                # clear acceptances
                acceptances = 0
                # increment ballot
                ballot = (ballot[0]+1, ballot[1], len(chain))

                # send prepare messages
                # prepare/ballotnum/pid/depth
                msg = 'prepare/{}/{}/{}'.format(ballot[0], ballot[1], ballot[2])

                for p in ports:
                    if p != pid:
                        addr = ('127.0.0.1', ports[p])
                        print('Sending prepare:', msg)
                        threading.Thread(target = send, args = [s, msg, addr]).start()

            # wait to receive promises
            sleep(4.5)

            with lock:
                # if not enough promises, restart loop
                if len(promises) < 2:
                    continue

            with lock:
                if all(promise[-1] == 'None' for promise in promises):
                    prevHash = ''
                    if len(chain) != 0:
                        prevHash = '{}||{}||{}'.format(chain[-1][0], chain[-1][1], chain[-1][2])
                    prevHash = sha256(prevHash.encode('utf-8')).hexdigest()
                    while True:
                        nonce = str(randint(0, 50))
                        h = '{}||{}'.format(pending, nonce)
                        hash = sha256(h.encode('utf-8')).hexdigest()
                        if '0' <= hash[-1] <= '4':
                            acceptVal = '{}||{}'.format(h, prevHash)
                            print('Nonce:', nonce)
                            print('Hash value:', hash)
                            print('acceptVal:', acceptVal)
                            break
                # If acceptVal are not all empty, promote the acceptVal with the highest ballotNum
                else:
                    promises.sort(reverse = True)
                    acceptVal = promises[0][-1]

            # CONSENSUS
            with lock:
                # send accept messages
                # accept/ballotnum/pid/depth/acceptVal
                msg = 'accept/{}/{}/{}/{}'.format(ballot[0], ballot[1], ballot[2], acceptVal)
                for p in ports:
                    if p != pid:
                        addr = ('127.0.0.1', ports[p])
                        print('Sending accept:', msg)
                        threading.Thread(target = send, args = [s, msg, addr]).start()

            # wait to receive acceptances
            sleep(4.5)

            with lock:
                # If the process does not receive the majority of 'accepted', restart
                if acceptances < 2:
                    continue

            # DECIDE
            with lock:
                # decide/ballotNum/acceptVal
                msg = 'decide/{}/{}'.format(ballot[0], acceptVal)
                print('SENDING DECIDE TO ALL PROCESSES')
                print(acceptVal)
                for p in ports:
                    if p != pid:
                        addr = ('127.0.0.1', ports[p])
                        threading.Thread(target = send, args = [s, msg, addr]).start()
                chain.append(acceptVal.split('||'))
                transactions = acceptVal.split('||')[0]
                transactions = transactions[1:-1].split(',')
                print('transactions:', transactions)
                for t in transactions:
                    fields = t.strip()[1:-1].split('-')
                    if fields[0] == pid:
                        balance -= int(fields[2])
                pending.clear()
                promises.clear()
                acceptances = 0
                ballotNum = (0, pid, 0)
                acceptNum = (0, '', 0)
                acceptVal = None
                break


def main():
    # python server.py pid
    global ports, connections, pid, balance, chain, transfers, pending, promises, acceptances, ballot, acceptVal, acceptNum, lock
    ports = {
    'p1':3000,
    'p2':3001,
    'p3':3002,
    'p4':3003,
    'p5':3004
    }
    connections = {
        ('127.0.0.1', 3000): True,
        ('127.0.0.1', 3001): True,
        ('127.0.0.1', 3002): True,
        ('127.0.0.1', 3003): False,
        ('127.0.0.1', 3004): False
    }
    pid = sys.argv[1]
    balance = 100
    chain = []
    transfers = []
    pending = []
    promises = []
    acceptances = 0
    # ballotnum, pid, depth
    ballot = (0,pid, 0)
    # ballotnum, depth
    acceptNum = (0,'',0)
    acceptVal = None
    lock = threading.Lock()
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.0.0.1', ports[pid]))
    threading.Thread(target = communication, args=[s]).start()
    threading.Thread(target = processing, args=[s]).start()
    while True:
        event = input('Enter event (e for examples, p to print balance/chain): ')
        if event =='e':
            print('sender-receiver-amt')
            print('p1-p2-10')
        elif event =='p':
            print('balance:', balance)
            print('blockchain:', chain)
        elif event.split('-')[0]==pid:
            transfers.append(event)
        else:
            print('invalid commmand')

if __name__ == '__main__':
    main()
