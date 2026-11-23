import pickle
import queue
import random
import sys
import threading
import time
from concurrent import futures
from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR

import grpc
from requests import get

import MPComm_pb2
import MPComm_pb2_grpc
from constMP import *

handshake_count = 0

send_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket = socket(AF_INET, SOCK_DGRAM)
recv_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
recv_socket.bind(('0.0.0.0', PEER_UDP_PORT))

group_manager = MPComm_pb2_grpc.GroupManagerStub(
    grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_PORT)))
comparison_server = MPComm_pb2_grpc.ComparisonServerStub(
    grpc.insecure_channel(SERVER_ADDR + ':' + str(SERVER_PORT)))

start_requests = queue.Queue()


def get_public_ip():
    ip_address = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ip_address))
    return ip_address


def register_with_group_manager(ip_address):
    print('Registering with group manager:', ip_address, PEER_UDP_PORT)
    group_manager.Register(MPComm_pb2.PeerAddress(ip_address=ip_address, port=PEER_UDP_PORT))


def get_list_of_peers():
    peers = list(group_manager.ListPeers(MPComm_pb2.Empty()).ip_addresses)
    print('Got list of peers:', peers)
    return peers


class PeerService(MPComm_pb2_grpc.PeerServicer):
    def Start(self, request, context):
        start_requests.put((request.peer_id, request.num_messages))
        return MPComm_pb2.StartReply(status='Peer process ' + str(request.peer_id) + ' started.')


class MsgHandler(threading.Thread):
    def __init__(self, sock, my_id):
        threading.Thread.__init__(self)
        self.sock = sock
        self.my_id = my_id

    def run(self):
        global handshake_count
        print('Handler is ready. Waiting for the handshakes...')

        while handshake_count < N_PEERS:
            msg = pickle.loads(self.sock.recv(1024))
            if msg[0] == 'READY':
                handshake_count += 1
                print('--- Handshake received:', msg[1])
        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        log_list = []
        stop_count = 0
        while True:
            msg = pickle.loads(self.sock.recv(1024))
            if msg[0] == -1:
                stop_count += 1
                if stop_count == N_PEERS:
                    break
            else:
                print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
                log_list.append(msg)

        with open('logfile' + str(self.my_id) + '.log', 'w') as log_file:
            log_file.write(str(log_list))

        print('Sending the list of messages to the server for comparison...')
        messages = [MPComm_pb2.LoggedMessage(sender=sender, number=number) for sender, number in log_list]
        comparison_server.SendLog(MPComm_pb2.MessageLog(peer_id=self.my_id, messages=messages))
        handshake_count = 0


def main():
    rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    MPComm_pb2_grpc.add_PeerServicer_to_server(PeerService(), rpc_server)
    rpc_server.add_insecure_port('0.0.0.0:' + str(PEER_RPC_PORT))
    rpc_server.start()

    ip_address = sys.argv[1] if len(sys.argv) > 1 else get_public_ip()
    register_with_group_manager(ip_address)
    while True:
        print('Waiting for signal to start...')
        my_id, n_msgs = start_requests.get()
        print('I am up, and my ID is:', my_id)
        if n_msgs == 0:
            break

        MsgHandler(recv_socket, my_id).start()
        peers = get_list_of_peers()
        for peer_ip in peers:
            print('Sending handshake to', peer_ip)
            send_socket.sendto(pickle.dumps(('READY', my_id)), (peer_ip, PEER_UDP_PORT))
        print('Main Thread: Sent all handshakes. handshake_count =', handshake_count)

        while handshake_count < N_PEERS:
            pass

        for msg_number in range(n_msgs):
            time.sleep(random.randrange(10, 100) / 1000)
            msg_pack = pickle.dumps((my_id, msg_number))
            for peer_ip in peers:
                send_socket.sendto(msg_pack, (peer_ip, PEER_UDP_PORT))
            print('Sent message', msg_number)

        for peer_ip in peers:
            send_socket.sendto(pickle.dumps((-1, -1)), (peer_ip, PEER_UDP_PORT))

    print('Terminating.')
    rpc_server.stop(1).wait()


main()
