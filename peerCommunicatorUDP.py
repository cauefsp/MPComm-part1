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

udp_socket = socket(AF_INET, SOCK_DGRAM)
udp_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
udp_socket.bind(('0.0.0.0', PEER_UDP_PORT))

group_manager = MPComm_pb2_grpc.GroupManagerStub(
    grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_PORT)))
comparison_server = MPComm_pb2_grpc.ComparisonServerStub(
    grpc.insecure_channel(SERVER_ADDR + ':' + str(SERVER_PORT)))

start_requests = queue.Queue()
delivered_messages = queue.Queue()

next_seq_number = 0
unacked_packets = {}
unacked_lock = threading.Lock()
expected_seq_numbers = {}
held_back_messages = {}


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


def multicast(sender, peers, content):
    global next_seq_number
    seq = next_seq_number
    next_seq_number += 1
    packet = pickle.dumps(('MSG', sender, seq, content))
    for peer_ip in peers:
        with unacked_lock:
            unacked_packets[(peer_ip, seq)] = (packet, time.time())
            udp_socket.sendto(packet, (peer_ip, PEER_UDP_PORT))


def retransmit_loop():
    while True:
        time.sleep(RETRANSMIT_TIMEOUT / 2)
        now = time.time()
        with unacked_lock:
            for (peer_ip, seq), (packet, sent_time) in unacked_packets.items():
                if now - sent_time > RETRANSMIT_TIMEOUT:
                    print('Retransmitting message', seq, 'to', peer_ip)
                    udp_socket.sendto(packet, (peer_ip, PEER_UDP_PORT))
                    unacked_packets[(peer_ip, seq)] = (packet, now)


def receive_loop():
    while True:
        packet, address = udp_socket.recvfrom(1024)
        msg = pickle.loads(packet)
        if msg[0] == 'ACK':
            with unacked_lock:
                unacked_packets.pop((address[0], msg[1]), None)
            continue

        _, sender, seq, content = msg
        udp_socket.sendto(pickle.dumps(('ACK', seq)), address)
        expected_seq = expected_seq_numbers.get(sender, 0)
        held_back = held_back_messages.setdefault(sender, {})
        if seq < expected_seq or seq in held_back:
            continue

        held_back[seq] = content
        if seq > expected_seq:
            print('Holding back message', seq, 'from process', sender, 'while waiting for', expected_seq)
            continue

        while expected_seq in held_back:
            delivered_messages.put((sender, held_back.pop(expected_seq)))
            expected_seq += 1
        expected_seq_numbers[sender] = expected_seq


def handle_messages(my_id, all_ready):
    print('Handler is ready. Waiting for the handshakes...')
    ready_count = 0
    stop_count = 0
    log_list = []
    while stop_count < N_PEERS:
        sender, content = delivered_messages.get()
        if content == 'READY':
            ready_count += 1
            print('--- Handshake received:', sender)
            if ready_count == N_PEERS:
                print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')
                all_ready.set()
        elif content == 'STOP':
            stop_count += 1
        else:
            print('Message ' + str(content) + ' from process ' + str(sender))
            log_list.append((sender, content))

    with open('logfile' + str(my_id) + '.log', 'w') as log_file:
        log_file.write(str(log_list))

    print('Sending the list of messages to the server for comparison...')
    messages = [MPComm_pb2.LoggedMessage(sender=sender, number=number) for sender, number in log_list]
    comparison_server.SendLog(MPComm_pb2.MessageLog(peer_id=my_id, messages=messages))


def main():
    rpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    MPComm_pb2_grpc.add_PeerServicer_to_server(PeerService(), rpc_server)
    rpc_server.add_insecure_port('0.0.0.0:' + str(PEER_RPC_PORT))
    rpc_server.start()
    threading.Thread(target=receive_loop, daemon=True).start()
    threading.Thread(target=retransmit_loop, daemon=True).start()

    ip_address = sys.argv[1] if len(sys.argv) > 1 else get_public_ip()
    register_with_group_manager(ip_address)
    while True:
        print('Waiting for signal to start...')
        my_id, n_msgs = start_requests.get()
        print('I am up, and my ID is:', my_id)
        if n_msgs == 0:
            break

        all_ready = threading.Event()
        threading.Thread(target=handle_messages, args=(my_id, all_ready)).start()
        peers = get_list_of_peers()
        print('Sending handshake to', peers)
        multicast(my_id, peers, 'READY')
        print('Main Thread: Sent all handshakes.')

        all_ready.wait()

        for msg_number in range(n_msgs):
            time.sleep(random.randrange(10, 100) / 1000)
            multicast(my_id, peers, msg_number)
            print('Sent message', msg_number)

        multicast(my_id, peers, 'STOP')

    print('Terminating.')
    rpc_server.stop(1).wait()


main()
