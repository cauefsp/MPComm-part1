import queue
from concurrent import futures

import grpc

import MPComm_pb2
import MPComm_pb2_grpc
from constMP import *


class ComparisonServer(MPComm_pb2_grpc.ComparisonServerServicer):
    def __init__(self, received_logs):
        self.received_logs = received_logs

    def SendLog(self, request, context):
        print('Received log from peer', request.peer_id)
        self.received_logs.put(request)
        return MPComm_pb2.Empty()


def prompt_user():
    return int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))


def start_peers(peers, n_msgs):
    for peer_id, peer_ip in enumerate(peers):
        with grpc.insecure_channel(peer_ip + ':' + str(PEER_RPC_PORT)) as channel:
            request = MPComm_pb2.StartRequest(peer_id=peer_id, num_messages=n_msgs)
            reply = MPComm_pb2_grpc.PeerStub(channel).Start(request)
        print(reply.status)


def wait_for_logs_and_compare(received_logs, n_msgs):
    logs = [None] * N_PEERS
    for _ in range(N_PEERS):
        message_log = received_logs.get()
        logs[message_log.peer_id] = [(m.sender, m.number) for m in message_log.messages]

    unordered = 0
    for j in range(0, n_msgs - 1):
        first_msg = logs[0][j]
        for i in range(1, N_PEERS - 1):
            if first_msg != logs[i][j]:
                unordered += 1
                break
    print('Found ' + str(unordered) + ' unordered message rounds')


def main():
    received_logs = queue.Queue()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    MPComm_pb2_grpc.add_ComparisonServerServicer_to_server(ComparisonServer(received_logs), server)
    server.add_insecure_port('0.0.0.0:' + str(SERVER_PORT))
    server.start()

    with grpc.insecure_channel(GROUPMNGR_ADDR + ':' + str(GROUPMNGR_PORT)) as channel:
        group_manager = MPComm_pb2_grpc.GroupManagerStub(channel)
        while True:
            n_msgs = prompt_user()
            peers = list(group_manager.ListPeers(MPComm_pb2.Empty()).ip_addresses)
            print('List of Peers:', peers)
            start_peers(peers, n_msgs)
            if n_msgs == 0:
                break
            print('Now, wait for the message logs from the communicating peers...')
            wait_for_logs_and_compare(received_logs, n_msgs)
        print('Stopping.')
        group_manager.Stop(MPComm_pb2.Empty())
    server.stop(1).wait()


main()
