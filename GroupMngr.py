import threading
from concurrent import futures

import grpc

import MPComm_pb2
import MPComm_pb2_grpc
from constMP import *


class GroupManager(MPComm_pb2_grpc.GroupManagerServicer):
    def __init__(self, stop_requested):
        self.membership = []
        self.lock = threading.Lock()
        self.stop_requested = stop_requested

    def Register(self, request, context):
        with self.lock:
            self.membership.append((request.ip_address, request.port))
        print('Registered peer:', request.ip_address, request.port)
        return MPComm_pb2.Empty()

    def ListPeers(self, request, context):
        with self.lock:
            peers = [ip_address for (ip_address, port) in self.membership]
        print('List of peers sent:', peers)
        return MPComm_pb2.PeerList(ip_addresses=peers)

    def Stop(self, request, context):
        print('Stopping.')
        self.stop_requested.set()
        return MPComm_pb2.Empty()


def main():
    stop_requested = threading.Event()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    MPComm_pb2_grpc.add_GroupManagerServicer_to_server(GroupManager(stop_requested), server)
    server.add_insecure_port('0.0.0.0:' + str(GROUPMNGR_PORT))
    server.start()
    print('Group manager listening on port', GROUPMNGR_PORT)
    stop_requested.wait()
    server.stop(1).wait()


main()
