#!/bin/bash
set -e

if [ -z "$INSIDE_EMULATED_NETWORK" ]; then
    INSIDE_EMULATED_NETWORK=1 exec unshare --user --map-root-user --net --mount "$0" "$@"
fi

DELAY=${DELAY:-50ms}
JITTER=${JITTER:-40ms}
LOSS=${LOSS:-0%}

cd "$(dirname "$0")"
N_PEERS=$(python3 -c 'from constMP import N_PEERS; print(N_PEERS)')
mkdir -p logs
mount -t tmpfs none /run

ip link add br0 type bridge
ip link set br0 up

add_node() {
    ip netns add "$1"
    ip link add "veth-$1" type veth peer name "br-$1"
    ip link set "veth-$1" netns "$1"
    ip link set "br-$1" master br0
    ip link set "br-$1" up
    ip -n "$1" addr add "$2/24" dev "veth-$1"
    ip -n "$1" link set "veth-$1" up
    ip -n "$1" link set lo up
}

add_node server 10.0.0.1
for i in $(seq 0 $((N_PEERS - 1))); do
    add_node "peer$i" "10.0.0.$((11 + i))"
    ip netns exec "peer$i" tc qdisc add dev "veth-peer$i" root netem delay "$DELAY" "$JITTER" loss "$LOSS"
done
echo "Emulated network: $N_PEERS peers, delay $DELAY +- $JITTER, loss $LOSS"

wait_for_log_lines() {
    until [ "$(grep -c "$1" logs/groupmngr.out)" -ge "$2" ]; do sleep 0.5; done
}

trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
ip netns exec server python3 -u GroupMngr.py > logs/groupmngr.out 2>&1 &
wait_for_log_lines 'Group manager listening' 1
for i in $(seq 0 $((N_PEERS - 1))); do
    ip_address="10.0.0.$((11 + i))"
    ip netns exec "peer$i" python3 -u peerCommunicatorUDP.py "$ip_address" > "logs/peer-$ip_address.out" 2>&1 &
done
wait_for_log_lines 'Registered peer' "$N_PEERS"
echo "All $N_PEERS peers registered"
ip netns exec server python3 -u comparisonServer.py
wait
