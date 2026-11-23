# MPComm - Parte 1

Trabalho de Sistemas Distribuídos, Parte 1: melhorias nos protocolos de comunicação do
[MPComm](https://github.com/professorfabio/MPComm) do professor. Este repositório é uma cópia
integral do original, com o histórico dele preservado, e as mudanças da Parte 1 estão nos commits
seguintes.

Integrantes: Cauê Filipe de Siqueira Pereira e Caio Souza Coelho Gonzaga.

O MPComm é uma demonstração de comunicação multicast sem coordenação. Um conjunto de peers envia
mensagens numeradas a todos os outros, em intervalos aleatórios, e um servidor de comparação recolhe a
ordem em que cada peer recebeu as mensagens e compara essas ordens. Nesta parte, todas as interações
cliente-servidor, que usavam sockets TCP com pickle, passaram a usar RPC (gRPC). O multicast entre os
peers continua em sockets UDP.

## Arquitetura

O sistema continua com os mesmos três programas do original:

- `GroupMngr.py`: gerenciador de grupo. Guarda os endereços dos peers que se registraram e devolve a
  lista quando pedida.
- `comparisonServer.py`: coordena o experimento. Pergunta quantas mensagens cada peer deve enviar,
  dá a largada a cada peer, recebe os logs de mensagens recebidas e compara.
- `peerCommunicatorUDP.py`: peer. Espera a largada, troca handshakes com os outros peers, envia as
  mensagens por multicast, registra as que recebe e manda o log para o servidor de comparação.

A tabela mostra cada interação antes e depois desta parte:

| Interação | Original | Parte 1 |
|---|---|---|
| Peer se registra no gerenciador de grupo | TCP, pedido `{"op": "register"}` | `GroupManager.Register` |
| Peer e servidor de comparação pedem a lista de peers | TCP, pedido `{"op": "list"}` | `GroupManager.ListPeers` |
| Servidor de comparação encerra o gerenciador | TCP, pedido `{"op": "stop"}` | `GroupManager.Stop` |
| Servidor de comparação dá a largada a cada peer | TCP, o peer escutava na `PEER_TCP_PORT` | `Peer.Start` |
| Peer envia o log ao servidor de comparação | TCP, lista em pickle | `ComparisonServer.SendLog` |
| Peers trocam handshakes, mensagens e aviso de fim | UDP | UDP |

Um experimento funciona assim: o servidor de comparação lê o número de mensagens, pede a lista ao
gerenciador de grupo e chama `Start` em cada peer, passando o ID do peer (a posição dele na lista) e o
número de mensagens. Cada peer envia um `READY` a todos, espera o `READY` de todos os peers (incluindo
o próprio), envia suas mensagens numeradas de 0 a n-1 com intervalos aleatórios de 10 a 100 ms e por
fim um aviso de fim. Quando recebe o aviso de fim de todos, grava `logfile<ID>.log` e chama `SendLog`.
Depois de receber os logs de todos os peers, o servidor de comparação mostra o resultado e pergunta de
novo. Com 0, ele chama `Start` com 0 mensagens, o que encerra os peers, e depois `Stop` no gerenciador
de grupo.

## Parte 1a: RPC no lugar de sockets

As interfaces estão em `MPComm.proto`, com três serviços: `GroupManager` (`Register`, `ListPeers` e
`Stop`), `ComparisonServer` (`SendLog`) e `Peer` (`Start`). Cada processo é servidor do seu serviço e
cliente dos serviços dos outros.

No `GroupMngr.py`, o laço de `accept` com a escolha da operação pelo campo `op` virou uma classe com um
método por RPC. Como o gRPC atende as chamadas em várias threads, a lista de membros é protegida por um
lock. O `Stop` sinaliza um `threading.Event` e a thread principal desliga o servidor.

No `comparisonServer.py`, o `SendLog` só coloca o log recebido em uma fila, e a thread principal tira
da fila um log por peer antes de comparar. Os logs chegam com o ID do peer, então ficam indexados por
ele.

No peer, a espera pela largada era um `accept` na `PEER_TCP_PORT`. Agora o peer sobe um servidor gRPC
com o serviço `Peer`, e o `Start` coloca o ID e o número de mensagens em uma fila que a thread principal
lê. Ao terminar, o peer chama `rpc_server.stop(1).wait()`, para que a resposta do último `Start`
chegue ao servidor de comparação antes de o processo sair.

Em `constMP.py`, `PEER_TCP_PORT` e `GROUPMNGR_TCP_PORT` viraram `PEER_RPC_PORT` e `GROUPMNGR_PORT`, e `N`
virou `N_PEERS`. O peer também aceita, como argumento opcional, o IP com que deve se registrar. Sem o
argumento, ele continua usando o IP público obtido no ipify, como no original.

### Comparação entre sockets e RPC

Vantagens do RPC, a partir do que mudou no código:

- A interface entre os processos fica explícita em um arquivo só, o `MPComm.proto`, e os stubs são
  gerados a partir dele. No original o protocolo era implícito: um dicionário em pickle com um campo
  `op`, que cliente e servidor tinham que interpretar igual. O `register` não tinha resposta e uma
  operação desconhecida caía em um `pass`, sem resposta para o cliente. Com gRPC toda chamada tem
  resposta ou um erro com código de status (`UNAVAILABLE`, por exemplo, quando o servidor não está no
  ar).
- O gRPC cuida do enquadramento das mensagens. O original lia cada mensagem com um único `recv` de
  tamanho fixo (`recv(2048)`, `recv(32768)`), supondo que a mensagem inteira chega em uma leitura e cabe
  no buffer. Com 6 peers e 1000 mensagens por peer, o log em pickle tem 40.490 bytes e não cabe no
  `recv(32768)` do servidor de comparação, então o `pickle.loads` receberia a mensagem cortada. Com gRPC
  o mesmo log (39.220 bytes em protobuf) chegou inteiro no teste com 1000 mensagens, e o limite padrão
  é de 4 MB por mensagem.
- Cada interação ficou menor e parece uma chamada local, como
  `group_manager.ListPeers(MPComm_pb2.Empty())`, sem `connect`, `send`, `recv`, `close` e pickle em
  cada ponto do código.
- O servidor gRPC atende várias chamadas ao mesmo tempo com um pool de threads, enquanto o gerenciador
  de grupo original atendia uma conexão por vez no laço de `accept`.
- Protobuf só carrega dados e não depende da linguagem. O pickle só funciona entre programas em Python
  e, ao decodificar bytes vindos da rede, pode executar código escolhido por quem os enviou. Isso deixou
  de acontecer nas interações que passaram para gRPC; o multicast em UDP manteve o pickle do original.

Desvantagens:

- O sistema passou a depender de bibliotecas externas (`grpcio` e `grpcio-tools`) e de um passo de
  geração de código. Qualquer mudança em uma mensagem exige gerar os stubs de novo em todas as máquinas.
- Há mais camadas entre o programa e a rede (protobuf, HTTP/2, conexões mantidas pelo canal e threads
  do servidor). Para mensagens pequenas como o sinal de largada, isso é mais custo do que um `send` em
  um socket TCP.
- O controle do fluxo fica com o framework. O peer, que antes chamava `accept` no momento em que queria
  esperar a largada, agora mantém um servidor gRPC em outras threads e precisa de uma fila para passar a
  largada à thread principal. O encerramento também ficou mais delicado, porque o processo não pode sair
  antes de o gRPC enviar a resposta do último `Start`.
- O tráfego é binário sobre HTTP/2, o que dificulta inspecionar as mensagens na rede. O modelo de RPC
  também é ponto a ponto e de pedido e resposta, então não serve para o multicast entre os peers, que
  continua em UDP.

## Como executar

Requer Python 3. Na raiz do repositório:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install grpcio grpcio-tools requests
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. MPComm.proto
```

### Em uma máquina, com rede emulada (Linux ou WSL)

Com o `.venv` ativado:

```bash
./run_local.sh
```

O script cria, sem precisar de root, uma rede isolada com namespaces de rede do Linux: o gerenciador de
grupo e o servidor de comparação ficam em 10.0.0.1 (o endereço que está no `constMP.py`) e cada um dos
`N_PEERS` peers fica em um endereço próprio a partir de 10.0.0.11. Nos pacotes que saem de cada peer, o
`netem` acrescenta atraso de 50 ms com variação de 40 ms, o que faz pacotes chegarem fora de ordem. O
atraso, a variação e a perda de pacotes (`LOSS`, 0% por padrão) podem ser trocados por variáveis de
ambiente:

```bash
DELAY=80ms JITTER=60ms ./run_local.sh
```

O servidor de comparação roda no terminal e pede o número de mensagens. A saída de cada peer fica em
`logs/peer-<IP>.out`.

### Em máquinas separadas

Como no original: em `constMP.py`, coloque em `SERVER_ADDR` e `GROUPMNGR_ADDR` o IP da máquina dos
servidores e ajuste `N_PEERS`. Libere as portas TCP 5678, 5679 e 5680 e a porta UDP 6789. Na máquina dos
servidores, rode `python3 GroupMngr.py`. Em cada máquina de peer, rode `python3 peerCommunicatorUDP.py`
(o peer se registra com o IP público; para usar outro, passe o IP como argumento). Por último, rode
`python3 comparisonServer.py` na máquina dos servidores.
