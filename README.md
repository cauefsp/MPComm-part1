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
peers continua em sockets UDP, mas agora trata a perda de mensagens, com confirmação e retransmissão, e
entrega as mensagens de cada remetente na ordem em que ele as enviou.

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
| Peers trocam handshakes, mensagens e aviso de fim | UDP, sem tratamento | UDP com confirmação, retransmissão e ordem FIFO |

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
virou `N_PEERS`. Para o item (b) entrou o `RETRANSMIT_TIMEOUT`, o tempo (0,3 s) que o peer espera
pela confirmação de uma mensagem antes de reenviá-la. O peer também aceita, como argumento opcional, o IP com que deve se registrar. Sem o
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

## Parte 1b: perda de mensagens e ordem FIFO

### O problema na versão original

O multicast entre os peers era feito com `sendto` em UDP para cada peer, sem nenhum tratamento. Como o
UDP não garante entrega nem ordem, vários problemas podiam acontecer. Um `READY` perdido deixava todos
os peers esperando para sempre pelo handshake que faltava. Um aviso de fim `(-1, -1)` perdido impedia o
peer de sair do laço e de mandar o log. Uma mensagem de dados perdida simplesmente sumia do log, e
mensagens do mesmo remetente podiam ser registradas fora da ordem de envio, já que o `run_local.sh`
aplica atraso variável e reordena pacotes. Além disso, o laço de handshake lia o socket e descartava
tudo o que não era `READY`, então mensagens de dados que chegavam antes do último `READY` se perdiam
mesmo sem perda na rede. Em um teste de duas rodadas sem perda, os logs da segunda rodada ficaram com
298 a 300 mensagens em vez de 300.

### Multicast com números de sequência

Tudo o que o peer envia aos outros (`READY`, dados e aviso de fim) passa pela função `multicast`. Ela
dá à mensagem o próximo número de sequência do remetente, monta o pacote `('MSG', sender, seq, content)`
com pickle e envia a todos os peers, inclusive ao próprio. Para cada destino, o par `(peer_ip, seq)`
entra em uma tabela de pacotes sem confirmação, junto com o pacote e o horário de envio. A tabela é
protegida por um lock, e a inserção acontece antes do envio, dentro do lock, para que uma confirmação
muito rápida não chegue antes de a entrada existir.

O conteúdo das mensagens passou a ser `'READY'`, o número da mensagem (um inteiro) ou `'STOP'`, que
substitui o antigo `(-1, -1)`.

### Retransmissão

A thread `retransmit_loop` roda durante toda a vida do peer. A cada `RETRANSMIT_TIMEOUT / 2`, ela
percorre a tabela e reenvia os pacotes que estão sem confirmação há mais de `RETRANSMIT_TIMEOUT`,
atualizando o horário de envio. Cada reenvio aparece na saída como
`Retransmitting message <seq> to <ip>`. Um pacote só sai da tabela quando chega a confirmação daquele
destino, então ele é reenviado quantas vezes for preciso, tanto quando o pacote se perde quanto quando
a confirmação se perde.

### Recepção

A thread `receive_loop` também é iniciada quando o programa começa e lê o socket durante toda a vida do
peer. Para cada pacote recebido:

1. Se for `('ACK', seq)`, remove `(ip de origem, seq)` da tabela de pacotes sem confirmação.
2. Se for uma mensagem, responde `('ACK', seq)` ao endereço de origem. Isso é feito sempre, mesmo para
   mensagens repetidas, porque uma repetição normalmente significa que o ACK anterior se perdeu.
3. Descarta a mensagem se `seq` for menor que o próximo número esperado daquele remetente (ela já foi
   entregue) ou se ela já estiver guardada.
4. Se `seq` for maior que o esperado, a mensagem chegou adiantada: fica guardada e aparece na saída
   como `Holding back message <seq> from process <sender> while waiting for <esperado>`.
5. Se for a esperada, ela é entregue, junto com as seguintes que já estavam guardadas, em ordem, em uma
   fila de mensagens entregues.

Assim, a aplicação vê as mensagens de cada remetente na ordem de envio, sem buracos e sem repetição.

### Um socket só

Na versão original o peer enviava por um socket e recebia por outro. Agora há um único socket UDP,
ligado à `PEER_UDP_PORT`, usado para enviar e receber. Com isso, o endereço de origem de toda mensagem
é a porta do peer que a enviou, e o ACK mandado de volta para esse endereço chega à thread de recepção
dele. Com dois sockets, o ACK iria para a porta efêmera do socket de envio, que ninguém lê.

### A sequência não recomeça a cada rodada

Os números de sequência continuam de uma rodada para a outra. Se recomeçassem do zero, uma
retransmissão atrasada da rodada anterior poderia ser confundida com uma mensagem nova da rodada
seguinte. Como a sequência só cresce, essa retransmissão é apenas uma mensagem repetida e é descartada.
Também pode acontecer de um peer receber o `READY` de outro antes de receber o próprio `Start`. Como a
thread de recepção já está rodando, esse `READY` é entregue normalmente e fica na fila até a thread da
rodada começar a ler.

### O laço da rodada

A aplicação só lê da fila de mensagens entregues. A cada rodada, a thread `handle_messages` trata tudo
em um único laço: conta os `READY` e, quando chega a `N_PEERS`, sinaliza um `threading.Event`, que a
thread principal espera antes de enviar os dados (no lugar da espera ativa
`while handshake_count < N_PEERS: pass`); conta os `STOP` até `N_PEERS`; e registra as mensagens de
dados como `(sender, number)`. Como o `READY` é a primeira mensagem de cada remetente na rodada e o
`STOP` é a última, a ordem FIFO garante que nenhum dado de um remetente é entregue antes do `READY` dele
ou depois do `STOP` dele. Por isso nenhuma mensagem de dados é descartada durante o handshake, como
acontecia no original. No fim, a thread grava `logfile<ID>.log` e chama `SendLog`, como antes.

### Novas métricas no servidor de comparação

Antes da métrica original, o servidor de comparação mostra, para cada peer,
`Peer <id>: <n> lost messages, <m> FIFO violations`. As mensagens perdidas são `N_PEERS * n_msgs`
menos o número de mensagens distintas no log. Uma violação FIFO é uma mensagem registrada depois de
outra de número maior do mesmo remetente.

A métrica original, `unordered message rounds`, não mudou e continua acima de zero. Ela compara a
posição j do log de cada peer, ou seja, verifica se todos receberam as mensagens na mesma ordem global.
A ordem FIFO vale só por remetente: duas mensagens de remetentes diferentes ainda podem chegar em
ordens diferentes a peers diferentes. Garantir a mesma ordem para todos é ordenação total, que fica
para a Parte 3.

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
atraso, a variação e a perda de pacotes (`LOSS`, 10% por padrão) podem ser trocados por variáveis de
ambiente:

```bash
LOSS=20% DELAY=80ms JITTER=60ms ./run_local.sh
```

O servidor de comparação roda no terminal e pede o número de mensagens. A saída de cada peer fica em
`logs/peer-<IP>.out`, onde aparecem as linhas `Holding back` e `Retransmitting`.

### Em máquinas separadas

Como no original: em `constMP.py`, coloque em `SERVER_ADDR` e `GROUPMNGR_ADDR` o IP da máquina dos
servidores e ajuste `N_PEERS`. Libere as portas TCP 5678, 5679 e 5680 e a porta UDP 6789. Na máquina dos
servidores, rode `python3 GroupMngr.py`. Em cada máquina de peer, rode `python3 peerCommunicatorUDP.py`
(o peer se registra com o IP público; para usar outro, passe o IP como argumento). Por último, rode
`python3 comparisonServer.py` na máquina dos servidores.

Para emular perda de pacotes em uma máquina de peer, troque `eth0` pela interface de rede dela, se
for outra:

```bash
sudo tc qdisc add dev eth0 root netem loss 10%
```

## Resultado

Com `./run_local.sh` (6 peers, atraso de 50 ms com variação de 40 ms e 10% de perda) e 100 mensagens
por peer, o servidor de comparação mostrou:

```
Peer 0: 0 lost messages, 0 FIFO violations
Peer 1: 0 lost messages, 0 FIFO violations
Peer 2: 0 lost messages, 0 FIFO violations
Peer 3: 0 lost messages, 0 FIFO violations
Peer 4: 0 lost messages, 0 FIFO violations
Peer 5: 0 lost messages, 0 FIFO violations
Found 99 unordered message rounds
```

Todos os `logfile*.log` ficaram com 600 mensagens, sem repetição, e todos os processos terminaram
sozinhos. Nesse teste, cada peer fez entre 101 e 129 retransmissões e guardou entre 230 e 281 mensagens
que chegaram adiantadas.

Com 20% de perda e duas rodadas seguidas (50 e 100 mensagens), e com 10% de perda e 1000 mensagens por
peer, o resultado foi o mesmo: nenhuma mensagem perdida, nenhuma violação FIFO e todos os processos
terminando sozinhos. No teste com 1000 mensagens, cada peer fez entre 1135 e 1199 retransmissões e
guardou entre 2457 e 2624 mensagens.
