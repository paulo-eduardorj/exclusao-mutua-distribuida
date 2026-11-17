# Exclusão Mútua Distribuída: Algoritmo Centralizado

Implementação em Python do algoritmo centralizado de exclusão mútua para sistemas distribuídos. Vários processos independentes disputam o acesso a um recurso compartilhado (o arquivo `resultado.txt`) e um **coordenador** garante que apenas um deles o utilize por vez.

## Fundamentação teórica

Em um sistema distribuído, vários processos executam de forma independente e concorrente, muitas vezes em máquinas diferentes, e se comunicam apenas por troca de mensagens. Quando mais de um processo precisa acessar um mesmo recurso (como um arquivo), o trecho de código que usa esse recurso é chamado de **região crítica**, e a **exclusão mútua** é a garantia de que no máximo um processo esteja dentro dela a cada instante, evitando conflitos como a escrita misturada de dados. No **algoritmo centralizado**, um dos processos assume o papel de **coordenador**: quem deseja entrar na região crítica envia uma mensagem `REQUEST`; o coordenador, quando o recurso está livre, responde com `GRANT`; e, ao sair, o processo avisa com `RELEASE`. Pedidos que chegam com o recurso ocupado esperam em uma **fila FIFO** (o primeiro a pedir é o primeiro a ser atendido), o que garante justiça e evita que algum processo fique esperando indefinidamente. A comunicação entre os processos é feita por **sockets TCP**, que oferecem um canal de bytes confiável entre duas pontas, e o coordenador usa **threads** (linhas de execução dentro de um mesmo programa) para fazer várias tarefas ao mesmo tempo, como aceitar conexões, receber mensagens e atender o terminal. Como as threads compartilham dados, é necessário protegê-los com **locks** (travas), garantindo que só uma thread os modifique por vez.

## Visão geral

```
Processo 1 ──REQUEST──▶ ┌──────────────┐
Processo 2 ──REQUEST──▶ │ COORDENADOR │   fila: [1, 2]
                        │              │
Processo 1 ◀──GRANT──── │ (libera o 1) │
Processo 1 escreve no resultado.txt
Processo 1 ──RELEASE──▶ │              │
Processo 2 ◀──GRANT──── │ (libera o 2) │
                        └──────────────┘
```

O sistema é composto por um coordenador e `n` processos. Todos rodam na mesma máquina e escrevem no mesmo `resultado.txt`, mas a comunicação já usa sockets TCP com IP e porta, então o mesmo código funcionaria com máquinas diferentes (bastaria que compartilhassem o arquivo).

## Estrutura de arquivos

```
exclusao_mutua/
├── mensagens.py            # formato das mensagens (montar e ler)
├── coordenador.py          # servidor: fila, algoritmo, log e interface
├── processo.py             # cliente: pede acesso e escreve no arquivo
├── executar_processos.py   # dispara n processos de uma vez
├── verificar.py            # confere automaticamente resultado.txt e o log
├── resultado.txt           # gerado: recurso compartilhado
└── coordenador.log         # gerado: registro das mensagens
```

| Arquivo                 | Responsabilidade                                                                                                |
| ----------------------- | --------------------------------------------------------------------------------------------------------------- |
| `mensagens.py`          | Concentra as regras de formato (10 bytes, separador `\|`) em um único lugar, usado por coordenador e processos. |
| `coordenador.py`        | Recebe conexões, mantém a fila, concede o acesso, gera o log e atende comandos do terminal.                     |
| `processo.py`           | Executa o laço de requisições à região crítica.                                                                 |
| `executar_processos.py` | Facilita os testes iniciando `n` instâncias de `processo.py`.                                                   |
| `verificar.py`          | Confere a corretude de uma execução, lendo `resultado.txt` e `coordenador.log`.                                 |

## Protocolo de mensagens

Todas as mensagens são strings de tamanho fixo de **10 bytes**, no formato:

```
TIPO|PROCESSO|ZEROS
```

| Tipo      | Código | Quem envia             | Exemplo (processo 3) |
| --------- | ------ | ---------------------- | -------------------- |
| `REQUEST` | 1      | Processo → Coordenador | `1\|3\|000000`       |
| `GRANT`   | 2      | Coordenador → Processo | `2\|3\|000000`       |
| `RELEASE` | 3      | Processo → Coordenador | `3\|3\|000000`       |

- O primeiro campo é o identificador do tipo da mensagem; o segundo, o identificador do processo (origem em `REQUEST` e `RELEASE`, destino em `GRANT`).
- Os zeros no final servem apenas para completar os 10 bytes. Como `1|` e `|` ocupam 3 bytes, o identificador do processo pode ter no máximo **7 dígitos**.

### `montar_mensagem(tipo, id_processo)`

Monta o texto `"tipo|id|"`, completa com zeros à direita até 10 caracteres (`ljust`) e converte para bytes (`.encode()`), que é o que o socket envia. Se o identificador for grande demais, levanta um erro.

### `ler_mensagem(conexao)`

Lê **exatamente 10 bytes** do socket e devolve uma tupla `(tipo, id_processo)`. Se a outra ponta fechar a conexão, devolve `None`.

> **Ponto importante:** o TCP é um _fluxo de bytes_, não de mensagens. Uma chamada a `recv(10)` pode devolver menos de 10 bytes. Por isso a função repete a leitura em um laço `while` até acumular os 10 bytes. Sem isso, o sistema funcionaria em testes simples e falharia de forma aleatória sob carga.

## O coordenador (`coordenador.py`)

### Threads

O enunciado pede três responsabilidades concorrentes. Há ainda um detalhe: como o coordenador atende vários processos, alguém precisa ficar lendo cada socket para perceber os `REQUEST` e `RELEASE`. Para manter o código simples, foi usada uma **thread leitora pequena por cliente**, que apenas faz leitura bloqueante.

| Thread                        | Função               | O que faz                                                                                                           |
| ----------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Conexões**                  | `receber_conexoes`   | Fica em `accept()`. Para cada novo cliente, cria uma thread leitora.                                                |
| **Leitora** (uma por cliente) | `atender_cliente`    | Lê mensagens do cliente. `REQUEST` coloca o processo na fila; `RELEASE` marca a região como livre.                  |
| **Algoritmo**                 | `executar_algoritmo` | Quando há alguém na fila **e** a região está livre, retira o primeiro, envia `GRANT` e marca a região como ocupada. |
| **Interface**                 | `executar_interface` | Fica bloqueada em `input()` aguardando comandos do terminal. Roda na thread principal.                              |

**Alternativa descartada:** uma única thread usando `select`/`poll` para monitorar todos os sockets. Ela evita as threads leitoras, mas é mais difícil de entender. Ler com `recv` bloqueante é bem mais intuitivo.

### Dados compartilhados

| Variável         | Tipo    | Finalidade                                                                                     |
| ---------------- | ------- | ---------------------------------------------------------------------------------------------- |
| `fila`           | `deque` | Fila FIFO de IDs de processos aguardando. `append()` entra no fim e `popleft()` sai do início. |
| `sockets`        | `dict`  | Mapeia `id do processo → socket`, para saber por qual conexão enviar o `GRANT`.                |
| `atendimentos`   | `dict`  | Conta quantas vezes cada processo recebeu `GRANT` (comando 2).                                 |
| `regiao_ocupada` | `bool`  | Indica se algum processo está na região crítica.                                               |
| `encerrando`     | `bool`  | Sinaliza que o usuário pediu para encerrar.                                                    |

Todos esses dados são protegidos por **um único lock**, a `threading.Condition` chamada `condicao`. Ela serve a dois propósitos:

1. **Exclusão mútua entre threads:** `with condicao:` garante que só uma thread mexa nos dados por vez.
2. **Espera sem gastar CPU:** a thread do algoritmo chama `condicao.wait()` e dorme até que outra thread chame `condicao.notify()`, em vez de ficar verificando a fila em um laço infinito.

O arquivo de log tem um lock separado (`trava_log`), pois sua escrita é independente dos demais dados.

### Thread leitora: `atender_cliente`

```
repete:
    lê uma mensagem do cliente
    registra no log (RECEBIDA)
    com o lock:
        guarda o socket do processo em sockets[id]
        se REQUEST: coloca o id no fim da fila
        se RELEASE: regiao_ocupada = False
        acorda a thread do algoritmo (notify)
```

O coordenador só descobre o ID de um cliente ao ler sua primeira mensagem. Por isso o socket é guardado em `sockets` a cada mensagem recebida (repetir a atribuição é inofensivo). O laço termina quando o cliente fecha a conexão ou ocorre um erro de socket.

### Thread do algoritmo: `executar_algoritmo`

```
repete:
    com o lock:
        enquanto (região ocupada ou fila vazia) e não encerrando: dorme (wait)
        se encerrando: termina
        id = retira o primeiro da fila
        região ocupada = True
        atendimentos[id] += 1
    registra no log (ENVIADA GRANT)
    envia GRANT ao processo
```

Pontos de atenção:

- **`while`, e não `if`, no `wait()`:** ao acordar, a condição precisa ser reavaliada, pois outra thread pode ter alterado o estado antes.
- **O envio fica fora do lock:** operações de rede podem demorar, e segurar o lock durante o envio travaria as outras threads sem necessidade.
- **O log é gravado antes do envio:** isso garante que, no arquivo de log, o `GRANT` apareça antes do `RELEASE` correspondente.

### Log

Cada mensagem enviada ou recebida gera uma linha em `coordenador.log`, feita pela função `registrar_log`:

```
2025-01-10 14:30:01.123 | RECEBIDA | REQUEST | processo 3
2025-01-10 14:30:01.125 | ENVIADA | GRANT | processo 3
2025-01-10 14:30:02.130 | RECEBIDA | RELEASE | processo 3
```

O formato é: instante (com milissegundos) | direção | tipo da mensagem | processo de origem ou destino. O `%f` do `strftime` gera microssegundos (6 dígitos) e o `[:-3]` corta os 3 últimos, sobrando os milissegundos.

### Interface de terminal

Comandos aceitos pela thread de interface:

| Comando | Ação                                              |
| ------- | ------------------------------------------------- |
| `1`     | Imprime a fila de pedidos atual.                  |
| `2`     | Imprime quantas vezes cada processo foi atendido. |
| `3`     | Encerra a execução.                               |

Ao ler fila e contadores, a interface usa o lock, para que nenhuma outra thread os modifique durante a leitura. O comando `3` ativa `encerrando`, acorda a thread do algoritmo com `notify_all()` e, em seguida, a função `main` fecha o servidor e todos os sockets.

### Função principal: `main`

1. Cria o socket do servidor, com `SO_REUSEADDR` (permite reiniciar o coordenador logo após fechá-lo, sem o erro "porta em uso").
2. Associa o socket ao endereço e à porta (`bind`) e começa a escutar (`listen`).
3. Inicia as threads de conexões e do algoritmo (ambas `daemon=True`, para morrerem junto com o programa).
4. Executa a interface na thread principal.
5. Ao sair da interface, fecha o servidor e os sockets dos clientes.

## O processo (`processo.py`)

Recebe cinco parâmetros pela linha de comando: `id`, `ip`, `porta`, `r` (número de repetições) e `k` (segundos de espera). Possui **um único socket**, conectado ao coordenador, e não usa threads.

```
conecta ao coordenador
repete r vezes:
    envia REQUEST
    espera GRANT                      (recv bloqueante)
    abre resultado.txt em modo append
    escreve "id hora-com-ms"
    fecha o arquivo
    sleep(k)
    envia RELEASE
fecha o socket
```

- Cada volta do laço é um ciclo completo: **REQUEST → espera GRANT → região crítica → RELEASE**.
- Como o coordenador só envia `GRANT` a este processo quando é a vez dele, o processo não precisa conferir o tipo da mensagem recebida.
- A região crítica é a escrita no arquivo somada ao `sleep(k)`. O `RELEASE` só é enviado **depois** do `sleep`, como define o enunciado.
- O `with open(..., "a")` abre o arquivo em modo append e o fecha automaticamente ao final do bloco.

## O script de execução (`executar_processos.py`)

Recebe `n`, `r` e `k` e inicia `n` cópias de `processo.py`, com IDs de 1 a `n`. O `subprocess.Popen` não espera o término do processo, então todos rodam ao mesmo tempo. Depois, `wait()` aguarda cada um terminar. O uso de `sys.executable` garante que seja usado o mesmo Python que está executando o script.

## O script de verificação (`verificar.py`)

Automatiza as conferências que o enunciado pede para a etapa de avaliação, evitando contar linhas e comparar horários e logs manualmente. Ele lê `resultado.txt` e `coordenador.log` de uma execução e imprime `[OK]` ou `[FALHOU]` para cada verificação.

**Uso:**

```bash
python verificar.py <n> <r> [k]
```

`n` e `r` são os mesmos valores usados na execução; `k` é opcional e habilita a verificação do intervalo mínimo entre escritas.

### Verificações realizadas

| Arquivo           | Verificação                                                                    | Por que importa                                                                           |
| ----------------- | ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `resultado.txt`   | Total de linhas igual a `n × r`                                                | Nenhum acesso foi perdido ou duplicado                                                    |
| `resultado.txt`   | Horários em ordem crescente                                                    | As escritas respeitam a evolução do relógio do sistema                                    |
| `resultado.txt`   | Cada processo escreveu `r` vezes                                               | Todos foram atendidos, sem privilegiar nem esquecer ninguém                               |
| `resultado.txt`   | Escritas consecutivas separadas por pelo menos `k` s (só se `k` for informado) | Prova indireta da exclusão mútua: dois processos nunca estiveram juntos na região crítica |
| `coordenador.log` | `REQUEST`, `GRANT` e `RELEASE` somam `n × r` cada                              | O ciclo completo ocorreu para todos os acessos                                            |
| `coordenador.log` | Cada processo recebeu `r` `GRANT`s                                             | Contagem de atendimentos por processo correta                                             |
| `coordenador.log` | `GRANT` e `RELEASE` sempre intercalados, do mesmo processo                     | Nunca houve dois processos na região crítica ao mesmo tempo                               |
| `coordenador.log` | Ordem dos `REQUEST` igual à ordem dos `RELEASE`                                | Verificação exigida pelo enunciado                                                        |
| `coordenador.log` | Ordem dos `GRANT` igual à ordem dos `REQUEST`                                  | A fila FIFO foi respeitada                                                                |

### Como o script está organizado

| Função                       | O que faz                                                                                |
| ---------------------------- | ---------------------------------------------------------------------------------------- |
| `conferir`                   | Imprime `[OK]` ou `[FALHOU]` e conta as falhas.                                          |
| `ler_resultado` / `ler_log`  | Transformam cada arquivo em listas de dados: `(id, hora em ms)` e `(direção, tipo, id)`. |
| `verificar_resultado`        | Executa as conferências sobre `resultado.txt`.                                           |
| `grant_release_intercalados` | Filtra do log só os `GRANT` e `RELEASE` e confere se alternam, começando por `GRANT`.    |
| `verificar_log`              | Executa as conferências sobre `coordenador.log`.                                         |

Os horários são convertidos para milissegundos desde a meia-noite, o que simplifica as comparações. O código de saída é `0` se tudo passou e `1` se alguma verificação falhou, o que permite usar o script em automações.

**Observações:**

- Rode o script **depois** que todos os processos terminarem e **apenas com os arquivos de uma execução** (apague-os entre testes).
- O `resultado.txt` guarda só `hora:minuto:segundo.ms`, sem a data. Uma execução que atravesse a meia-noite seria reportada como falha de ordenação dos horários.

## Fluxo completo de um exemplo

Dois processos (1 e 2) pedem acesso quase ao mesmo tempo:

| Passo | Evento                                      | `fila` | `regiao_ocupada` |
| ----- | ------------------------------------------- | ------ | ---------------- |
| 1     | Chega `REQUEST` do processo 1               | `[1]`  | falso            |
| 2     | Algoritmo retira o 1 e envia `GRANT`        | `[]`   | verdadeiro       |
| 3     | Chega `REQUEST` do processo 2               | `[2]`  | verdadeiro       |
| 4     | Processo 1 escreve no arquivo, espera `k` s | `[2]`  | verdadeiro       |
| 5     | Chega `RELEASE` do processo 1               | `[2]`  | falso            |
| 6     | Algoritmo retira o 2 e envia `GRANT`        | `[]`   | verdadeiro       |
| 7     | Processo 2 escreve, espera, envia `RELEASE` | `[]`   | falso            |

## Como executar

### Requisitos

- Python 3.8 ou superior (apenas a biblioteca padrão; não há nada para instalar).
- Todos os arquivos `.py` na mesma pasta.
- Em alguns sistemas (Linux/macOS), o comando pode ser `python3` em vez de `python`.

### Passo a passo

**1. Terminal 1: inicie o coordenador**

```bash
python coordenador.py
```

O coordenador passa a escutar na porta `5000` e exibe o prompt de comandos.

**2. Terminal 2: inicie os processos**

```bash
python executar_processos.py <n> <r> <k>
```

| Parâmetro | Significado                                 |
| --------- | ------------------------------------------- |
| `n`       | Quantidade de processos                     |
| `r`       | Quantidade de repetições por processo       |
| `k`       | Segundos de espera dentro da região crítica |

Exemplo com 5 processos, 3 repetições cada e 1 segundo de espera:

```bash
python executar_processos.py 5 3 1
```

Também é possível iniciar um processo isoladamente:

```bash
python processo.py <id> <ip> <porta> <r> <k>
python processo.py 1 127.0.0.1 5000 3 1
```

**3. Verifique os resultados**

Quando os processos terminarem, ainda no Terminal 2, rode o verificador com os mesmos valores de `n` e `r` (e, opcionalmente, `k`):

```bash
python verificar.py 5 3 1
```

**4. Encerrar**

No Terminal 1, digite `3`. O coordenador fecha os sockets e termina.

## Como testar

> Apague `resultado.txt` e `coordenador.log` entre os testes, pois ambos são abertos em modo append.
>
> Após cada execução, rode `python verificar.py <n> <r> <k>`: ele faz automaticamente as conferências descritas abaixo.

### Teste 1: funcionamento básico

Execute `python executar_processos.py 5 3 1` e aguarde (cerca de 15 segundos, pois são 15 acessos de 1 s cada). Verifique:

1. **`resultado.txt`** deve ter **15 linhas** (5 processos × 3 repetições), cada uma no formato `id hora`, por exemplo:
   ```
   2 14:30:01.125
   5 14:30:02.131
   1 14:30:03.140
   ```
2. **Os horários** devem estar separados por **pelo menos `k` segundo(s)**. Se duas linhas tiverem horários mais próximos que isso, houve violação da exclusão mútua.
3. **`coordenador.log`** deve seguir sempre o padrão `RECEBIDA REQUEST` → `ENVIADA GRANT` → `RECEBIDA RELEASE`, sem dois `GRANT` seguidos sem um `RELEASE` entre eles.

Para conferir tudo de uma vez, execute `python verificar.py 5 3 1`. A saída esperada é `[OK]` em todas as linhas e a mensagem final `RESULTADO: todas as verificações passaram.`

### Teste 2: comandos da interface

Com os processos rodando, volte ao Terminal 1 e digite:

- `1`: deve mostrar a fila mudando (por exemplo, `[3, 5, 2]`).
- `2`: ao final da execução, cada processo deve aparecer com `3 vez(es)`.
- `3`: o coordenador deve encerrar sem erros.

### Teste 3: estresse

Execute com mais processos e tempo menor, por exemplo:

```bash
python executar_processos.py 20 5 0.2
```

Em seguida, rode `python verificar.py 20 5 0.2` (agora com 100 linhas esperadas). Esse cenário aumenta a chance de expor problemas de concorrência, caso existam.

### Teste 4: justiça da fila (FIFO)

No `coordenador.log`, confira que a ordem dos `GRANT` enviados é a mesma ordem dos `REQUEST` recebidos (ignorando os pedidos já atendidos). Isso confirma que a fila FIFO está sendo respeitada. O `verificar.py` faz essa conferência automaticamente.

### Resumo das verificações

| Propriedade      | Como verificar                                                                              |
| ---------------- | ------------------------------------------------------------------------------------------- |
| Exclusão mútua   | Horários no `resultado.txt` separados por pelo menos `k` s; sem `GRANT` consecutivos no log |
| Completude       | Total de linhas em `resultado.txt` igual a `n × r`                                          |
| Justiça (FIFO)   | Ordem dos `GRANT` igual à ordem dos `REQUEST` no log                                        |
| Contagem correta | Comando `2` mostra `r` atendimentos para cada processo                                      |

O `verificar.py` confere automaticamente todas as propriedades desta tabela (a contagem por processo é verificada a partir do log, em vez do comando `2`).
