import queue
import socket
import threading
from collections import deque
from datetime import datetime

from mensagem import (REQUEST, GRANT, RELEASE, NOMES_TIPOS,
                       montar_mensagem, ler_mensagem)

ENDERECO = "0.0.0.0"
PORTA = 5000

caixa_de_entrada = queue.Queue()  # mensagens recebidas, na ordem de chegada
fila = deque()  # o primeiro da fila é quem está na região crítica
sockets = {}
atendimentos = {}

trava_dados = threading.Lock()  # protege fila e atendimentos, que a interface também lê


def registrar_log(direcao, tipo, id_processo):
    instante = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    linha = f"{instante} | {direcao} | {NOMES_TIPOS[tipo]} | processo {id_processo}\n"
    with open("coordenador.log", "a") as arquivo:
        arquivo.write(linha)


def receber_conexoes(servidor):
    while True:
        try:
            conexao, _ = servidor.accept()
        except OSError:
            break  # servidor foi fechado
        threading.Thread(target=atender_cliente, args=(conexao,), daemon=True).start()


def atender_cliente(conexao):
    while True:
        try:
            mensagem = ler_mensagem(conexao)
        except OSError:
            break
        if mensagem is None:
            break

        tipo, id_processo = mensagem
        caixa_de_entrada.put((conexao, tipo, id_processo))

    conexao.close()


def enviar_grant(id_processo):
    registrar_log("ENVIADA", GRANT, id_processo)
    sockets[id_processo].sendall(montar_mensagem(GRANT, id_processo))
    with trava_dados:
        atendimentos[id_processo] = atendimentos.get(id_processo, 0) + 1


def executar_algoritmo():
    while True:
        item = caixa_de_entrada.get()
        if item is None:
            return  # sinal de encerramento

        conexao, tipo, id_processo = item
        registrar_log("RECEBIDA", tipo, id_processo)
        sockets[id_processo] = conexao

        if tipo == REQUEST:
            # Só esta thread altera a fila, então ler sem a trava é seguro
            if not fila:
                enviar_grant(id_processo)
            with trava_dados:
                fila.append(id_processo)

        elif tipo == RELEASE:
            with trava_dados:
                fila.popleft()
                proximo = fila[0] if fila else None
            if proximo is not None:
                enviar_grant(proximo)


def executar_interface():
    while True:
        comando = input("Comando (1=fila, 2=atendimentos, 3=encerrar): ").strip()
        if comando == "1":
            with trava_dados:
                print("Fila atual (o primeiro está na região crítica):", list(fila))
        elif comando == "2":
            with trava_dados:
                for id_processo, total in sorted(atendimentos.items()):
                    print(f"Processo {id_processo}: atendido {total} vez(es)")
        elif comando == "3":
            break


def main():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((ENDERECO, PORTA))
    servidor.listen()
    print(f"Coordenador ouvindo na porta {PORTA}")

    threading.Thread(target=receber_conexoes, args=(servidor,), daemon=True).start()
    thread_algoritmo = threading.Thread(target=executar_algoritmo)
    thread_algoritmo.start()

    executar_interface()

    caixa_de_entrada.put(None)
    thread_algoritmo.join()
    servidor.close()
    for conexao in sockets.values():
        conexao.close()
    print("Coordenador encerrado.")


main()