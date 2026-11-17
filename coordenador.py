import socket
import threading
from collections import deque
from datetime import datetime

from mensagem import (REQUEST, GRANT, RELEASE, NOMES_TIPOS,
                       montar_mensagem, ler_mensagem)

ENDERECO = "0.0.0.0"
PORTA = 5000

fila = deque()
sockets = {} # mapeia id_processo -> socket para saber por qual conexão enviar o GRANT
atendimentos = {} # conta quantas vezes cada processo recebeu GRANT
regiao_ocupada = False # indica se há processo na região crítica
encerrando = False

condicao = threading.Condition() # lock de dados, usado para proteger fila, sockets, atendimentos e regiao_ocupada
trava_log = threading.Lock() # lock separado para a escrita no arquivo de log


# chamada sempre que o coordenador envia ou recebe uma mensagem, para registrar no log
def registrar_log(direcao, tipo, id_processo):
    instante = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    linha = f"{instante} | {direcao} | {NOMES_TIPOS[tipo]} | processo {id_processo}\n"
    with trava_log:
        with open("coordenador.log", "a") as arquivo:
            arquivo.write(linha)


# bloqueia até um processo se conectar e cria a thread atender_cliente para atendê-lo
def receber_conexoes(servidor):
    while True:
        try:
            conexao, _ = servidor.accept()
        except OSError:
            break  # servidor foi fechado
        threading.Thread(target=atender_cliente, args=(conexao,), daemon=True).start()


# thread que atende um cliente, lendo mensagens e atualizando a fila de processos
def atender_cliente(conexao):
    global regiao_ocupada
    while True:
        try:
            mensagem = ler_mensagem(conexao)
        except OSError:
            break
        if mensagem is None:
            break

        tipo, id_processo = mensagem

        with condicao:
            registrar_log("RECEBIDA", tipo, id_processo)
            sockets[id_processo] = conexao
            if tipo == REQUEST:
                fila.append(id_processo)
            elif tipo == RELEASE:
                regiao_ocupada = False
            condicao.notify()

    conexao.close()


# thread que executa o algoritmo de exclusão mútua, enviando GRANT para processos na fila
def executar_algoritmo():
    global regiao_ocupada
    while True:
        with condicao:
            while not encerrando and (regiao_ocupada or not fila):
                condicao.wait()
            if encerrando:
                return
            id_processo = fila.popleft()
            regiao_ocupada = True
            atendimentos[id_processo] = atendimentos.get(id_processo, 0) + 1
            conexao = sockets[id_processo]

        registrar_log("ENVIADA", GRANT, id_processo)
        conexao.sendall(montar_mensagem(GRANT, id_processo))


# thread de interface com o usuário, que permite ver a fila, os atendimentos e encerrar o coordenador
def executar_interface():
    global encerrando
    while True:
        comando = input("Comando (1=fila, 2=atendimentos, 3=encerrar): ").strip()
        if comando == "1":
            with condicao:
                print("Fila atual:", list(fila))
        elif comando == "2":
            with condicao:
                for id_processo, total in sorted(atendimentos.items()):
                    print(f"Processo {id_processo}: atendido {total} vez(es)")
        elif comando == "3":
            with condicao:
                encerrando = True
                condicao.notify_all()
            break


# thread principal, que cria o socket do servidor, inicia as threads e aguarda a interface encerrar
# ao encerrar, fecha o socket do servidor e todas as conexões com os clientes
# O coordenador encerra quando o usuário digita 3 na interface e não encerra sozinho, mesmo que todos os clientes se desconectem
def main():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((ENDERECO, PORTA))
    servidor.listen()
    print(f"Coordenador ouvindo na porta {PORTA}")

    threading.Thread(target=receber_conexoes, args=(servidor,), daemon=True).start()
    threading.Thread(target=executar_algoritmo, daemon=True).start()

    executar_interface()

    servidor.close()
    with condicao:
        for conexao in sockets.values():
            conexao.close()
    print("Coordenador encerrado.")


main()