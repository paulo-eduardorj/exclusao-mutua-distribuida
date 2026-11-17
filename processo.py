import socket
import sys
import time
from datetime import datetime

from mensagem import REQUEST, RELEASE, montar_mensagem, ler_mensagem

# abre em modo append e fecha o arquivo automaticamente ao sair do bloco with
def escrever_resultado(id_processo):
    hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with open("resultado.txt", "a") as arquivo:
        arquivo.write(f"{id_processo} {hora}\n")

# recebe 5 parâmetros via linha de comando: id_processo, ip, porta, r (repetições), k (segundos)
def main():
    id_processo = int(sys.argv[1])
    ip = sys.argv[2]
    porta = int(sys.argv[3])
    repeticoes = int(sys.argv[4])
    segundos = float(sys.argv[5])

    conexao = socket.create_connection((ip, porta))

    # envia REQUEST, espera GRANT, escreve resultado, espera k segundos e envia RELEASE
    for _ in range(repeticoes):
        conexao.sendall(montar_mensagem(REQUEST, id_processo))

        if ler_mensagem(conexao) is None:  # espera o GRANT
            print("Coordenador encerrou a conexão.")
            break

        # região crítica: escreve no arquivo resultado.txt e espera k segundos
        escrever_resultado(id_processo)
        time.sleep(segundos)

        conexao.sendall(montar_mensagem(RELEASE, id_processo))

    conexao.close()


main()