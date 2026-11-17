TAMANHO_MENSAGEM = 10

REQUEST = 1
GRANT = 2
RELEASE = 3

NOMES_TIPOS = {REQUEST: "REQUEST", GRANT: "GRANT", RELEASE: "RELEASE"}

# gera b"tipo|id_processo|0000" com tamanho 10, preenchendo com zeros à direita
def montar_mensagem(tipo, id_processo):
    texto = f"{tipo}|{id_processo}|"
    if len(texto) > TAMANHO_MENSAGEM:
        raise ValueError("Identificador do processo grande demais")
    return texto.ljust(TAMANHO_MENSAGEM, "0").encode() # converte para bytes e preenche com zeros à direita

# lê uma mensagem de 10 bytes da conexão e retorna (tipo, id_processo) ou None se a conexão foi fechada
def ler_mensagem(conexao):
    dados = b""
    # O TCP pode entregar menos bytes que o pedido, então repetimos até completar 10
    while len(dados) < TAMANHO_MENSAGEM:
        pedaco = conexao.recv(TAMANHO_MENSAGEM - len(dados))
        if not pedaco:
            return None  # a outra ponta fechou a conexão
        dados += pedaco
    tipo, id_processo, _ = dados.decode().split("|")
    return int(tipo), int(id_processo)