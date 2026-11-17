import sys
from collections import Counter

ARQUIVO_RESULTADO = "resultado.txt"
ARQUIVO_LOG = "coordenador.log"

falhas = 0


def conferir(descricao, passou, detalhe=""):
    global falhas
    if passou:
        print(f"[OK]     {descricao}")
    else:
        falhas += 1
        print(f"[FALHOU] {descricao} {detalhe}")


def hora_para_ms(hora):
    horas, minutos, resto = hora.split(":")
    segundos, ms = resto.split(".")
    return ((int(horas) * 60 + int(minutos)) * 60 + int(segundos)) * 1000 + int(ms)


def ler_resultado():
    registros = []
    with open(ARQUIVO_RESULTADO) as arquivo:
        for linha in arquivo:
            id_processo, hora = linha.split()
            registros.append((int(id_processo), hora_para_ms(hora)))
    return registros


def ler_log():
    eventos = []
    with open(ARQUIVO_LOG) as arquivo:
        for linha in arquivo:
            _, direcao, tipo, processo = [campo.strip() for campo in linha.split("|")]
            eventos.append((direcao, tipo, int(processo.split()[1])))
    return eventos


def verificar_resultado(n, r, k):
    print(f"\n--- {ARQUIVO_RESULTADO} ---")
    try:
        registros = ler_resultado()
    except (OSError, ValueError):
        conferir("Arquivo existe e todas as linhas têm o formato 'id hora'", False)
        return

    horarios = [hora for _, hora in registros]
    esperado = {id_processo: r for id_processo in range(1, n + 1)}
    contagem = Counter(id_processo for id_processo, _ in registros)

    conferir(f"Número de linhas é n*r = {n * r}",
             len(registros) == n * r, f"(encontradas: {len(registros)})")
    conferir("Horários em ordem crescente", horarios == sorted(horarios))
    conferir(f"Cada processo escreveu {r} vez(es)",
             dict(contagem) == esperado, f"(encontrado: {dict(sorted(contagem.items()))})")

    if k is not None:
        minimo_ms = round(k * 1000)
        # Duas escritas consecutivas devem ficar separadas por pelo menos k segundos
        intervalos_ok = all(b - a >= minimo_ms for a, b in zip(horarios, horarios[1:]))
        conferir(f"Escritas consecutivas separadas por pelo menos {k} s", intervalos_ok)


def grant_release_intercalados(eventos):
    sequencia = [(tipo, id_processo) for _, tipo, id_processo in eventos
                 if tipo in ("GRANT", "RELEASE")]
    if len(sequencia) % 2 != 0:
        return False
    for posicao, (tipo, id_processo) in enumerate(sequencia):
        if posicao % 2 == 0:
            if tipo != "GRANT":
                return False
        # O RELEASE deve vir do mesmo processo que recebeu o GRANT anterior
        elif tipo != "RELEASE" or id_processo != sequencia[posicao - 1][1]:
            return False
    return True


def verificar_log(n, r):
    print(f"\n--- {ARQUIVO_LOG} ---")
    try:
        eventos = ler_log()
    except (OSError, ValueError, IndexError):
        conferir("Arquivo existe e todas as linhas têm o formato esperado", False)
        return

    requests = [i for d, t, i in eventos if d == "RECEBIDA" and t == "REQUEST"]
    grants = [i for d, t, i in eventos if d == "ENVIADA" and t == "GRANT"]
    releases = [i for d, t, i in eventos if d == "RECEBIDA" and t == "RELEASE"]
    esperado = {id_processo: r for id_processo in range(1, n + 1)}

    conferir(f"Quantidade de REQUEST, GRANT e RELEASE é n*r = {n * r}",
             len(requests) == len(grants) == len(releases) == n * r,
             f"(REQUEST={len(requests)}, GRANT={len(grants)}, RELEASE={len(releases)})")
    conferir(f"Cada processo recebeu {r} GRANT(s)", dict(Counter(grants)) == esperado)
    conferir("GRANT e RELEASE sempre intercalados (mesmo processo)",
             grant_release_intercalados(eventos))
    conferir("Ordem dos REQUEST igual à ordem dos RELEASE", requests == releases)
    conferir("Ordem dos GRANT igual à ordem dos REQUEST (FIFO)", grants == requests)


def main():
    if len(sys.argv) < 3:
        print("Uso: python verificar.py <n> <r> [k]")
        sys.exit(1)

    n = int(sys.argv[1])
    r = int(sys.argv[2])
    k = float(sys.argv[3]) if len(sys.argv) > 3 else None

    verificar_resultado(n, r, k)
    verificar_log(n, r)

    print()
    if falhas == 0:
        print("RESULTADO: todas as verificações passaram.")
    else:
        print(f"RESULTADO: {falhas} verificação(ões) falharam.")
    sys.exit(1 if falhas else 0)


main()