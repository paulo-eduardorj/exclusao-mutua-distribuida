import subprocess
import sys

IP = "127.0.0.1"
PORTA = 5000

n = int(sys.argv[1]) # número de processos
r = sys.argv[2] # número de repetições
k = sys.argv[3] # número de segundos que cada processo espera na região crítica

processos = []
for id_processo in range(1, n + 1):
    comando = [sys.executable, "processo.py", str(id_processo), IP, str(PORTA), r, k]
    processos.append(subprocess.Popen(comando))

for processo in processos:
    processo.wait()

print("Todos os processos terminaram.")