import os
import shutil

def MoveArquivosCNPJ():
    dirStaging = '/home/uoperator/workdir/staging/'
    dirProcessing = '/home/uoperator/workdir/processing/raw/'

    #senão existir, cria a pasta
    if not os.path.exists(dirProcessing):
        os.makedirs(dirProcessing)

    try:
        for dirAbsoluto, subDir, arquivos in sorted(os.walk(dirStaging)):
            if dirAbsoluto.split("/")[-1].startswith('CNPJ'):
                for arquivo in arquivos:
                    if arquivo.upper().endswith('.CSV'):
                        arqOrigem = os.path.join(dirAbsoluto, arquivo)
                        arqDestino = arqOrigem.replace(dirStaging, dirProcessing, 1)
                        if not os.path.exists(os.path.dirname(arqDestino)):
                            os.makedirs(os.path.dirname(arqDestino))
                            print(f'Criação de {os.path.dirname(arqDestino)}')
                        print(f' INFO: Movendo {arqDestino}')
                        shutil.move(arqOrigem, arqDestino)
                if os.path.dirname(arqOrigem):
                    shutil.rmtree(dirAbsoluto, ignore_errors=True)
                    print(f'Deletado {dirAbsoluto}')
        print("INFO: Movimentação concluída")
    except:
        print("ERRO: Falha na movimentação")

if __name__ == "__main__":
    MoveArquivosCNPJ()
    print('INFO: Finished')
