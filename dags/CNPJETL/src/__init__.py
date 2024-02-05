from CNPJETL.src.moveProcessing import MoveArquivosCNPJ
from CNPJETL.src.convertCsvParquet import transformaCsvParquet
from CNPJETL.src.mountingDataFra import montarIngeriDataframes
from CNPJETL.src.returnDirName import retornaReg
from CNPJETL.src.sendToDatabasePg import persisteBasePg
import airflow


# import returnDirName
# import mountingDataFra
# import sendToDatabasePg
# import airflow

def main():
    MoveArquivosCNPJ()
    transformaCsvParquet()
    montarIngeriDataframes()
    
if __name__ == "__main__":
    main()
