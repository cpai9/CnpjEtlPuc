from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ShortType, DoubleType, ByteType, LongType
import os
from shutil import rmtree
from CNPJETL.src.returnDirName import retornaReg

dirRaws = '/home/uoperator/workdir/processing/raw/'
dirParquets = '/home/uoperator/workdir/processing/distr/'
os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ['SPARK_HOME'] = "/opt/spark"

def transformaCsvParquet():
  flagDelRaw = False
  try:
    #senão existir, cria a pasta
    if not os.path.exists(dirParquets):
        print("Criando pasta distr em processing")
        os.makedirs(dirParquets)

    # Inicializa a sessão Spark
    spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()
    print("Sessão Instanciada")

    schemas = { 'SOCIOS': StructType([StructField("nroCnpjBase", IntegerType(), True),StructField("idntfSocio", ByteType(), True),StructField("nomeSocio", StringType(), True),StructField("cpfCnpjSocio", LongType(), True),StructField("codQlfcSocio", ShortType(), True),StructField("dataEntrd", IntegerType(), True),StructField("codPais", ShortType(), True),StructField("nroCpfRprst", LongType(), True),StructField("nomeRprst", StringType(), True),StructField("codQlfcRprst", ShortType(), True),StructField("codFaixaEtr", ShortType(), True)])
                ,'EMPRESAS': StructType([StructField("nroCnpjBase", IntegerType(), True), StructField("razSocEmp", StringType(), True), StructField("codNatJur", IntegerType(), True), StructField("qulfRespEmp", ShortType(), True), StructField("valCapSoc", DoubleType(), True), StructField("codPrtEmp", ShortType(), True), StructField("entFedResp", StringType(), True), ])
                ,'ESTABELECIMENTOS' : StructType([StructField("nroCnpjBase", IntegerType(), True),StructField("nroCnpjOrdm", ShortType(), True),StructField("nroDigCnpj", ShortType(), True),StructField("idntMtrzFlil", ByteType(), True),StructField("nomeFant", StringType(), True),StructField("codSttCad", ByteType(), True),StructField("dataSttCad", IntegerType(), True),StructField("codMtvoSttCad", ShortType(), True),StructField("nomeCdadExt", StringType(), True),StructField("pais", StringType(), True),StructField("dataIncAtvd", IntegerType(), True),StructField("codCnaePrnc", IntegerType(), True),StructField("codCnaeSec", IntegerType(), True),StructField("tpoLogrd", StringType(), True),StructField("nomeLogrd", StringType(), True),StructField("nroLogrd", IntegerType(), True),StructField("iCompl", StringType(), True),StructField("iBairro", StringType(), True),StructField("cCep", IntegerType(), True),StructField("iUF", StringType(), True),StructField("codMunEmp", IntegerType(), True),StructField("nroDDD1", ShortType(), True),StructField("nroTel1", IntegerType(), True),StructField("nroDDD2", ShortType(), True),StructField("nroTel2", IntegerType(), True),StructField("nroDddFax", ShortType(), True),StructField("nroTelFax", IntegerType(), True),StructField("iEmail", StringType(), True),StructField("codSitEspcEmp", StringType(), True),StructField("dataSitEspcEmp", IntegerType(), True)])
                ,'SIMPLES' : StructType([StructField("nroCnpjBase", StringType(), True),StructField("flagSmpl", StringType(), True),StructField("dataOpcSmpl", StringType(), True),StructField("dataExcSmpl", StringType(), True),StructField("cOpcMei", StringType(), True),StructField("dataOpcMei", StringType(), True),StructField("dataExcMei", StringType(), True)])
                ,'PAISES' : StructType([StructField("cPais", ShortType(), True),StructField("iNomePais", StringType(), True)])
                ,'MUNICIPIOS' : StructType([StructField("cMuncp", ShortType(), True),StructField("iNomeMuncp", StringType(), True)])
                ,'CNAES' : StructType([StructField("cCnaes", IntegerType(), True),StructField("iCnaes", StringType(), True)])
                ,'QUALIFICACOES' : StructType([StructField("cQlfc", ShortType(), True),StructField("iQlfc", StringType(), True)])
                ,'NATUREZAS' : StructType([StructField("cNtrz", ShortType(), True),StructField("iNtrz", StringType(), True)])
                ,'MOTIVOS' : StructType([StructField("cMtvo", ShortType(), True),StructField("iMtvo", StringType(), True)])
}

    # Percorre todos os subdiretórios do diretório de entrada
    for dirAbsoluto, dirs, arquivos in os.walk(dirRaws):
        if dirAbsoluto.split("/")[-1].startswith('CNPJ'):
          for arquivo in arquivos:
              flagDelRaw = False
              if arquivo.upper().endswith(".CSV"):
                  # Lê o arquivo CSV
                  print(f"  {arquivo}")
                  dirAgrupaProc = dirParquets + "CNPJ_" + retornaReg(arquivo)
                  
                  #senão existir, cria a pasta
                  if not os.path.exists(dirAgrupaProc):
                    os.makedirs(dirAgrupaProc)
                  schemafull = schemas.get(dirAgrupaProc.split("_")[-1])                  

                  df = spark.read.format("csv") \
                  .option("header", "false") \
                  .option("delimiter", ";") \
                  .option('encoding', 'windows-1252') \
                  .schema(schemafull).load(os.path.join(dirAbsoluto, arquivo))

                  #df.show(n=5)
                  # Escreve o arquivo Parquet
                  df.write.format("parquet").mode("append").save(dirAgrupaProc)
                  flagDelRaw = True              
        if flagDelRaw:
          print(f'Deletando {dirAbsoluto}')      
          try:
            rmtree(dirAbsoluto, ignore_errors=True)
            print(f'Deletado {dirAbsoluto}')
          except OSError as error:
            print(error)
            print(f'Operação de delete abortada sobre {dirAbsoluto}')      
    print(f'Operação de conversão concluida')

    # Encerra a sessão Spark
    spark.stop()
  except Exception as e:    
    print("Falha na conversão para parquet")
    print(e)
    
if __name__ == "__main__":
    transformaCsvParquet()
    print('INFO: Finished')
