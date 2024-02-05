from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, desc
import os
from airflow.models import Variable
from datetime import datetime
#import sendToDatabasePg as pg
from CNPJETL.src.sendToDatabasePg import persisteBasePg

dirParquets = '/home/uoperator/workdir/processing/distr/'
os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ['SPARK_HOME'] = "/opt/spark"

lisdf = []
userAccessPg = Variable.get("userAccessPostgres")
passwordAcessPG = Variable.get("passwordAcessPostgres")

def montarIngeriDataframes():
        global lisdf
        iniOper = datetime.now()
        print(f"INFO: Iniciado criação de dataframes em {str(iniOper).split('.')[0]}")

        spark = SparkSession.builder.appName('Criação Dataframes CNPJ').getOrCreate()

        for dirAbsoluto, dirs, arquivos in os.walk(dirParquets):
                if dirAbsoluto.split("/")[-1].startswith('CNPJ'):
                        nomeDinDf = "df"+dirAbsoluto.split("_")[-1].capitalize()
                        print("INFO: Criando dataframe {}".format(nomeDinDf))
                        dfFixo = spark.read.format("parquet").load(dirAbsoluto)
                        if 'estabelecimentos' in nomeDinDf.lower():
                                dfFixo.select("nroCnpjBase", "nroCnpjOrdm", "nroDigCnpj", "nomeFant", "codSttCad", "dataSttCad", "codMtvoSttCad",
                                        "dataIncAtvd", "codCnaePrnc", "cCep", "nroLogrd", "iUF", "codMunEmp", "nroDDD1", "nroTel1", "iEmail"
                                        ).distinct().createOrReplaceTempView(nomeDinDf)
                                dfSql = spark.sql(f"select count(1) from {nomeDinDf}")
                                #dfSql.show()
                                #spark.sql(f"DESCRIBE {nomeDinDf}").show(truncate=False)

                                                         
                        elif 'empresas' in nomeDinDf.lower():
                                dfFixo.select("nroCnpjBase", "razSocEmp", "codPrtEmp", "codNatJur"
                                        ).distinct().createOrReplaceTempView(nomeDinDf)
                                dfSql = spark.sql(f"select count(1) from {nomeDinDf}")
                                #dfSql.show()
                                #spark.sql(f"DESCRIBE {nomeDinDf}").show(truncate=False)

                        elif 'simples' in nomeDinDf.lower():
                                dfFixo.select("nroCnpjBase", "flagSmpl", "cOpcMei"
                                        ).distinct().createOrReplaceTempView(nomeDinDf)
                                dfSql = spark.sql(f"select count(1) from {nomeDinDf}")
                                #dfSql.show()
                                #spark.sql(f"DESCRIBE {nomeDinDf}").show(truncate=False)

                        elif 'socios' in nomeDinDf.lower():
                                dfSilSocios = dfFixo.select('nroCnpjBase', 'nomeSocio', 'cpfCnpjSocio', 'codQlfcSocio').distinct()
                                        
                        else:
                                dfFixo.distinct().createOrReplaceTempView(nomeDinDf)
                                dfSql = spark.sql(f"select count(1) from {nomeDinDf}")
                                #spark.sql(f"DESCRIBE {nomeDinDf}").show(truncate=False)

        ### Transformações individuais com limpeza
        ## Naturezas excluidas com base em critérios de negócio
        dfSilver = spark.sql("""
                select
                est.*
                ,emp.razSocEmp, emp.codPrtEmp, emp.codNatJur
                ,CASE WHEN sim.flagSmpl IS NULL THEN 'N' ELSE sim.flagSmpl END flagSmpl,
                CASE WHEN sim.cOpcMei IS NULL THEN 'N' ELSE sim.cOpcMei END cOpcMei
                ,mu.iNomeMuncp
                ,cn.iCnaes
                from dfestabelecimentos est
                left join dfmunicipios mu on mu.cMuncp = est.codMunEmp
                left join dfcnaes cn on cn.cCnaes = est.codCnaePrnc
                left join dfempresas emp on emp.nroCnpjBase = est.nroCnpjBase
                left join dfnaturezas nat on nat.cNtrz = emp.codNatJur
                left join dfsimples sim on est.nroCnpjBase = sim.nroCnpjBase
                where nat.cNtrz in (
                0, 3999, 4014, 4120, 5010, 1252, 1260, 1279, 2038, 2046, 2054, 2062, 2070, 2089, 2097, 2100, 2127
                , 2135, 2143, 2151, 2160, 2178, 2194, 2216, 2224, 2232, 2240, 2259, 2267, 2275, 2283, 2291, 2305
                , 2313, 2321, 2330, 2348, 3069, 3077, 3085, 3115, 3131, 3204, 3212, 3247, 5037, 3328, 8885
                )
                """)
        
        dfSilMaisMun = dfSilver.groupBy("iNomeMuncp").agg(count("iNomeMuncp").alias("QtdEmpresas")) \
        .sort(desc("QtdEmpresas")).limit(50)

        dfSilMaisCnaes = dfSilver.groupBy("iCnaes").agg(count("iCnaes").alias("QtdEmpresas")) \
        .sort(desc("QtdEmpresas")).limit(50)

        dfSilSocios.select(count("*").alias("ContSocios")).show()
        dfSilver.select(count("*").alias("ContSilver")).show()
        dfSilMaisMun.select(count("*").alias("ContMunic")).show()
        dfSilMaisCnaes.select(count("*").alias("ContCnaes")).show()

        #dfSilSocios.printSchema()
        
        # Chamando a função para enviar o DataFrame para a tabela adequada
            # Atribui em uma lista os dataframes da sessão
        lisdf = [nome for nome, valor in locals().items() if isinstance(valor, DataFrame) and nome.startswith("dfSil")]
              
        for item in lisdf:
                tPostgres = ""
                flagdrop = 0
                if "dfsilver" in item.lower():
                        dfBase = dfSilver
                        tPostgres = "tcnpjucs"
                        flagdrop = 1
                elif "dfsilsocios" in item.lower():
                        dfBase = dfSilSocios
                        tPostgres = "tcnpjsocios"
                elif "dfsilmaismun" in item.lower():
                        dfBase = dfSilMaisMun
                        tPostgres = "tcnpjbigcities"
                elif "dfsilmaiscnaes" in item.lower():
                        dfBase = dfSilMaisCnaes
                        tPostgres = "tcnpjbigcnaes"        
                else:
                        print(f"INFO: Dataframe {item} não Aplicavel")
                if item.lower() in ["dfsilver", "dfsilsocios", "dfsilmaismun", "dfsilmaiscnaes"]:
                        persisteBasePg(dfBase, tPostgres, userAccessPg, passwordAcessPG, flagdrop)

        fimOper = datetime.now()
        delta = fimOper - iniOper
        print(f"INFO: Operação executada em {str(delta).split('.')[0]}")
        print(f"INFO: Tempo gasto na operação: {str(delta.total_seconds()).split('.')[0]} segundos") 
        spark.stop()

if __name__ == "__main__":

    montarIngeriDataframes()
    print("\nINFO: Fim Operação")      
