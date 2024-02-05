import psycopg2 as pg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def persisteBasePg(df, tabela, usuario, senha, flagdrop, host='localhost', porta=5432, database='dbCnpjEtl'):# config_file='.config'):

    iniOper = datetime.now()
    print(f"INFO: Carga Postgres tabela {tabela}- Iniciado em {str(iniOper).split('.')[0]}")

    try:        
         # Converte o DataFrame do PySpark para um DataFrame do Pandas
        dfPandas = df.toPandas()

        try:
            # Criar a URI do PostgreSQL
            postgresUri = f"postgresql://{usuario}:{senha}@{host}:{porta}/{database}"

            #Instancia dos conectores para o Pandas e pg2
            engine = create_engine(postgresUri)
            con = pg2.connect(postgresUri)
            
            con.autocommit = True
            cursor = con.cursor()

            #Drop view atual por conflito de dependência com a tabela no banco
            if flagdrop == 1:
                cursor.execute("drop view if exists vwmailingcnpj;")
                print("INFO: view dropada com sucesso")

            # Usando o método to_sql do Pandas para inserir os dados no PostgreSQL
            dfPandas.to_sql(tabela, engine, index=False, if_exists='replace')

            print("INFO: Dados enviados com sucesso para a tabela {} no PostgreSQL.".format(tabela))

            #Recria view para a tabela tcnpjucs
            if flagdrop == 1:
                cursor.execute('''create view public.vwMailingCnpj as
                SELECT "nroCnpjBase", "nroCnpjOrdm", "nroDigCnpj", "nomeFant", "dataSttCad", 
                "dataIncAtvd", "codCnaePrnc", "cCep", "nroLogrd", "iUF", "nroDDD1", "nroTel1", lower("iEmail"), 
                "razSocEmp", "codPrtEmp", "flagSmpl", "cOpcMei", "iNomeMuncp", "iCnaes"
                FROM public.tcnpjucs
                where "iEmail" is not null and "codSttCad" = 2''') 
                print("INFO: view vwMailingCnpj criada com sucesso")

        except Exception as e:
            print("ERROR: Erro ao enviar dados para o PostgreSQL:", e)

        finally:
            # Fecha a conexão
            if engine:
                engine.dispose()
            fimOper = datetime.now()
            delta = fimOper - iniOper    
            print(f"INFO: Carga da {tabela} executada em {str(delta).split('.')[0]}")
            print(f"INFO: Tempo gasto na operação: {str(delta.total_seconds()).split('.')[0]} segundos")      

    except ValueError as ve:
        print("Erro de ValueError:", ve)
