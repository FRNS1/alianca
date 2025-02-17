import requests
from datetime import datetime
import calendar
import psycopg2
import time

def getToken():

    url = "https://openapi.stormfin.com.br/token"
    payload = 'grant_type=password&username=1007&password=123456&client_id=62db44b9301aaa085da38ddd428fd69101f93fff50bd6e678d8f457070dc675f'
    headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(url, headers=headers, data=payload)

    if response.status_code != 200:
        raise ValueError("Falha ao logar")

    data = response.json()

    return data['access_token']

def get_table(ff):
    try:
        # Estabelece a conexão com o banco de dados
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="dindin@2025",
            host="54.83.71.63",
            port="5432"
        )

        # Cria um cursor para interagir com o banco de dados
        cursor = conn.cursor()

        hoje = datetime.now()
        ultimo_dia_mes = calendar.monthrange(hoje.year, hoje.month)[1]
        
        selectQuery = f"""
        SELECT codigo FROM contratos 
        WHERE data_cadastro >= '{hoje.year}-{str(hoje.month).zfill(2)}-01' 
        AND data_cadastro <= '{hoje.year}-{str(hoje.month).zfill(2)}-{ultimo_dia_mes}';
        """

        cursor.execute(selectQuery)
        table = cursor.fetchall()

        table_codigos = {row[0] for row in table}

        if ff in table_codigos:
            return True
        return False

    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL: {error}")

def getContractByFF(date, number, token):
    ff = f"FF-{str(date.day).zfill(2)}/{str(date.month).zfill(2)}/{date.year}-{number}"
    if get_table(ff):
        print("Já existe! ", number)
        return
    url = f"https://openapi.stormfin.com.br/contratos?ff={ff}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    time.sleep(10)

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        if response.status_code == 500:
            return {"data": [], "error": True}
        raise ValueError("Erro ao recuperar contrato com FF")
    
    data = response.json()

    return data

def saveOnDataBase(contrato):
    try:
        # Estabelece a conexão com o banco de dados
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="dindin@2025",
            host="54.83.71.63",
            port="5432"
        )

        # Cria um cursor para interagir com o banco de dados
        cursor = conn.cursor()

        # Query de INSERT
        query = """
        INSERT INTO contratos (
            codigo, ade, data_cadastro, data_pgto_bc, coeficiente, total_parcelas, prazo, 
            valor_bruto, valor_liquido, corretor, status_contrato, id_equipe, nome_equipe
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id;
        """

        # Dados para inserção
        dados = (
            contrato[0]['codigo'],        # codigo
            contrato[0]['ade'],           # ade
            datetime.now(),
            '1910-09-01' if '0000-00-00' in contrato[0]['data_pgto_bc'] else contrato[0]['data_pgto_bc'],  # data_pgto_bc
            contrato[0]['coeficiente'],   # coeficiente
            contrato[0]['total_parcelas'],  # total_parcelas
            contrato[0]['prazo'],         # prazo
            contrato[0]['valor_bruto'],   # valor_bruto
            contrato[0]['valor_liquido'],  # valor_liquido
            contrato[0]['corretor']['nome'],  # corretor
            contrato[0]['status_contrato']['nome'],  # status_contrato
            contrato[0]['corretor']['loja_sala']['id'],
            contrato[0]['corretor']['loja_sala']['nome']
        )

        # Executar o INSERT
        cursor.execute(query, dados)

        # Obter o ID gerado
        novo_id = cursor.fetchone()[0]
        print(f"Contrato inserido com ID: {novo_id}")

        # Commit e fechar conexão
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL: {error}")


def registrar_execucao():
    try:
        # Estabelece a conexão com o banco de dados
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="dindin@2025",
            host="54.83.71.63",
            port="5432"
        )

        # Cria um cursor para interagir com o banco de dados
        cursor = conn.cursor()

        # Obtém o horário atual
        horario_atual = datetime.now()

        # Query de INSERT
        query = """
        INSERT INTO execucoes (horario_execucao)
        VALUES (%s);
        """

        # Executa o INSERT com o horário atual
        cursor.execute(query, (horario_atual,))

        # Confirma a transação
        conn.commit()

        print(f"Execução registrada em: {horario_atual}")

    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL: {error}")

    finally:
        # Fecha o cursor e a conexão
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def getClosedContracts():
    ano = datetime.now().year
    mes = datetime.now().month
    token = getToken()
    number = 1
    null_count = 0

    dias_do_mes = [datetime(ano, mes, dia) for dia in range(1, calendar.monthrange(ano, mes)[1] + 1)]
    for day in dias_do_mes:
        go = True
        while go == True:
            if day > datetime.now():
                return
            contract = getContractByFF(day, number, token)
            if contract == None:
                number += 1
                continue
            elif len(contract['data']) == 0 and day == dias_do_mes[-1]:
                return
            elif len(contract['data']) == 0 and number == 1:
                go = False
                number = 1
                continue
            elif len(contract['data']) == 0 and number > 1:
                try:
                    error = contract['error']
                except:
                    error = False
                if error:
                    number += 1
                    continue
                null_count += 1
                if null_count >= 3:
                    go = False
                    number = 1
                    continue
            saveOnDataBase(contract['data'])
            number += 1
            print(number)
            ...
    return


registrar_execucao()
aprovadosList = getClosedContracts()
print("oi")

