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
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="dindin@2025",
            host="54.83.71.63",
            port="5432"
        )
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

        cursor.close()
        conn.close()

        return ff in table_codigos

    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL: {error}")
        return False

def getContractByFF(date, number, token):
    ff = f"FF-{str(date.day).zfill(2)}/{str(date.month).zfill(2)}/{date.year}-{number}"
    if get_table(ff):
        print("Já existe! ", number)
        return

    url = f"https://openapi.stormfin.com.br/contratos?ff={ff}"
    headers = {"Authorization": f"Bearer {token}"}

    time.sleep(10)
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        if response.status_code == 500:
            return {"data": [], "error": True}
        raise ValueError("Erro ao recuperar contrato com FF")

    return response.json()

def saveOnDataBase(contrato):
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="dindin@2025",
            host="54.83.71.63",
            port="5432"
        )
        cursor = conn.cursor()

        query = """
        INSERT INTO contratos (
            codigo, ade, data_cadastro, data_pgto_bc, coeficiente, total_parcelas, prazo, 
            valor_bruto, valor_liquido, corretor, status_contrato, id_equipe, nome_equipe
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id;
        """

        dados = (
            contrato[0]['codigo'],
            contrato[0]['ade'],
            datetime.now(),
            '1910-09-01' if '0000-00-00' in contrato[0]['data_pgto_bc'] else contrato[0]['data_pgto_bc'],
            contrato[0]['coeficiente'],
            contrato[0]['total_parcelas'],
            contrato[0]['prazo'],
            contrato[0]['valor_bruto'],
            contrato[0]['valor_liquido'],
            contrato[0]['corretor']['nome'],
            contrato[0]['status_contrato']['nome'],
            contrato[0]['corretor']['loja_sala']['id'],
            contrato[0]['corretor']['loja_sala']['nome']
        )

        cursor.execute(query, dados)
        novo_id = cursor.fetchone()[0]
        print(f"Contrato inserido com ID: {novo_id}")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL: {error}")

def registrar_execucao():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="dindin@2025",
            host="54.83.71.63",
            port="5432"
        )
        cursor = conn.cursor()

        horario_atual = datetime.now()
        query = "INSERT INTO execucoes (horario_execucao) VALUES (%s);"
        cursor.execute(query, (horario_atual,))

        conn.commit()
        print(f"Execução registrada em: {horario_atual}")

        cursor.close()
        conn.close()

    except Exception as error:
        print(f"Erro ao conectar ao PostgreSQL: {error}")

def getClosedContracts():
    ano = datetime.now().year
    mes = datetime.now().month
    token = getToken()
    number = 1
    null_count = 0

    dias_do_mes = [datetime(ano, mes, dia) for dia in range(1, calendar.monthrange(ano, mes)[1] + 1)]
    
    for day in dias_do_mes:
        go = True
        while go:
            if day > datetime.now():
                print("Parando porque o dia é maior que hoje! #1")
                return
            
            contract = getContractByFF(day, number, token)

            if contract is None:
                number += 1
                continue

            if len(contract["data"]) == 0:
                try:
                    error = contract["error"]
                except KeyError:
                    error = False

                if error:
                    number += 1
                    continue
                
                null_count += 1

                # Se `null_count` chegar a 3 para `day == hoje`, interrompe a execução
                if null_count >= 3 and day == datetime.now().date():
                    print(f"Parando a execução, pois não há contratos para hoje após 3 tentativas ({day}). #2")
                    return
                
                if null_count >= 3:
                    print(f"Parando o loop para {day}, pois 3 tentativas retornaram vazias consecutivamente. #3")
                    go = False  # Para de buscar novos contratos para este dia
                    number = 1  # Reseta para o próximo dia
                    null_count = 0  # Reseta o contador para o próximo dia
                    continue
            else:
                saveOnDataBase(contract["data"])
                null_count = 0  # Reseta o contador ao encontrar um contrato válido
                number += 1  # Continua buscando mais contratos para o mesmo dia

            print(f"Tentativa {number} para {day}")

    print("Execução finalizada. #4")
    return

registrar_execucao()
getClosedContracts()
print("oi")
