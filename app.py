import psycopg2 
from datetime import datetime

# Conectar ao banco de dados PostgreSQL
conn = psycopg2.connect(
    dbname='projeto_concessionaria_bd',
    user='postgres',
    password='12341asd',
    host='localhost',
    port='5432'
)

# Criar cursor
cur = conn.cursor()

# Lista de queries com descrição
queries = [
    ("SELF JOIN: Retorna o relacionamento de cada funcionário com seu chefe", "SELECT F1.NOME AS FUNCIONARIO, F2.NOME AS CHEFE FROM FUNCIONARIO F1 LEFT JOIN FUNCIONARIO F2 ON F1.CPF_CHEFE = F2.CPF"),
    ("CONSTRUÇÃO CASE: Classificar clientes em novos, regulares e antigos", "SELECT NOME, DT_CAD, CASE WHEN DT_CAD >= '2023-01-01' THEN 'Novo Cliente' WHEN DT_CAD BETWEEN '2020-01-01' AND '2022-12-31' THEN 'Cliente Regular' ELSE 'Antigo Cliente' END AS CATEGORIA FROM CLIENTE"),
    ("LEFT JOIN: Retorna nome de clientes e serviços contratados, ou null se nenhum serviço", "SELECT nome, cod_serv FROM cliente c LEFT JOIN contrata c2 ON c.cpf = c2.cpf"),
    ("GROUP BY com HAVING: Funcionários que venderam 2 ou mais carros", "SELECT cpf_func, COUNT(*) AS VND_REALIZADAS FROM venda GROUP BY CPF_FUNC HAVING COUNT(*) >= 2"),
    ("GROUP BY com ORDER BY: Quantos funcionários cada chefe tem", "SELECT CPF_CHEFE, COUNT(*) AS NUM_FUNCIONARIOS FROM FUNCIONARIO WHERE CPF_CHEFE IS NOT NULL GROUP BY CPF_CHEFE ORDER BY NUM_FUNCIONARIOS DESC"),
    ("INNER JOIN: Nome dos clientes que contrataram serviços", "SELECT C.NOME, COUNT(*) as QTD_SRV_CONT FROM CONTRATA C2 JOIN CLIENTE C ON C.CPF = C2.CPF GROUP BY C.NOME"),
    ("Subconsulta Escalar: Carros com valor acima da média", "SELECT chassi, valor FROM auto_placa WHERE valor > (SELECT ROUND(AVG(valor), 2) FROM auto_placa)"),
    ("Semi-join: Clientes que contrataram serviços", "SELECT nome FROM cliente C WHERE EXISTS (SELECT * FROM contrata C2 WHERE C.cpf = C2.cpf)"),
    ("Anti-join: Clientes que não contrataram serviços", "SELECT nome FROM cliente C WHERE NOT EXISTS (SELECT * FROM contrata C2 WHERE C.cpf = C2.cpf)"),
    ("Subconsulta Tabela: Clientes que realizaram serviços na data da compra", "SELECT nome FROM cliente C WHERE (C.cpf, C.DT_CAD) IN (SELECT cpf, DT_ENT FROM contrata C2)"),
    ("Subconsulta Linha: Último cliente que comprou na data do cadastro", "SELECT nome FROM cliente C WHERE (C.cpf, C.DT_CAD) IN (SELECT CPF_CLIENTE, DATA_COMPRA FROM VENDA ORDER BY DATA_COMPRA DESC FETCH FIRST 1 ROW ONLY)"),
    ("UNION: União de nomes de clientes e funcionários", "SELECT NOME FROM (SELECT NOME FROM CLIENTE) UNION (SELECT NOME FROM FUNCIONARIO)"),
    ("INTERSECT: Clientes que compraram carros e contrataram serviços", "SELECT CPF_CLIENTE FROM VENDA INTERSECT SELECT CPF FROM CONTRATA"),
    ("EXCEPT: Clientes que compraram carros mas não contrataram serviços", "SELECT CPF_CLIENTE FROM VENDA EXCEPT SELECT CPF FROM CONTRATA"),
    ("DELETE com Subquery: Remove clientes que nunca compraram um carro", "DELETE FROM CLIENTE WHERE CPF NOT IN (SELECT CPF_CLIENTE FROM VENDA)"),
    ("UPDATE Condicional: Aumenta valor de táxis em 15%", "UPDATE AUTO_PLACA SET VALOR = VALOR * 1.15 WHERE TIPO = 'TAXI'"),
    ("VIEW: Clientes que compraram carros acima da média", "CREATE OR REPLACE VIEW ClientesPremium AS SELECT C.NOME, A.VALOR FROM CLIENTE C JOIN VENDA V ON C.CPF = V.CPF_CLIENTE JOIN AUTO_PLACA A ON V.CHASSI = A.CHASSI WHERE A.VALOR > (SELECT AVG(VALOR) FROM AUTO_PLACA)"),
    ("TRIGGER: Impede vendas de carros abaixo de 5000", "CREATE OR REPLACE FUNCTION verifica_valor_venda() RETURNS TRIGGER AS $$ BEGIN IF (SELECT VALOR FROM AUTO_PLACA WHERE CHASSI = NEW.CHASSI) < 5000 THEN RAISE EXCEPTION 'Venda não permitida para carros com valor abaixo de 5000'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql; CREATE TRIGGER trigger_verifica_venda BEFORE INSERT ON VENDA FOR EACH ROW EXECUTE FUNCTION verifica_valor_venda();"),
    ("Função de Janela: Classificar funcionários pelo número de vendas", "SELECT CPF_FUNC, COUNT(*) AS TOTAL_VENDAS, RANK() OVER (ORDER BY COUNT(*) DESC) AS RANKING FROM VENDA GROUP BY CPF_FUNC"),
    ("Atualização em Lote: Altera o CEP de clientes da Rua B", "UPDATE CLIENTE SET CEP = '50000099' WHERE RUA = 'B'"),
]

# Função para executar query
def executar_query(sql):
    cur.execute(sql)
    if cur.description:
        for row in cur.fetchall():
            print(row)
    conn.commit()

# Função para manipular dados com input manual
def manipular_dados(sql, valores):
    cur.execute(sql, valores)
    conn.commit()

# Menu interativo
while True:
    print("\nEscolha uma opção:")
    for i, (desc, _) in enumerate(queries, 1):
        print(f"{i}: {desc}")
    print("21: Adicionar cliente")
    print("22: Editar cliente")
    print("23: Adicionar funcionário")
    print("24: Editar funcionário")
    print("25: Adicionar venda")
    print("26: Contratar serviço")
    print("27: Deletar cliente")
    print("28: Deletar funcionário")
    print("29: Visualizar VIEW ClientesPremium")
    print("30: Inserir dono da concessionária")
    print("31: Tornar dono chefe do gerente com mais subordinados")
    print("32: Visualizar hierarquia de funcionários")
    print("0: Sair")

    escolha = input("Escolha uma opção: ")

    if escolha == '0':
        break
    elif escolha.isdigit() and 1 <= int(escolha) <= 20:
        executar_query(queries[int(escolha) - 1][1])
    elif escolha == '21':
        valores = input("Digite CPF, RUA, NUM, CEP, NOME, DT_CAD (YYYY-MM-DD) separados por vírgula: ").split(",")
        manipular_dados("INSERT INTO CLIENTE (CPF, RUA, NUM, CEP, NOME, DT_CAD) VALUES (%s, %s, %s, %s, %s, %s)", valores)
    elif escolha == '22':
        valores = tuple(input("Digite o novo NOME e o CPF do cliente: ").split(","))
        manipular_dados("UPDATE CLIENTE SET NOME = %s WHERE CPF = %s", valores)
    elif escolha == '23':
        valores = tuple(input("Digite CPF, RUA, NUM, CEP, NOME, MAT, CPF_CHEFE: ").split(","))
        manipular_dados("INSERT INTO FUNCIONARIO (CPF, RUA, NUM, CEP, NOME, MAT, CPF_CHEFE) VALUES (%s, %s, %s, %s, %s, %s, %s)", valores)
    elif escolha == '24':
        valores = tuple(input("Digite o novo NOME e o CPF do funcionário: ").split(","))
        manipular_dados("UPDATE FUNCIONARIO SET NOME = %s WHERE CPF = %s", valores)
    elif escolha == '25':
        valores = tuple(input("Digite CPF_CLIENTE, CPF_FUNC, CHASSI, DATA_COMPRA, STATUS: ").split(","))
        manipular_dados("INSERT INTO VENDA (CPF_CLIENTE, CPF_FUNC, CHASSI, DATA_COMPRA, STATUS) VALUES (%s, %s, %s, %s, %s)", valores)
    elif escolha == '26':
        valores = tuple(input("Digite CPF do cliente, COD do serviço, CHASSI do carro, data de entrada (YYYY-MM-DD), data de saída (YYYY-MM-DD): ").split(","))
        manipular_dados("INSERT INTO CONTRATA (CPF, COD, CHASSI, DT_ENT, DT_SAI) VALUES (%s, %s, %s, %s, %s)", valores)
    elif escolha == '27':
        cpf = input("Digite o CPF do cliente a ser deletado: ")
        manipular_dados("DELETE FROM CLIENTE WHERE CPF = %s", (cpf,))
    elif escolha == '28':
        cpf = input("Digite o CPF do funcionário a ser deletado: ")
        manipular_dados("DELETE FROM FUNCIONARIO WHERE CPF = %s", (cpf,))
    elif escolha == '29':
        executar_query("SELECT * FROM ClientesPremium")
    elif escolha == '30':
        executar_query("INSERT INTO FUNCIONARIO (CPF, RUA, NUM, CEP, NOME, MAT, CPF_CHEFE) VALUES ('00000000001', 'Rua da Empresa', '01', '00000000', 'FLAVIO', '00000000', '00000000001')")
    elif escolha == '31':
        executar_query("UPDATE FUNCIONARIO SET CPF_CHEFE = '00000000001' WHERE CPF = (SELECT CPF_CHEFE FROM FUNCIONARIO GROUP BY CPF_CHEFE ORDER BY COUNT(*) DESC LIMIT 1)")
    elif escolha == '32':
        executar_query("WITH RECURSIVE Hierarquia AS (SELECT CPF, NOME, CPF_CHEFE, 1 AS NIVEL FROM FUNCIONARIO WHERE CPF = '00000000001' UNION ALL SELECT F.CPF, F.NOME, F.CPF_CHEFE, H.NIVEL + 1 FROM FUNCIONARIO F JOIN Hierarquia H ON F.CPF_CHEFE = H.CPF WHERE F.CPF != H.CPF) SELECT * FROM Hierarquia ORDER BY NIVEL")

cur.close()
conn.close()

# Exemplo para adicionar venda no terminal:
# 25: Digite CPF_CLIENTE, CPF_FUNC, CHASSI, DATA_COMPRA, STATUS
# Exemplo: 11122233345, 11122233344, 9BD111060T5002170, 2025-03-20, PENDENTE

# Exemplo para contratar serviço no terminal:
# 26: Digite CPF, COD, CHASSI, DT_ENT (YYYY-MM-DD), DT_SAI (YYYY-MM-DD)
# Exemplo: 11122233345, 11111111, 9BD111060T5002170, 2025-03-20, 2025-03-25
