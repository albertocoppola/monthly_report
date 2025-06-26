import requests
import pandas as pd
import numpy as np
from datetime import date
import pandas_market_calendars as mcal

# Filtro de datas
start_date = pd.Timestamp(date(date.today().year, 1, 1))
end_date = pd.Timestamp(date.today())

# Calendário de feriados da b3
B3 = mcal.get_calendar('B3')
B3_holidays = B3.holidays()

# Filtro correto por range
feriados = [h for h in B3_holidays.holidays if start_date <= h <= end_date]

# Define um CustomBusinessDay com esses feriados
bday_brasil = pd.offsets.CustomBusinessDay(holidays=feriados)

# data d-2
d2_date = (date.today()-2*bday_brasil).strftime('%Y-%m-%d')
# d2_date = '2025-06-02'

date_range = pd.date_range(start=start_date, end=d2_date, freq='B')
date_range_no_holidays = date_range[~date_range.isin(feriados)]

# User configs for API authentication
api_acces_id = "89297662e92386720e192e56ffdc0d5e.access"
api_secret = "b8b3cfabf25982a64a1074360f83b0dc143aa5bd75560abf5c901b0977364de4"
api_password = "juNTr1QbtbY9NZ8ACrMF"
user_name = "alberto.coppola@perenneinvestimentos.com.br"

columns_filter = ['portfolio_id','overview_date', 'instrument_name', 'instrument_id','book_name', 'instrument_type', 'quantity', 'price', 'asset_value']

# Retorna um dataframe a partir da parametros
def fetch_data(url: str, params: dict, access_id, secret, user_name, api_password):
    # trocar para a URL do ambiente desejado
    base_url = "https://perenne.bluedeck.com.br/api"
    url_token = "auth/token"
    
    # trocar o e-mail e a senha de aplicação
    data = {"username": user_name, "password": api_password}

    # trocar o ID e o secret
    client_headers = {
        "CF-Access-Client-Id": access_id,
        "CF-Access-Client-Secret": secret
    }

    # realiza request
    response = requests.post(f"{base_url}/{url_token}", data=data, headers=client_headers)

    # Converte em json
    json_response = response.json()

    # token de acesso
    access_token = json_response['access_token']
    token_type = json_response['token_type']
    # expiration_dt = json_response['expires_at']

    # Definindo headers para realização de chamadas
    client_header = {
        "CF-Access-Client-Id": access_id,
        "CF-Access-Client-Secret": secret
    }
    token_header = {"Authorization": f"{token_type} {access_token}"}
    headers = {**client_header, **token_header}

    response_api = requests.post(f"{base_url}/{url}", headers=headers, json=params)
    response_json = response_api.json()['objects']
    
    return pd.DataFrame(response_json)

# URL de posições
url = 'portfolio_position/positions/get'

# define URL de acesso
url_funds = 'portfolio_registration/portfolio/get'

# define parametros de busca
params_funds = {
    "is_active": True,
    "internally_managed": True
}

# Identifica todos os fundos do sistema para explodi-los
all_funds = fetch_data(url_funds, params_funds, api_acces_id, api_secret, user_name, api_password)

for dt in sorted(date_range_no_holidays):
    # define parametros de busca
    params = {
        "start_date": dt.strftime('%Y-%m-%d'),
        "end_date": dt.strftime('%Y-%m-%d'),
        "instrument_position_aggregation": 3
        # "portfolio_group_ids": [1]                 # ID do Fundo
    }

    df_positions = fetch_data(url, params, api_acces_id, api_secret, user_name, api_password)

    # Salva o de-para de portfolio_id e nome
    df_positions.loc[['portfolio_id','name']].T.drop_duplicates().to_csv("portfolio_id.csv", index=False)

    registros = pd.DataFrame()
    costs_df = pd.DataFrame()

    # Iterar pelas colunas de posição (ids)
    for data_col in df_positions.columns:

        try:
            # Pega os dados do id (coluna), vira Series
            id_col = df_positions[data_col]

            # Pega o portfolio_id (inteiro)
            portfolio_id = id_col['portfolio_id']

            # Pula o portfolio consolidado para evitar duplicidade 
            if portfolio_id == 49:
                continue

            # Pega o dicionário de instrument_positions e transforma em df
            instrument_positions = pd.DataFrame(id_col['instrument_positions'])

            # Pega o dicionário de custos e transforma em df
            costs_position = pd.DataFrame(id_col['financial_transaction_positions'])

            # Concatena o portfolio id
            instrument_positions['portfolio_id'] = portfolio_id

            # Concatena a data nos custos
            costs_position['overview_date'] = instrument_positions.overview_date.unique()[0]
            costs_position['origin_portfolio_id'] = portfolio_id

            # Concatena no df de registros
            registros = pd.concat([registros,instrument_positions])

            # Concatena no df de custos
            costs_df = pd.concat([costs_df,costs_position])
        
        except Exception as e:
            print(f"Erro ao processar data {data_col}: {e}")

    # Filtro pelas colunas de interesse e transforma quantidade, preço e aum em float
    registros = registros[columns_filter]
    registros.loc[:,['quantity','asset_value','price']] = registros[['quantity','asset_value','price']].astype(float)

    # Transforma a coluna de custos em float e o id em int
    costs_df['financial_value']=costs_df['financial_value'].astype(float)


    """
    ----------- Filtro de contingencia enquanto não arruma as categorias no sistema ---------

            soma as quantidades e notional pegando o primeiro book name para categorização
    """
    registros = registros.groupby(['portfolio_id','instrument_name']).agg({
        'overview_date':'first',
        'instrument_id':'first',
        'book_name':'first',
        'instrument_type':'first',
        'quantity':'sum',
        'price':'first',
        'asset_value':'sum'
    }).reset_index()


    def explodir_portfolio(portfolio_id, data, todas_posicoes, todos_custos, visitados=None, notional = None, portfolio_origem_id=None, nivel = 0):
        """
        - portfolio_id: o portfólio que estamos processando
        - data: a data da posição
        - todas_posicoes: DataFrame com todas as posições
        - multiplicador: proporção da posição herdada
        """
        
        if visitados is None:
            visitados = set()
            # print("STARTING: ",portfolio_id)
        if portfolio_id in visitados:
            return [], pd.DataFrame()

        visitados.add(portfolio_id)

        # Filtra as posições desse portfolio na data
        posicoes = todas_posicoes[
            (todas_posicoes['overview_date'] == data) &
            (todas_posicoes['portfolio_id'] == portfolio_id)
        ]

        # Filtra os custos do portfolio na data
        custos = todos_custos[
            (todos_custos['overview_date'] == data) &
            (todos_custos['origin_portfolio_id'] == portfolio_id)
        ]
        

        # Calculo do AUM total do fundo, soma ativos + custos
        aum = posicoes.asset_value.sum() + custos.financial_value.sum()
        if notional is None:
            notional = aum
        # print("Notional: ",notional)
        mult = notional/aum

        if portfolio_origem_id is None:
            portfolio_origem_id = portfolio_id
        else:
            # print("Multiplicando por: ", round(mult*100,4), "%")
            posicoes.loc[:,['quantity','asset_value']] = posicoes[['quantity','asset_value']] * mult
            custos.loc[:,['financial_value']] = custos.loc[:,['financial_value']] * mult

        # Seta o portfolio_id para a origem e cria na tabela de custos
        posicoes.loc[:,['portfolio_id']] = portfolio_origem_id
        custos.loc[:,['root_portfolio']] = portfolio_origem_id

        resultados = []

        for _, row in posicoes.iterrows():
            
            row_portfolio_id = row['instrument_id']
            if row.instrument_name in (all_funds.loc['name'].unique()):  # Checa se a linha é um fundo
                # print("----")
                # print(row.instrument_name)

                # É um fundo investido, explodir recursivamente
                sub_resultados, resultado_custo = explodir_portfolio(
                    row_portfolio_id,
                    data,
                    todas_posicoes,
                    todos_custos,
                    visitados=visitados,
                    notional = np.float64(row.asset_value),
                    portfolio_origem_id=portfolio_origem_id,
                    nivel = nivel + 1
                )
                resultados += sub_resultados
                # Concatena no df de custos
                custos = pd.concat([custos,resultado_custo])
            else:
                novo = row.copy()
                novo['portfolio_origem'] = portfolio_id
                novo["nivel"] = nivel
                resultados.append(novo)
        
        return resultados, custos


    todas_explodidas = []
    todos_custos_explodidos = pd.DataFrame()

    datas = registros.overview_date.unique()
    portfolios = registros['portfolio_id'].unique()

    for data in datas:
        for portfolio in portfolios:
            explodido, custo = explodir_portfolio(portfolio, data, registros, costs_df)
            todas_explodidas += explodido
            todos_custos_explodidos = pd.concat([todos_custos_explodidos,custo])
    df_explodido = pd.DataFrame(todas_explodidas)

    try:
        main_csv_position = pd.read_csv("portfolio_positions_exploded.csv")
        main_csv_costs = pd.read_csv("portfolio_costs_exploded.csv")

        """ Para posições: """
        # Primeiro: remove do main_csv_position todas as linhas que têm datas que existem no df_explodido
        main_csv_filtrado = main_csv_position[~main_csv_position['overview_date'].isin(df_explodido['overview_date'])]

        # Segundo: concatena os dois
        df_resultado = pd.concat([main_csv_filtrado, df_explodido], ignore_index=True)

        # Terceiro: opcional - reordenar por data, se quiser
        df_resultado = df_resultado.sort_values('overview_date').reset_index(drop=True)

        #Exporta CSV
        df_resultado.to_csv("portfolio_positions_exploded.csv", index=False)


        """ Para custos: """
        # Primeiro: remove do main_csv_costs todas as linhas que têm datas que existem no todos_custos_explodidos
        main_csv_filtrado = main_csv_costs[~main_csv_costs['overview_date'].isin(todos_custos_explodidos['overview_date'])]

        # Segundo: concatena os dois
        todos_custos_explodidos.loc[:,'book_name'] = 'CPR'
        df_resultado = pd.concat([main_csv_filtrado, todos_custos_explodidos], ignore_index=True)

        # Terceiro: opcional - reordenar por data, se quiser
        df_resultado = df_resultado.sort_values('overview_date').reset_index(drop=True)

        #Exporta CSV
        df_resultado.to_csv("portfolio_costs_exploded.csv", index=False)
    except:  # noqa: E722
        #Exporta CSV
        df_explodido.to_csv("portfolio_positions_exploded.csv", index=False)
        todos_custos_explodidos.loc[:,'book_name'] = 'CPR'
        todos_custos_explodidos.to_csv("portfolio_costs_exploded.csv", index=False)