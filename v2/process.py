# app_gui_etl_csv.py
# -----------------------------------------------------------------------------
# GUI + ETL (CSV incremental) em um único arquivo.
# - Usuário digita a data inicial no formato dd-mm-yyyy.
# - O script calcula d2_date como D-2 de dias úteis B3.
# - Executa o ETL (mantendo sua lógica original).
# - Em vez de sobrescrever CSVs, atualiza de forma INCREMENTAL:
#   remove do CSV existente a janela [start_date, d2_date] e concatena os novos dados.
# - Gera/atualiza: groups.csv, positions.csv, costs_breakdown.csv.
# -----------------------------------------------------------------------------

# =========================
# Imports globais
# =========================
import os
import sys
import threading
import traceback
from datetime import date, datetime
from pathlib import Path
from tkcalendar import DateEntry

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# GUI
import tkinter as tk
from tkinter import messagebox, scrolledtext

# Dados / HTTP
import requests
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal

# =========================
# Configurações (edite aqui)
# =========================
# >>> Troque pelos seus valores reais (mantive placeholders propositalmente) <<<
API_ACCESS_ID = "89297662e92386720e192e56ffdc0d5e.access"
API_SECRET    = "b8b3cfabf25982a64a1074360f83b0dc143aa5bd75560abf5c901b0977364de4"
API_USERNAME  = "alberto.coppola@perenneinvestimentos.com.br"
API_PASSWORD  = "juNTr1QbtbY9NZ8ACrMF"
BASE_URL      = "https://perenne.bluedeck.com.br/api"

# Onde salvar/ler os CSVs
CSV_GROUPS_PATH    = "groups.csv"
CSV_POSITIONS_PATH = "positions.csv"
CSV_COSTS_PATH     = "costs_breakdown.csv"

# =========================
# Funções auxiliares (GUI)
# =========================
def _gui_log(text_widget: scrolledtext.ScrolledText, msg: str) -> None:
    """Escreve uma linha no painel de log da GUI."""
    text_widget.insert(tk.END, f"{msg}\n")
    text_widget.see(tk.END)
    text_widget.update_idletasks()

def read_csv_safe(path, **kwargs) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

# =========================
# ETL - helpers (mantendo sua lógica)
# =========================
def fetch_data(url: str, params: dict, access_id: str, secret: str, user_name: str, api_password: str) -> dict:
    """
    Sua função original: autentica e chama o endpoint.
    Mantive exatamente a estrutura (gera token a cada chamada).
    """
    base_url = BASE_URL
    url_token = "auth/token"

    # Autenticação via Cloudflare Access + JWT
    data = {"username": user_name, "password": api_password}
    client_headers = {
        "CF-Access-Client-Id": access_id,
        "CF-Access-Client-Secret": secret
    }
    resp_token = requests.post(f"{base_url}/{url_token}", data=data, headers=client_headers)
    resp_token.raise_for_status()
    j = resp_token.json()

    access_token = j['access_token']
    token_type   = j['token_type']

    # Headers finais para chamadas de API
    headers = {
        "CF-Access-Client-Id": access_id,
        "CF-Access-Client-Secret": secret,
        "Authorization": f"{token_type} {access_token}"
    }
    response_api = requests.post(f"{base_url}/{url}", headers=headers, json=params)
    response_api.raise_for_status()
    return response_api.json()

def write_incremental_csv(df_new: pd.DataFrame, csv_path: str, date_col: str | None,
                          d1: pd.Timestamp | None, d2: pd.Timestamp | None) -> None:
    """
    Atualiza CSV de forma incremental:
      - Se o arquivo existir, carrega-o.
      - Se 'date_col' for informado, remove as linhas do intervalo [d1, d2].
      - Concatena df_new e (se houver coluna de data) ordena por ela.
      - Salva no mesmo caminho.
    """
    p = Path(csv_path)

    # Carrega CSV existente, se houver
    if p.exists() and p.stat().st_size > 0:
        df_old = pd.read_csv(p)
        if date_col and (date_col in df_old.columns):
            df_old[date_col] = pd.to_datetime(df_old[date_col])
    else:
        # Garante mesmas colunas (ou deixa o pandas alinhar)
        df_old = pd.DataFrame(columns=df_new.columns)

    df_new2 = df_new.copy()
    if date_col and (date_col in df_new2.columns):
        df_new2[date_col] = pd.to_datetime(df_new2[date_col])

    # Remove janela do antigo
    if date_col and (date_col in df_old.columns) and (d1 is not None) and (d2 is not None):
        mask_keep = ~((df_old[date_col] >= pd.to_datetime(d1)) & (df_old[date_col] <= pd.to_datetime(d2)))
        df_old = df_old.loc[mask_keep]

    # Concatena
    df_out = pd.concat([df_old, df_new2], ignore_index=True)

    # Ordena por data, se existir
    if date_col and (date_col in df_out.columns):
        df_out.sort_values(by=[date_col], inplace=True)

    # Salva
    df_out.to_csv(p, index=False)

# =========================
# ETL - núcleo (seu código com mínimas alterações)
# =========================
def run_etl_with_start_date(ts_start: pd.Timestamp, logfn=lambda m: None) -> None:
    """
    Executa o ETL completo, mantendo sua lógica original.
    Apenas:
      - usa 'ts_start' (escolhida no GUI) como start_date,
      - calcula d2_date,
      - no final grava CSVs incrementalmente.
    """
    # ---------- Bloco de datas (seu bloco, com START_DATE vindo do GUI) ----------
    start_date = ts_start
    end_date = pd.Timestamp(date.today())

    # Calendário B3
    B3 = mcal.get_calendar('B3')
    B3_holidays = B3.holidays()
    feriados = [h for h in B3_holidays.holidays if start_date <= h <= end_date]
    bday_brasil = pd.offsets.CustomBusinessDay(holidays=feriados)

    # d-2 (D-2 dias úteis B3)
    d2_date = (date.today() - 2 * bday_brasil).strftime('%Y-%m-%d')

    # (opcional) ranges úteis
    # date_range = pd.date_range(start=start_date, end=d2_date, freq='B')
    # date_range_no_holidays = date_range[~date_range.isin(feriados)]

    logfn(f"Janela de processamento: {start_date.strftime('%Y-%m-%d')} até {d2_date}")

    # ---------- Config de API (mantive nomes/original) ----------
    api_acces_id = API_ACCESS_ID
    api_secret   = API_SECRET
    api_password = API_PASSWORD
    user_name    = API_USERNAME

    columns_filter = ['portfolio_id','overview_date', 'instrument_name', 'instrument_id','book_name',
                      'instrument_type', 'quantity', 'price', 'asset_value','exposure_value','dtd_ativo_fin']

    # ---------- POSIÇÕES ----------
    logfn("Baixando posições...")
    url = 'portfolio_position/positions/get'
    params = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": d2_date,
        "portfolio_group_ids": [1],
    }
    df_positions = fetch_data(url, params, api_acces_id, api_secret, user_name, api_password)['objects']
    df_positions = pd.DataFrame(df_positions)

    # De-para NAV por (portfolio_id,date)
    funds_nav = df_positions.loc[['portfolio_id','date','net_asset_value']].T.drop_duplicates()
    funds_nav.rename(columns={"net_asset_value":"portfolio_nav"}, inplace=True)
    funds_nav['date'] = pd.to_datetime(funds_nav['date'], errors='coerce')
    funds_nav['portfolio_id'] = pd.to_numeric(funds_nav['portfolio_id'], errors='coerce')


    # ---------- FUNDOS INTERNOS (explodíveis) ----------
    logfn("Listando fundos internos...")
    url_funds = 'portfolio_registration/portfolio_group/get'
    params_funds = {"get_composition": True}
    all_funds = fetch_data(url_funds, params_funds, api_acces_id, api_secret, user_name, api_password)['objects']
    all_funds = pd.DataFrame(all_funds)
    all_funds = list(all_funds['8'].loc['composition_names'].values())

    funds_name = df_positions.loc[['portfolio_id','name']].T.drop_duplicates()
    # Opcional: salvar esse de-para
    # funds_name.to_csv("funds_name.csv", index=False)

    # ---------- BENCHMARKS ----------
    logfn("Baixando benchmarks...")
    url = 'market_data/pricing/prices/get'
    params = {
        "start_date": "2024-12-31",
        "end_date": d2_date,
        "instrument_ids": [1540, 1932, 9]
    }
    bench_df = fetch_data(url, params, api_acces_id, api_secret, user_name, api_password)
    bench_df = pd.DataFrame(bench_df['prices'])[['date','instrument','variation']]
    bench_df.variation = bench_df.variation.astype(np.float64)
    bench_df['date'] = pd.to_datetime(bench_df['date'])
    bench_df = bench_df.sort_values(['date'])
    bench_df['ytd_pct'] = bench_df.groupby(['instrument'])['variation'].transform(lambda x: (1 + x).cumprod() - 1)

    # ---------- ERROS PnL ----------
    logfn("Checando erros de PnL...")
    url = 'portfolio_position/attribution/errors_view/get'
    for _, g in funds_name.iterrows():
        if g['name'] in all_funds:
            params = {
                "base_date": d2_date,
                "consolidation_type": 3,
                "show_errors": True,
                "periods": [2, 3],
                "attribution_types": [1],
                "portfolio_ids": [g.portfolio_id]
            }
            errors = fetch_data(url, params, api_acces_id, api_secret, user_name, api_password)['objects']
            errors = pd.DataFrame(errors).T
            if len(errors) >= 1:
                tem_erros_pnl = True
                errors = (errors).merge(funds_name,on='portfolio_id',how='left')[['name','date']]
                logfn("Erro encontrado:")
                logfn(errors.to_string(index=False))

    # --- pergunta se achou erro ---
    if tem_erros_pnl:
        seguir = messagebox.askyesno(
            "Erros de PnL encontrados",
            "Foram encontrados erros de PnL.\nDeseja continuar o processamento?"
        )
        if not seguir:
            logfn("Processamento cancelado pelo usuário devido a erros de PnL.")
            return  # interrompe a função ETL aqui

    # ---------- Construção de registros e custos ----------
    logfn("Processando posições e custos...")
    registros = pd.DataFrame()
    costs_df  = pd.DataFrame()

    for data_col in df_positions.columns:
        try:
            id_col = df_positions[data_col]
            portfolio_id = id_col['portfolio_id']

            # pula o consolidado para evitar duplicidade
            if portfolio_id == 49:
                continue

            instrument_positions = pd.DataFrame(id_col['instrument_positions'])
            costs_position       = pd.DataFrame(id_col['financial_transaction_positions'])
            instrument_positions['portfolio_id'] = portfolio_id

            # PnL total do ativo
            pnl_ = []
            for _, row in instrument_positions.iterrows():
                try:
                    pnl_.append(np.float64(row.attribution['total']['financial_value']))
                except:
                    pnl_.append(0.0)
            instrument_positions['dtd_ativo_fin'] = pnl_

            # PnL dos custos
            pnl_ = []
            for _, row in costs_position.iterrows():
                try:
                    pnl_.append(np.float64(row.attribution['total']['financial_value']))
                except:
                    pnl_.append(0.0)
            costs_position['dtd_custos_fin'] = pnl_

            # Data/Origem para custos
            costs_position['overview_date'] = instrument_positions.overview_date.unique()[0]
            costs_position['origin_portfolio_id'] = portfolio_id

            registros = pd.concat([registros, instrument_positions], ignore_index=True)
            costs_df  = pd.concat([costs_df,  costs_position],      ignore_index=True)

        except Exception as e:
            logfn(f"Aviso: erro ao processar coluna {data_col}: {e}")

    # Tipos
    if not registros.empty:
        registros.loc[:, ['quantity','asset_value','exposure_value','price','dtd_ativo_fin']] = (
            registros[['quantity','asset_value','exposure_value','price','dtd_ativo_fin']].astype(np.float64)
        )
    if not costs_df.empty:
        costs_df['financial_value'] = costs_df['financial_value'].astype(float)
        costs_df = costs_df[['financial_value','attribution','book_name','category_name',
                             'origin_portfolio_id','origin_accounting_transaction_id',
                             'dtd_custos_fin','overview_date']]

    # Exceções de book
    if not registros.empty:
        registros['book_name'] = registros['book_name'].replace({
            "Risco >> HYPE >> Ação HYPE":  "Risco >> HYPE",
            "Risco >> HYPE >> Opção HYPE": "Risco >> HYPE"
        })

    registros['overview_date'] = pd.to_datetime(registros['overview_date'], errors='coerce')
    registros['portfolio_id']  = pd.to_numeric(registros['portfolio_id'], errors='coerce')

    # Agregação de contingência (sua regra)
    registros = registros.groupby(['overview_date','portfolio_id','instrument_name']).agg({
        'instrument_id':'first',
        'book_name':'first',
        'instrument_type':'first',
        'quantity':'sum',
        'price':'first',
        'asset_value':'sum',
        'exposure_value':'sum',
        'dtd_ativo_fin': 'sum'
    }).reset_index()

    # NAV do fundo (merge)
    registros = pd.merge(registros, funds_nav, left_on=['portfolio_id','overview_date'],
                         right_on=['portfolio_id','date']).drop(columns='date')
    registros.rename(columns={"net_asset_value":"portfolio_nav"}, inplace=True)

    # ---------- Explosão recursiva ----------
    def explodir_portfolio(portfolio_id, data, todas_posicoes, todos_custos, visitados=None,
                           notional=None, portfolio_origem_id=None, nivel=0):
        if visitados is None:
            visitados = set()
        if portfolio_id in visitados:
            return [], pd.DataFrame()
        visitados.add(portfolio_id)

        posicoes = todas_posicoes[
            (todas_posicoes['overview_date'] == data) &
            (todas_posicoes['portfolio_id'] == portfolio_id)
        ]
        custos = todos_custos[
            (todos_custos['overview_date'] == data) &
            (todos_custos['origin_portfolio_id'] == portfolio_id)
        ]

        # garanta tipos numéricos para as somas
        if not posicoes.empty:
            posicoes['asset_value']   = pd.to_numeric(posicoes['asset_value'],   errors='coerce').fillna(0.0)
            posicoes['dtd_ativo_fin'] = pd.to_numeric(posicoes['dtd_ativo_fin'], errors='coerce').fillna(0.0)
            posicoes['exposure_value']= pd.to_numeric(posicoes['exposure_value'],errors='coerce').fillna(0.0)

        if not custos.empty:
            custos['financial_value'] = pd.to_numeric(custos['financial_value'], errors='coerce').fillna(0.0)
            custos['dtd_custos_fin']  = pd.to_numeric(custos['dtd_custos_fin'],  errors='coerce').fillna(0.0)

        aum = posicoes.asset_value.sum() + (custos.financial_value.sum() if not custos.empty else 0.0)
        if aum == 0:
            return [], pd.DataFrame()
        if notional is None:
            notional = aum
        mult = notional / aum

        if portfolio_origem_id is None:
            portfolio_origem_id = portfolio_id
        else:
            if not posicoes.empty:
                posicoes.loc[:, ['quantity','asset_value','exposure_value','dtd_ativo_fin']] = (
                    posicoes[['quantity','asset_value','exposure_value','dtd_ativo_fin']] * mult
                )
            if not custos.empty:
                custos.loc[:, ['financial_value','dtd_custos_fin']] = (
                    custos[['financial_value','dtd_custos_fin']] * mult
                )

        if not posicoes.empty:
            posicoes.loc[:, ['portfolio_id']] = portfolio_origem_id
        if not custos.empty:
            custos.loc[:, ['root_portfolio']] = portfolio_origem_id

        resultados = []
        for _, row in posicoes.iterrows():
            row_portfolio_id = row['instrument_id']
            if row.instrument_name in (all_funds):
                sub_resultados, resultado_custo = explodir_portfolio(
                    row_portfolio_id, data, todas_posicoes, todos_custos,
                    visitados=visitados, notional=np.float64(row.asset_value),
                    portfolio_origem_id=portfolio_origem_id, nivel=nivel+1
                )
                resultados += sub_resultados
                custos = pd.concat([custos, resultado_custo], ignore_index=True)
            else:
                novo = row.copy()
                novo['portfolio_origem'] = portfolio_id
                novo["nivel"] = nivel
                resultados.append(novo)

        return resultados, custos

    todas_explodidas = pd.DataFrame()
    todos_custos_explodidos = pd.DataFrame()

    registros['overview_date'] = pd.to_datetime(registros['overview_date'])
    datas = registros.overview_date.unique()
    portfolios = registros['portfolio_id'].unique()

    logfn("Explodindo portfolios...")
    for data_ in datas:
        for portfolio in portfolios:
            explodido, custo = explodir_portfolio(portfolio, data_, registros, costs_df)
            if explodido:
                todas_explodidas = pd.concat([todas_explodidas, pd.DataFrame(explodido)], ignore_index=True)
            if not custo.empty:
                todos_custos_explodidos = pd.concat([todos_custos_explodidos, custo], ignore_index=True)
    df_explodido = pd.DataFrame(todas_explodidas)

    # Repõe portfolio_nav do portfolio_id original
    if not df_explodido.empty:
        df_explodido = pd.merge(
            df_explodido.drop(columns=['portfolio_nav'], errors='ignore'),
            funds_nav, left_on=['portfolio_id','overview_date'], right_on=['portfolio_id','date'],
            how='left'
        ).drop(columns=['date'])

    if not todos_custos_explodidos.empty:
        todos_custos_explodidos = todos_custos_explodidos.groupby(
            ['overview_date','root_portfolio','origin_portfolio_id','category_name']
        ).agg({
            'book_name':'first',
            'financial_value':'sum',
            'dtd_custos_fin':'sum'
        }).reset_index()
        todos_custos_explodidos['overview_date'] = pd.to_datetime(todos_custos_explodidos['overview_date'])

    if not df_explodido.empty:
        df_explodido['overview_date'] = pd.to_datetime(df_explodido['overview_date'])
        df_explodido = df_explodido.groupby(['overview_date','portfolio_id','instrument_name','instrument_id']).agg({
            'book_name':'first',
            'asset_value':'sum',
            'dtd_ativo_fin':'sum',
            'exposure_value':'sum'
        }).reset_index()

    # ---------- Tratamentos de exceção / CPR ----------
    if not todos_custos_explodidos.empty:
        todos_custos_explodidos.loc[:, 'book_name'] = 'Risco >> Caixas e Provisionamentos >> CPR (Provisões)'
        todos_custos_explodidos.rename(columns={
            'root_portfolio':'portfolio_id',
            'category_name':'instrument_name',
            'dtd_custos_fin':'dtd_ativo_fin',
            'financial_value':'asset_value'
        }, inplace=True)

    if not df_explodido.empty:
        df_explodido.loc[df_explodido['book_name'] == 'Caixa', 'book_name'] = 'Caixa >> Títulos de Renda Fixa (PF)'

    # ---------- Feeds externos (se existirem) ----------
    logfn("Lendo feeds externos (se existirem)...")
    try:
        britechdf = pd.read_csv('feed_data/feed_britech.csv', encoding='latin', parse_dates=True).dropna(how='all')
        britechdf[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']] = \
            britechdf[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']].astype(float)
        britechdf['overview_date'] = pd.to_datetime(britechdf['overview_date'])

        betacurve = pd.read_csv('feed_data/beta_curva.csv', encoding='latin', parse_dates=True).dropna(how='all')
        betacurve[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']] = \
            betacurve[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']].astype(float)
        betacurve['overview_date'] = pd.to_datetime(betacurve['overview_date'])
    except Exception as e:
        # Se não houver feed, apenas cria DFs vazios
        britechdf = pd.DataFrame()
        betacurve = pd.DataFrame()
        logfn(f"Aviso feeds: {e}")

    # ---------- Montagem final (igual ao seu fluxo) ----------
    logfn("Montando df_final...")
    df_final = pd.DataFrame()
    if not britechdf.empty:
        for ptf in britechdf.portfolio_id.unique():
            filtered_britechdf = britechdf[britechdf.portfolio_id == ptf]
            filtered_beta = betacurve[betacurve.portfolio_id == ptf] if not betacurve.empty else pd.DataFrame()

            df_cpr = pd.DataFrame()
            if not df_explodido.empty:
                df_cpr = df_explodido[
                    ~(df_explodido['book_name'].isin(list(filtered_britechdf.book_name.drop_duplicates()))) &
                    (df_explodido['portfolio_id'] == ptf) &
                    ~(df_explodido['book_name'].str.lower().str.startswith('off'))
                ]
                if not df_cpr.empty:
                    df_cpr.loc[:, 'book_name'] = 'Risco >> Caixas e Provisionamentos >> CPR (Provisões)'
                    if not todos_custos_explodidos.empty:
                        df_cpr = pd.concat([df_cpr, todos_custos_explodidos[todos_custos_explodidos.portfolio_id == ptf]],
                                           ignore_index=True)
                    df_cpr = df_cpr.loc[df_cpr['overview_date'] > pd.Timestamp('2025-06-30')]
                    df_cpr['asset_value_ontem'] = df_cpr.groupby(['portfolio_id','instrument_name'])['asset_value'].shift(1)
                    df_cpr = df_cpr.groupby(['overview_date','portfolio_id','instrument_name']).agg({
                        'book_name':'first',
                        'asset_value':'sum',
                        'asset_value_ontem':'sum',
                        'dtd_ativo_fin':'sum'
                    }).reset_index()

            cpr_britech = filtered_britechdf[(filtered_britechdf['book_name']=='Risco >> Caixas e Provisionamentos')]
            if not cpr_britech.empty:
                cpr_britech['overview_date'] = pd.to_datetime(cpr_britech['overview_date'])
                df_cpr = pd.concat([df_cpr, cpr_britech], ignore_index=True).reset_index(drop=True)

            # posições incluídas no feed
            df_positions_f = pd.DataFrame()
            if not df_explodido.empty:
                df_positions_f = df_explodido[
                    (df_explodido['book_name'].isin(list(filtered_britechdf.book_name.drop_duplicates()))) &
                    (df_explodido['portfolio_id'] == ptf)
                ]
                # adiciona offshore
                df_offshore = df_explodido[
                    (df_explodido['book_name'].str.lower().str.startswith('off')) &
                    (df_explodido['portfolio_id'] == ptf)
                ]
                df_positions_f = pd.concat([df_positions_f, df_offshore], ignore_index=True).reset_index(drop=True)

                if not df_positions_f.empty:
                    df_positions_f['exposure_value_ontem'] = df_positions_f.groupby(['portfolio_id','instrument_name'])['exposure_value'].shift(1)
                    df_positions_f['asset_value_ontem']    = df_positions_f.groupby(['portfolio_id','instrument_name'])['asset_value'].shift(1)
                    df_positions_f = df_positions_f.dropna()

            pos_britech = filtered_britechdf[~(filtered_britechdf['book_name']=='Risco >> Caixas e Provisionamentos')]
            if not pos_britech.empty:
                df_positions_f = pd.concat([df_positions_f, pos_britech], ignore_index=True).reset_index(drop=True)

            if not filtered_beta.empty:
                df_positions_f = pd.concat([df_positions_f, filtered_beta], ignore_index=True).reset_index(drop=True)

            if not df_positions_f.empty:
                df_positions_f = df_positions_f.sort_values(['portfolio_id','instrument_name','overview_date'])
                df_positions_f['dtd_ativo_pct'] = df_positions_f['dtd_ativo_fin'] / df_positions_f['exposure_value_ontem']
                df_positions_f['Year']  = pd.to_datetime(df_positions_f['overview_date']).dt.year
                df_positions_f['Month'] = pd.to_datetime(df_positions_f['overview_date']).dt.month
                df_positions_f['mtd_ativo_pct'] = df_positions_f.groupby(
                    ['portfolio_id','instrument_name','Year','Month']
                )['dtd_ativo_pct'].transform(lambda x: (1 + x).cumprod() - 1)
                df_positions_f['ytd_ativo_pct'] = df_positions_f.groupby(
                    ['portfolio_id','instrument_name','Year']
                )['dtd_ativo_pct'].transform(lambda x: (1 + x).cumprod() - 1)

            df_concat = pd.concat([df_positions_f, df_cpr], ignore_index=True) if (not df_positions_f.empty or not df_cpr.empty) else df_positions_f
            if not df_concat.empty:
                df_concat['overview_date'] = pd.to_datetime(df_concat['overview_date'])
                df_concat = df_concat.fillna(0)
                df_final = pd.concat([df_final, df_concat], ignore_index=True)

    # ---------- Split hierarquia de books ----------
    if not df_final.empty:
        df_split = df_final['book_name'].astype(str).str.split(' >> ', expand=True)
        df_split.columns = [f'grupo_{i+1}' for i in range(df_split.shape[1])]
        groups_df = pd.concat([df_split, df_final['book_name']], axis=1).drop_duplicates()
        groups_df = pd.concat([groups_df, pd.DataFrame({'grupo_1':['Caixa','Risco'],'book_name':['Caixa','Risco']})])
        df_all = pd.concat([df_final, df_split], axis=1)
        df_all = df_all.replace(np.inf,0).replace(-np.inf,0)
    else:
        groups_df = pd.DataFrame()
        df_all = pd.DataFrame()

    # ---------- Persistência incremental (CSV) ----------
    logfn("Atualizando CSVs incrementalmente...")

    # 1) groups.csv (não tem coluna de data) — deduplicar por 'book_name'
    groups_out = groups_df.fillna("")
    p_groups = Path(CSV_GROUPS_PATH)
    if p_groups.exists() and p_groups.stat().st_size > 0:
        g_old = read_csv_safe(p_groups)
        if 'book_name' in g_old.columns:
            g_all = pd.concat([g_old, groups_out], ignore_index=True)
            g_all = g_all.drop_duplicates(subset=['book_name'], keep='last')
            g_all.to_csv(CSV_GROUPS_PATH, index=False)
        else:
            groups_out.to_csv(CSV_GROUPS_PATH, index=False)
    else:
        groups_out.to_csv(CSV_GROUPS_PATH, index=False)

    # 2) positions.csv (tem 'overview_date')
    write_incremental_csv(
        df_new=df_all,
        csv_path=CSV_POSITIONS_PATH,
        date_col="overview_date",
        d1=start_date,
        d2=pd.Timestamp(d2_date)
    )

    # 3) costs_breakdown.csv (tem 'overview_date')
    costs_out = locals().get('todos_custos_explodidos', pd.DataFrame()).copy()
    if not costs_out.empty:
        # Mantém sua lógica de filtrar ptfs presentes no britechdf, quando houver
        britech_local = locals().get('britechdf', pd.DataFrame())
        if not britech_local.empty and 'portfolio_id' in britech_local.columns:
            costs_out = costs_out[costs_out['portfolio_id'].isin(britech_local.portfolio_id.unique())]

    write_incremental_csv(
        df_new=costs_out,
        csv_path=CSV_COSTS_PATH,
        date_col="overview_date",
        d1=start_date,
        d2=pd.Timestamp(d2_date)
    )

    logfn("✔ ETL finalizado com sucesso.")

# =========================
# GUI principal (Tkinter)
# =========================
def main():
    root = tk.Tk()
    root.title("Perenne – Atualização de CSVs (ETL)")
    root.geometry("820x600")

    frm = tk.Frame(root, padx=10, pady=10)
    frm.pack(fill="both", expand=True)

    # Campo de data
    tk.Label(frm, text="Data inicial:").grid(row=0, column=0, sticky="w")
    ent_data = DateEntry(
                    frm,
                    width=12,
                    background="darkblue",
                    foreground="white",
                    borderwidth=2,
                    date_pattern="dd-mm-yyyy"  # formato mostrado
                )
    ent_data.grid(row=0, column=1, padx=10)
    ent_data.insert(0, "01-06-2025")  # sugestão

    # Log
    txt_log = scrolledtext.ScrolledText(frm, height=30)
    txt_log.grid(row=2, column=0, columnspan=4, sticky="nsew", pady=(10,0))

    # Botão processar
    def on_process():
        try:
            # Valida data do usuário
            d_user = ent_data.get_date()
            ts_start = pd.Timestamp(d_user)

            # Desabilita botão enquanto processa
            btn["state"] = "disabled"
            _gui_log(txt_log, f"Iniciando ETL a partir de {ts_start.strftime('%Y-%m-%d')}...\n")

            def worker():
                try:
                    run_etl_with_start_date(ts_start, logfn=lambda m: _gui_log(txt_log, m))
                    messagebox.showinfo("ETL", "Processamento finalizado com sucesso.")
                except Exception as e:
                    err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    _gui_log(txt_log, "✖ ERRO NO ETL:")
                    _gui_log(txt_log, err)
                    messagebox.showerror("ETL", f"Falha no processamento:\n{e}")
                finally:
                    btn["state"] = "normal"

            threading.Thread(target=worker, daemon=True).start()

        except Exception as e:
            messagebox.showerror("Entrada inválida", str(e))

    btn = tk.Button(frm, text="Processar", command=on_process)
    btn.grid(row=0, column=2, padx=8)

    # Layout responsivo
    frm.grid_rowconfigure(2, weight=1)
    frm.grid_columnconfigure(3, weight=1)

    root.mainloop()


if __name__ == "__main__":
    main()