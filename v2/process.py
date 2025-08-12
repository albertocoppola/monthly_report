# app_gui_etl_csv.py
# -----------------------------------------------------------------------------
# GUI + ETL (CSV incremental) em um único arquivo.
# - Mantém a lógica do seu script original (explosão, CPR, feeds, etc.)
# - DatePicker (dd-mm-yyyy)
# - d2_date = D-2 de dias úteis B3
# - SALVA: benchmarks.csv e funds_name.csv (como no original)
# - Atualização incremental: positions.csv e costs_breakdown.csv (NUNCA apaga tudo)
#   -> Remove do CSV apenas as datas que vierem no df novo e concatena (ordenado por data)
# - groups.csv: dedup por book_name
# - Pergunta se deseja continuar quando encontra erros de PnL
# - Pergunta se deseja atualizar a curva do Beta (Excel) -> feed_data/beta_curva.csv
# - Logs passam sempre por logfn (sem acessar txt_log diretamente dentro do ETL)
# -----------------------------------------------------------------------------

from __future__ import annotations

# =========================
# Imports globais
# =========================
import os
import threading
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Optional, List, Union

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# GUI
import tkinter as tk
from tkinter import messagebox, scrolledtext
from tkcalendar import DateEntry  # DatePicker

# Dados / HTTP
import requests
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal

# xlwings para atualizar a curva do Beta (opcional)
import xlwings as xw


# =========================
# Configurações (do seu original)
# =========================
API_ACCESS_ID = "89297662e92386720e192e56ffdc0d5e.access"
API_SECRET    = "b8b3cfabf25982a64a1074360f83b0dc143aa5bd75560abf5c901b0977364de4"
API_USERNAME  = "alberto.coppola@perenneinvestimentos.com.br"
API_PASSWORD  = "juNTr1QbtbY9NZ8ACrMF"
BASE_URL      = "https://perenne.bluedeck.com.br/api"

# Caminhos dos CSVs
CSV_GROUPS_PATH    = "data/groups.csv"
CSV_POSITIONS_PATH = "data/positions.csv"
CSV_COSTS_PATH     = "data/costs_breakdown.csv"
CSV_FUNDS_NAME     = "data/funds_name.csv"
CSV_BENCHMARKS     = "data/benchmarks.csv"

# =========================
# Config Beta Curve
# =========================
EXCEL_PATH   = r"Beta RF Curva.xlsx"
SHEET_NAME   = "Hist"
TARGET_COLS  = ["data", "NAV", "P&L dia", "daily ret."]
BETA_FEED_FN = Path("feed_data") / "beta_curva.csv"


# =========================
# Utilidades de GUI / IO
# =========================
def _gui_log(text_widget: scrolledtext.ScrolledText, msg: str) -> None:
    """Loga no widget de texto sem travar UI."""
    try:
        text_widget.insert(tk.END, f"{msg}\n")
        text_widget.see(tk.END)
        text_widget.update_idletasks()
    except Exception:
        # fallback silencioso se GUI não estiver pronta
        print(msg)


def read_csv_safe(path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """Lê CSV com segurança: se não existir / vazio -> DataFrame vazio."""
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(p, **kwargs)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def upsert_by_date(
    csv_path: Union[str, Path],
    df_new: pd.DataFrame,
    *,
    date_col: str,
    unique_keys: Optional[List[str]] = None,
    float_format: Optional[str] = None,
    logfn=None,
) -> Optional[pd.DataFrame]:
    """
    ***COMPORTAMENTO CHAVE (igual ao seu snippet original simples):***
    Remove do CSV existente TODAS as linhas cujas datas aparecem em df_new[date_col],
    depois concatena df_new, ordena por data e grava.

    - NÃO toca no arquivo se df_new estiver vazio (evita apagar tudo).
    - Converte e normaliza datas (Timestamp vs str) pra evitar TypeError em sort.
    - unique_keys: se informado, faz drop_duplicates nessas chaves (incluindo date_col).
    """
    def _log(msg: str):
        try:
            (logfn or print)(msg)
        except Exception:
            print(msg)

    csv_path = Path(csv_path)

    # 0) Se df_new vazio, não mexe
    if not isinstance(df_new, pd.DataFrame) or df_new.empty:
        _log(f"[SKIP] {csv_path.name}: df_new vazio — nada a atualizar.")
        return None

    # 1) Normaliza coluna de data do df_new
    if date_col not in df_new.columns:
        if getattr(df_new.index, "name", None) == date_col:
            df_new = df_new.reset_index()
        else:
            raise KeyError(f"'{date_col}' não está em df_new para {csv_path.name}. "
                           f"Colunas: {list(df_new.columns)} | index.name: {getattr(df_new.index, 'name', None)}")

    df_new = df_new.copy()
    df_new[date_col] = pd.to_datetime(df_new[date_col], errors="coerce").dt.normalize()
    if df_new[date_col].isna().any():
        nbad = int(df_new[date_col].isna().sum())
        raise ValueError(f"{csv_path.name}: {nbad} linhas com data inválida em df_new[{date_col}]")

    # 2) Lê CSV antigo (se houver)
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            df_old = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            df_old = pd.DataFrame()
    else:
        df_old = pd.DataFrame()

    # 3) Normaliza date_col do antigo, se existir
    if not df_old.empty:
        if date_col in df_old.columns:
            df_old = df_old.copy()
            df_old[date_col] = pd.to_datetime(df_old[date_col], errors="coerce").dt.normalize()
        else:
            # Se o CSV existir mas não tiver date_col, começamos do zero com df_new
            df_old = pd.DataFrame(columns=df_new.columns)

    # 4) União de colunas (garante cabeçalho completo)
    all_cols = df_old.columns.union(df_new.columns)
    df_old = df_old.reindex(columns=all_cols)
    df_new = df_new.reindex(columns=all_cols)

    # 5) Remove do antigo as datas que estão chegando no novo
    new_dates = df_new[date_col].dropna().unique()
    if not df_old.empty and date_col in df_old.columns:
        kept_old = df_old[~df_old[date_col].isin(new_dates)]
    else:
        kept_old = df_old

    # 6) Concatena e (opcional) dedup por chaves
    out = pd.concat([kept_old, df_new], ignore_index=True)
    if unique_keys:
        subset = list(unique_keys)
        if date_col not in subset:
            subset.append(date_col)
        out = out.drop_duplicates(subset=subset, keep="last", ignore_index=True)

    # 7) Ordena por data e grava ATOMICAMENTE
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce").dt.normalize()
    out = out.sort_values(date_col, kind="mergesort").reset_index(drop=True)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    out.to_csv(tmp, index=False, float_format=float_format)
    tmp.replace(csv_path)

    _log(f"[OK] {csv_path.name}: +{len(df_new)} novas; datas substituídas={len(new_dates)}; final={len(out)}")
    return out


# =========================
# Beta Curve helpers
# =========================
def load_hist_columns(path=EXCEL_PATH, sheet=SHEET_NAME, target_cols=TARGET_COLS) -> pd.DataFrame:
    """Lê a planilha e devolve apenas as colunas-alvo, com tipos tratados."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    app = xw.App(visible=False, add_book=False)
    book = None
    try:
        book = app.books.open(str(path), update_links=False, read_only=True)
        sht = book.sheets[sheet]

        used_range = sht.used_range
        data = used_range.value  # matriz (primeira linha são cabeçalhos)
        df = pd.DataFrame(data[1:], columns=data[0])

        # Mapeia cabeçalhos sem depender de caixa/acentos
        norm_cols = {c: str(c).strip().lower() for c in df.columns}
        reverse_map = {}
        for original, lowered in norm_cols.items():
            for wanted in target_cols:
                if lowered == wanted.strip().lower():
                    reverse_map[original] = wanted

        missing = [c for c in target_cols if c.lower() not in [v.lower() for v in reverse_map.values()]]
        if missing:
            raise KeyError(f"Colunas não encontradas: {missing}\nCabeçalhos disponíveis: {list(df.columns)}")

        df = df[list(reverse_map.keys())].rename(columns=reverse_map)

        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
        for col in ["NAV", "P&L dia", "daily ret."]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    finally:
        try:
            if book is not None:
                book.close()
        except Exception:
            pass
        app.quit()


def update_beta_curve(logfn=lambda m: None) -> None:
    """Lê o Excel do Beta, transforma e salva feed_data/beta_curva.csv."""
    logfn("Atualizando curva do Beta (lendo Excel)...")
    df = load_hist_columns().dropna(how='all').copy()

    # colunas fixas
    df['portfolio_id']    = 1295
    df['book_name']       = 'Risco >> Renda Fixa'
    df['instrument_name'] = 'BETA CURVA'

    # métricas de exposição
    df['exposure_value']        = df['NAV']
    df['asset_value_ontem']     = df['NAV'].shift(1)
    df['exposure_value_ontem']  = df['NAV'].shift(1)

    df = df.rename(columns={
        "data":"overview_date",
        "NAV":"asset_value",
        "P&L dia":"dtd_ativo_fin",
        "daily ret.":"dtd_ativo_pct"
    }).dropna()

    os.makedirs(BETA_FEED_FN.parent, exist_ok=True)
    df.to_csv(BETA_FEED_FN, index=False)
    logfn(f"✔ Beta curve atualizada: {BETA_FEED_FN}")


# =========================
# ETL - helpers (mantendo sua estrutura)
# =========================
def fetch_data(url: str, params: dict, access_id: str, secret: str, user_name: str, api_password: str) -> dict:
    """Wrapper com raise_for_status para não seguir com respostas inválidas."""
    base_url = BASE_URL
    url_token = "auth/token"

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

    headers = {
        "CF-Access-Client-Id": access_id,
        "CF-Access-Client-Secret": secret,
        "Authorization": f"{token_type} {access_token}"
    }
    response_api = requests.post(f"{base_url}/{url}", headers=headers, json=params)
    response_api.raise_for_status()
    return response_api.json()


# =========================
# ETL - núcleo (sua lógica original com correções defensivas)
# =========================
def run_etl_with_start_date(ts_start: pd.Timestamp, logfn=lambda m: None) -> None:
    start_date = ts_start

    # ========================
    # d-2 B3
    # ========================
    B3 = mcal.get_calendar('B3')
    B3_holidays = B3.holidays()
    feriados = [h for h in B3_holidays.holidays if ts_start <= h <= pd.Timestamp.today().normalize()]
    bday_brasil = pd.offsets.CustomBusinessDay(holidays=feriados)
    d2_ts = (pd.Timestamp.today().normalize() - 2 * bday_brasil)
    d2_date = d2_ts.strftime('%Y-%m-%d')

    logfn(f"Janela de processamento: {start_date.strftime('%Y-%m-%d')} até {d2_date}")

    # ========================
    # POSIÇÕES
    # ========================
    logfn("Baixando posições...")
    url = 'portfolio_position/positions/get'
    params = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": d2_date,
        "portfolio_group_ids": [1],
    }
    df_positions = fetch_data(url, params, API_ACCESS_ID, API_SECRET, API_USERNAME, API_PASSWORD)['objects']
    df_positions = pd.DataFrame(df_positions)

    # De-para NAV por (portfolio_id, date) — mantém como veio (string) aqui!
    funds_nav = df_positions.loc[['portfolio_id','date','net_asset_value']].T.drop_duplicates()
    funds_nav.rename(columns={"net_asset_value":"portfolio_nav"}, inplace=True)

    # ========================
    # FUNDOS INTERNOS
    # ========================
    logfn("Listando fundos internos...")
    url_funds = 'portfolio_registration/portfolio_group/get'
    params_funds = {"get_composition": True}
    all_funds = fetch_data(url_funds, params_funds, API_ACCESS_ID, API_SECRET, API_USERNAME, API_PASSWORD)['objects']
    all_funds = pd.DataFrame(all_funds)
    all_funds = list(all_funds['8'].loc['composition_names'].values())

    funds_name = df_positions.loc[['portfolio_id','name']].T.drop_duplicates()
    funds_name.to_csv(CSV_FUNDS_NAME, index=False)

    # ========================
    # BENCHMARKS
    # ========================
    logfn("Baixando benchmarks...")
    url = 'market_data/pricing/prices/get'
    params = {
        "start_date": "2024-12-31",
        "end_date": d2_date,
        "instrument_ids": [1540, 1932, 9]
    }
    bench_df = fetch_data(url, params, API_ACCESS_ID, API_SECRET, API_USERNAME, API_PASSWORD)
    bench_df = pd.DataFrame(bench_df['prices'])[['date','instrument','variation']]
    bench_df.variation = bench_df.variation.astype(np.float64)
    bench_df['date'] = pd.to_datetime(bench_df['date'])
    bench_df = bench_df.sort_values(['date'])
    bench_df['ytd_pct'] = bench_df.groupby(['instrument'])['variation'].transform(lambda x: (1 + x).cumprod() - 1)
    bench_df.to_csv(CSV_BENCHMARKS, index=False)

    # ========================
    # ERROS PnL
    # ========================
    logfn("Checando erros de PnL...")
    url = 'portfolio_position/attribution/errors_view/get'
    tem_erros_pnl = False

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
            try:
                errors = fetch_data(url, params, API_ACCESS_ID, API_SECRET, API_USERNAME, API_PASSWORD)['objects']
                errors = pd.DataFrame(errors).T
                if len(errors) >= 1:
                    tem_erros_pnl = True
                    errors = (errors).merge(funds_name, on='portfolio_id', how='left')[['name','date']]
                    logfn("Erro encontrado:")
                    logfn(errors.to_string(index=False))
            except Exception as e:
                logfn(f"Aviso ao consultar erros de PnL ({g['name']}): {e}")
                continue

    if tem_erros_pnl:
        seguir = messagebox.askyesno(
            "Erros de PnL encontrados",
            "Foram encontrados erros de PnL.\nDeseja continuar o processamento?"
        )
        if not seguir:
            logfn("Processamento cancelado pelo usuário devido a erros de PnL.")
            return

    # ========================
    # REGISTROS & CUSTOS (mantém overview_date como STRING aqui!)
    # ========================
    logfn("Processando posições e custos...")
    registros = pd.DataFrame()
    costs_df  = pd.DataFrame()

    for data_col in df_positions.columns:
        try:
            id_col = df_positions[data_col]
            portfolio_id = id_col['portfolio_id']
            if portfolio_id == 49:  # pula consolidado
                continue

            instrument_positions = pd.DataFrame(id_col['instrument_positions'])
            costs_position       = pd.DataFrame(id_col['financial_transaction_positions'])

            instrument_positions['portfolio_id'] = portfolio_id

            pnl_ = []
            for _, row in instrument_positions.iterrows():
                try:
                    pnl_.append(np.float64(row.attribution['total']['financial_value']))
                except:
                    pnl_.append(0.0)
            instrument_positions['dtd_ativo_fin'] = pnl_

            pnl_ = []
            for _, row in costs_position.iterrows():
                try:
                    pnl_.append(np.float64(row.attribution['total']['financial_value']))
                except:
                    pnl_.append(0.0)
            costs_position['dtd_custos_fin'] = pnl_

            # mantém overview_date como veio (string) pra casar com costs_df
            costs_position['overview_date'] = instrument_positions.overview_date.unique()[0]
            costs_position['origin_portfolio_id'] = portfolio_id

            registros = pd.concat([registros, instrument_positions], ignore_index=True)
            costs_df  = pd.concat([costs_df,  costs_position],      ignore_index=True)

        except Exception as e:
            logfn(f"Aviso: erro ao processar coluna {data_col}: {e}")

    if not registros.empty:
        registros.loc[:, ['quantity','asset_value','exposure_value','price','dtd_ativo_fin']] = (
            registros[['quantity','asset_value','exposure_value','price','dtd_ativo_fin']].astype(np.float64)
        )

        # normalização de books (igual ao seu original)
        registros['book_name'] = registros['book_name'].replace({
            "Risco >> HYPE >> Ação HYPE":  "Risco >> HYPE",
            "Risco >> HYPE >> Opção HYPE": "Risco >> HYPE"
        })

    if not costs_df.empty:
        costs_df['financial_value'] = pd.to_numeric(costs_df['financial_value'], errors='coerce')
        costs_df = costs_df[['financial_value','attribution','book_name','category_name',
                             'origin_portfolio_id','origin_accounting_transaction_id',
                             'dtd_custos_fin','overview_date']]

    # ========================
    # AGRUPA REGISTROS
    # ========================
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

    # merge NAV (ambos ainda como string na chave de data)
    registros = pd.merge(
        registros, funds_nav,
        left_on=['portfolio_id','overview_date'],
        right_on=['portfolio_id','date']
    ).drop(columns='date')
    registros.rename(columns={"net_asset_value":"portfolio_nav"}, inplace=True)

    # ========================
    # EXPLOSÃO (mantendo datas STRING até aqui!)
    # ========================
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

        if not posicoes.empty:
            posicoes['asset_value']    = pd.to_numeric(posicoes['asset_value'],    errors='coerce').fillna(0.0)
            posicoes['dtd_ativo_fin']  = pd.to_numeric(posicoes['dtd_ativo_fin'],  errors='coerce').fillna(0.0)
            posicoes['exposure_value'] = pd.to_numeric(posicoes['exposure_value'], errors='coerce').fillna(0.0)
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

    # NÃO converter `overview_date` aqui (mantém STRING)!
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

    # ========================
    # CONVERSÕES DE DATA — AGORA SIM!
    # ========================
    if not df_explodido.empty:
        df_explodido['overview_date'] = pd.to_datetime(df_explodido['overview_date'], errors='coerce')
        funds_nav['date']             = pd.to_datetime(funds_nav['date'],             errors='coerce')
        df_explodido['portfolio_id']  = pd.to_numeric(df_explodido['portfolio_id'], errors='coerce')
        funds_nav['portfolio_id']     = pd.to_numeric(funds_nav['portfolio_id'],    errors='coerce')

        df_explodido = pd.merge(
            df_explodido.drop(columns=['portfolio_nav'], errors='ignore'),
            funds_nav, left_on=['portfolio_id','overview_date'], right_on=['portfolio_id','date'],
            how='left'
        ).drop(columns=['date'])

        df_explodido = df_explodido.groupby(['overview_date','portfolio_id','instrument_name','instrument_id']).agg({
            'book_name':'first',
            'asset_value':'sum',
            'dtd_ativo_fin':'sum',
            'exposure_value':'sum'
        }).reset_index()

    if not todos_custos_explodidos.empty:
        todos_custos_explodidos = todos_custos_explodidos.groupby(
            ['overview_date','root_portfolio','origin_portfolio_id','category_name']
        ).agg({
            'book_name':'first',
            'financial_value':'sum',
            'dtd_custos_fin':'sum'
        }).reset_index()
        todos_custos_explodidos['overview_date'] = pd.to_datetime(todos_custos_explodidos['overview_date'], errors='coerce')

    # ========================
    # TRATAMENTOS / CPR
    # ========================
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

    # ========================
    # FEEDS EXTERNOS
    # ========================
    logfn("Lendo feeds externos (se existirem)...")
    try:
        britechdf = pd.read_csv('feed_data/feed_britech.csv', encoding='latin', parse_dates=['overview_date']).dropna(how='all')
        britechdf[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']] = \
            britechdf[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']].astype(float)
        britechdf['overview_date'] = pd.to_datetime(britechdf['overview_date'])

        betacurve = pd.read_csv(BETA_FEED_FN, encoding='latin', parse_dates=['overview_date']).dropna(how='all')
        betacurve[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']] = \
            betacurve[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']].astype(float)
        betacurve['overview_date'] = pd.to_datetime(betacurve['overview_date'])
    except Exception as e:
        britechdf = pd.DataFrame()
        betacurve = pd.DataFrame()
        logfn(f"Aviso feeds: {e}")

    # ========================
    # MONTAGEM FINAL
    # ========================
        # ---------------- Montagem final ----------------
    logfn("Montando df_final...")
    df_final = pd.DataFrame()
    if not britechdf.empty:
        for ptf in britechdf.portfolio_id.unique():
            filtered_britech = britechdf[britechdf.portfolio_id == ptf].copy()
            filtered_beta    = betacurve[betacurve.portfolio_id == ptf].copy() if not betacurve.empty else pd.DataFrame()

            # ===== CPR =====
            df_cpr = pd.DataFrame()
            if not df_explodido.empty:
                # tudo que NÃO está no britech vira CPR (exceto offshore)
                df_cpr = df_explodido[
                    ~(df_explodido['book_name'].isin(list(filtered_britech.book_name.drop_duplicates()))) &
                    (df_explodido['portfolio_id'] == ptf) &
                    ~(df_explodido['book_name'].str.lower().str.startswith('off'))
                ].copy()

                if not df_cpr.empty:
                    df_cpr.loc[:, 'book_name'] = 'Risco >> Caixas e Provisionamentos >> CPR (Provisões)'

                    if not todos_custos_explodidos.empty:
                        df_cpr = pd.concat(
                            [df_cpr, todos_custos_explodidos[todos_custos_explodidos.portfolio_id == ptf]],
                            ignore_index=True
                        )

                    df_cpr = df_cpr.loc[df_cpr['overview_date'] > pd.Timestamp('2025-06-30')]

                    # agrega CPR por dia/instrumento
                    df_cpr['asset_value_ontem'] = df_cpr.groupby(['portfolio_id','instrument_name'])['asset_value'].shift(1)
                    df_cpr = df_cpr.groupby(
                        ['overview_date','portfolio_id','instrument_name'],
                        as_index=False
                    ).agg({
                        'book_name':'first',
                        'asset_value':'sum',
                        'asset_value_ontem':'sum',
                        'dtd_ativo_fin':'sum'
                    })

            # CPR vindo do britech
            cpr_britech = filtered_britech[filtered_britech['book_name'] == 'Risco >> Caixas e Provisionamentos'].copy()
            if not cpr_britech.empty:
                cpr_britech['overview_date'] = pd.to_datetime(cpr_britech['overview_date'])
                df_cpr = pd.concat([df_cpr, cpr_britech], ignore_index=True).reset_index(drop=True)

            # ===== POSIÇÕES =====
            # 1) base do EXPLÓDIDO (apenas books presentes no britech + offshore)
            df_from_exploded = pd.DataFrame()
            if not df_explodido.empty:
                df_from_exploded = df_explodido[
                    (df_explodido['book_name'].isin(list(filtered_britech.book_name.drop_duplicates()))) &
                    (df_explodido['portfolio_id'] == ptf)
                ].copy()

                df_offshore = df_explodido[
                    (df_explodido['book_name'].str.lower().str.startswith('off')) &
                    (df_explodido['portfolio_id'] == ptf)
                ].copy()
                df_from_exploded = pd.concat([df_from_exploded, df_offshore], ignore_index=True)

            # 2) POSIÇÕES do BRITECH (prioridade até 30/06)
            pos_britech = filtered_britech[filtered_britech['book_name'] != 'Risco >> Caixas e Provisionamentos'].copy()
            if not pos_britech.empty:
                pos_britech['overview_date']    = pd.to_datetime(pos_britech['overview_date'], errors='coerce').dt.normalize()
                if not df_from_exploded.empty:
                    df_from_explodido = df_from_exploded.copy()
                    df_from_explodido['overview_date'] = pd.to_datetime(df_from_explodido['overview_date'], errors='coerce').dt.normalize()

                    cutoff = pd.Timestamp('2025-06-30')
                    keys_britech = pos_britech.loc[
                        pos_britech['overview_date'] <= cutoff,
                        ['overview_date','portfolio_id','instrument_name','book_name']
                    ].drop_duplicates()

                    if not keys_britech.empty:
                        df_from_explodido = df_from_explodido.merge(
                            keys_britech.assign(_drop=1),
                            on=['overview_date','portfolio_id','instrument_name','book_name'],
                            how='left'
                        )
                        # remove do explodido tudo que o britech cobre (<= 30/06)
                        df_from_explodido = df_from_explodido[df_from_explodido['_drop'].isna()].drop(columns=['_drop'])
                        df_from_exploded = df_from_explodido

            # 3) junta EXPLÓDIDO (limpo), BRITECH e BETA
            df_positions_f = pd.concat([df_from_exploded, pos_britech, filtered_beta], ignore_index=True)

            # 4) SOMA DUPLICADOS por dia (chaves) antes dos shifts
            if not df_positions_f.empty:
                for c in ['asset_value','exposure_value','dtd_ativo_fin']:
                    if c in df_positions_f.columns:
                        df_positions_f[c] = pd.to_numeric(df_positions_f[c], errors='coerce').fillna(0.0)

                group_keys = ['overview_date','portfolio_id','instrument_name','book_name']
                agg_dict = {'asset_value':'sum', 'exposure_value':'sum', 'dtd_ativo_fin':'sum'}
                if 'instrument_id' in df_positions_f.columns:
                    agg_dict['instrument_id'] = 'first'

                # normaliza data para garantir agrupamento correto
                df_positions_f['overview_date'] = pd.to_datetime(df_positions_f['overview_date'], errors='coerce').dt.normalize()

                df_positions_f = (
                    df_positions_f
                    .groupby(group_keys, as_index=False)
                    .agg(agg_dict)
                    .sort_values(['portfolio_id','instrument_name','overview_date'])
                )

                # 5) lags e métricas
                df_positions_f['exposure_value_ontem'] = (
                    df_positions_f.groupby(['portfolio_id','instrument_name'])['exposure_value'].shift(1)
                )
                df_positions_f['asset_value_ontem'] = (
                    df_positions_f.groupby(['portfolio_id','instrument_name'])['asset_value'].shift(1)
                )
                df_positions_f = df_positions_f.dropna(subset=['exposure_value_ontem'])

                df_positions_f['dtd_ativo_pct'] = df_positions_f['dtd_ativo_fin'] / df_positions_f['exposure_value_ontem']

                df_positions_f['Year']  = df_positions_f['overview_date'].dt.year
                df_positions_f['Month'] = df_positions_f['overview_date'].dt.month

                df_positions_f['mtd_ativo_pct'] = df_positions_f.groupby(
                    ['portfolio_id','instrument_name','Year','Month']
                )['dtd_ativo_pct'].transform(lambda x: (1 + x).cumprod() - 1)

                df_positions_f['ytd_ativo_pct'] = df_positions_f.groupby(
                    ['portfolio_id','instrument_name','Year']
                )['dtd_ativo_pct'].transform(lambda x: (1 + x).cumprod() - 1)

            # 6) junta posições e CPR do ptf
            df_concat = pd.concat([df_positions_f, df_cpr], ignore_index=True) \
                        if (not df_positions_f.empty or not df_cpr.empty) else df_positions_f

            if not df_concat.empty:
                df_concat['overview_date'] = pd.to_datetime(df_concat['overview_date'])
                df_concat = df_concat.fillna(0)
                df_final = pd.concat([df_final, df_concat], ignore_index=True)


    # ========================
    # SPLIT HIERARQUIA
    # ========================
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

    # ========================
    # PERSISTÊNCIA INCREMENTAL
    # ========================
    logfn("Atualizando CSVs incrementalmente...")

    # groups.csv
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

    # positions.csv
    if isinstance(df_all, pd.DataFrame) and not df_all.empty:
        df_all = df_all.copy()
        df_all["overview_date"] = pd.to_datetime(df_all["overview_date"], errors="coerce").dt.normalize()
        upsert_by_date(
            CSV_POSITIONS_PATH,
            df_all,
            date_col="overview_date",
            unique_keys=["overview_date", "portfolio_id", "instrument_id", "instrument_name", "book_name"],
            logfn=logfn,
        )
    else:
        logfn("[SKIP] positions.csv: df_all vazio.")

    # costs_breakdown.csv
    costs_out = locals().get('todos_custos_explodidos', pd.DataFrame()).copy()
    logfn(f"[CHK] costs_out shape={getattr(costs_out,'shape',None)} cols={list(getattr(costs_out,'columns',[]))[:8]}")
    if not costs_out.empty:
        costs_out["overview_date"] = pd.to_datetime(costs_out["overview_date"], errors="coerce").dt.normalize()
        britech_local = locals().get('britechdf', pd.DataFrame())
        if not britech_local.empty and 'portfolio_id' in britech_local.columns:
            costs_out = costs_out[costs_out['portfolio_id'].isin(britech_local['portfolio_id'].unique())]
        upsert_by_date(
            CSV_COSTS_PATH,
            costs_out,
            date_col="overview_date",
            unique_keys=["overview_date", "portfolio_id", "instrument_name", "book_name"],
            float_format="%.16f",
            logfn=logfn,
        )
    else:
        logfn("[SKIP] costs_breakdown.csv: costs_out vazio.")

    logfn("✔ ETL finalizado com sucesso.")



# =========================
# GUI principal (Tkinter)
# =========================
def main():
    root = tk.Tk()
    root.title("Perenne – Atualização de CSVs (ETL)")
    root.geometry("900x640")

    frm = tk.Frame(root, padx=10, pady=10)
    frm.pack(fill="both", expand=True)

    tk.Label(frm, text="Data inicial:").grid(row=0, column=0, sticky="w")
    ent_data = DateEntry(
        frm,
        width=12,
        background="darkblue",
        foreground="white",
        borderwidth=2,
        date_pattern="dd-mm-yyyy"
    )
    ent_data.grid(row=0, column=1, padx=10)
    ent_data.set_date(datetime.strptime("01-06-2025", "%d-%m-%Y"))

    txt_log = scrolledtext.ScrolledText(frm, height=30)
    txt_log.grid(row=2, column=0, columnspan=5, sticky="nsew", pady=(10,0))

    def on_process():
        try:
            d_user = ent_data.get_date()
            ts_start = pd.Timestamp(d_user)

            do_update_beta = messagebox.askyesno(
                "Atualizar curva do Beta?",
                "Deseja atualizar agora a curva do Beta a partir do Excel?"
            )

            btn["state"] = "disabled"
            _gui_log(txt_log, f"Iniciando ETL a partir de {ts_start.strftime('%Y-%m-%d')}...\n")

            def worker():
                try:
                    if do_update_beta:
                        try:
                            update_beta_curve(logfn=lambda m: _gui_log(txt_log, m))
                        except Exception as e:
                            _gui_log(txt_log, f"Aviso ao atualizar Beta: {e}")

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

    frm.grid_rowconfigure(2, weight=1)
    for c in range(5):
        frm.grid_columnconfigure(c, weight=1)

    root.mainloop()


if __name__ == "__main__":
    main()
