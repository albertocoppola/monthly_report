# process.py
# -----------------------------------------------------------------------------
# GUI unificada (Tkinter) com duas abas:
#   1) ETL (pipeline original, incremental)
#   2) Feed Prices (Outlook/PDF → prévia por ativo → upload Bluedeck)
#
# - Threads para não travar a UI
# - Pré-visualização por ativo (ttk.Treeview) com ordenação e Export CSV
# - COM do Outlook inicializado no thread (pythoncom.CoInitializeEx)
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import csv
import re
import threading
import traceback
import warnings
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Union

warnings.simplefilter(action='ignore', category=FutureWarning)

# ---------- GUI ----------
import tkinter as tk
from tkinter import messagebox, scrolledtext, filedialog
from tkinter import ttk
from tkcalendar import DateEntry

# ---------- Dados / HTTP ----------
import numpy as np
import pandas as pd
import requests
import pandas_market_calendars as mcal

# ---------- Excel (Beta curve) ----------
import xlwings as xw

# ---------- Feed Prices deps ----------
import win32com.client
from win32com.client import gencache
import pythoncom
from PyPDF2 import PdfReader

# --- DIFF HELPERS (comparativo por run) ---
import uuid, hashlib, json
from datetime import timezone

DIFF_LOG_CSV = Path("data/change_events.csv")

def _canonicalize_for_diff(df, keys, numeric_precision=8):
    if df is None or df.empty:
        return pd.DataFrame(columns=keys)
    df = df.copy()
    # normaliza datas
    for k in keys:
        if "date" in k and k in df.columns:
            df[k] = pd.to_datetime(df[k], errors="coerce").dt.normalize()
    # arredonda numéricos (exceto as chaves)
    num_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c not in keys]
    if num_cols:
        df[num_cols] = df[num_cols].round(numeric_precision)
    # agrega duplicatas por chave (soma numéricos, first() pros demais)
    agg = {c: ('sum' if c in num_cols else 'first') for c in df.columns if c not in keys}
    if agg:
        df = df.groupby(keys, as_index=False).agg(agg)
    else:
        df = df.drop_duplicates(subset=keys, keep="last")
    # ordena colunas: chaves primeiro
    others = [c for c in df.columns if c not in keys]
    return df[keys + others]

def _row_hash_series(df, cols):
    def _val(v):
        if pd.isna(v): return ''
        return str(v)
    if not cols:
        return pd.Series([""]*len(df), index=df.index)
    return (df[cols].astype(object)
             .applymap(_val)
             .agg('|'.join, axis=1)
             .apply(lambda s: hashlib.sha256(s.encode('utf-8')).hexdigest()))

def _changed_cols(row, tracked):
    out=[]
    for c in tracked:
        a = row.get(f"{c}_new", np.nan)
        b = row.get(f"{c}_cur", np.nan)
        # NaN == NaN
        if (pd.isna(a) and pd.isna(b)) or (a == b):
            continue
        out.append(c)
    return out

def _append_change_events(events_df):
    # colunas base SEMPRE no CSV (inclui book_name para suportar 'costs')
    cols_base = [
        "entity","change_type","run_id","run_ts",
        "overview_date","portfolio_id","instrument_name","book_name",
        "old_hash","new_hash","changed_cols"
    ]

    DIFF_LOG_CSV.parent.mkdir(parents=True, exist_ok=True)
    header_needed = not DIFF_LOG_CSV.exists()

    # Se vier vazio, não cria arquivo com header incompleto.
    if events_df is None or events_df.empty:
        return

    # Garante todas as bases e mantém dinâmicas (_old/_new/_net)
    for c in cols_base:
        if c not in events_df.columns:
            events_df[c] = np.nan

    dyn_cols = [c for c in events_df.columns if c not in cols_base]
    cols_out = cols_base + dyn_cols
    events_df = events_df[cols_out].copy()
    events_df["changed_cols"] = events_df["changed_cols"].astype(str)

    events_df.to_csv(
        DIFF_LOG_CSV,
        mode="a",
        header=header_needed,
        index=False,
        quoting=csv.QUOTE_ALL,
        lineterminator="\n",
        encoding="utf-8"
    )



def diff_and_log(entity, old_df, new_df, *, keys, tracked_cols, logfn=print):
    """Compara 'old_df' vs 'new_df' apenas nas keys/datas dadas e loga em CSV."""
    if new_df is None or new_df.empty:
        logfn(f"[{entity}] Nada novo para comparar.")
        return

    run_id = str(uuid.uuid4())
    run_ts = pd.Timestamp.now(tz=timezone.utc)

    # garante as colunas
    for c in tracked_cols:
        if c not in new_df.columns:
            new_df[c] = np.nan
    if old_df is None or old_df.empty:
        old_df = pd.DataFrame(columns=keys + tracked_cols)

    old_c = _canonicalize_for_diff(old_df, keys)
    new_c = _canonicalize_for_diff(new_df, keys)

    # alinhar colunas
    all_cols = sorted(set(old_c.columns).union(new_c.columns))
    old_c = old_c.reindex(columns=all_cols)
    new_c = new_c.reindex(columns=all_cols)

    # hashes
    new_c['row_hash_new'] = _row_hash_series(new_c, tracked_cols)
    old_c['row_hash_cur'] = _row_hash_series(old_c, tracked_cols)

    merged = new_c.merge(old_c, on=keys, how='outer', suffixes=('_new','_cur'), indicator=True)

    inserts = merged[merged['_merge'] == 'left_only'].copy()
    deletes = merged[merged['_merge'] == 'right_only'].copy()
    updates = merged[(merged['_merge'] == 'both') & (merged['row_hash_new'] != merged['row_hash_cur'])].copy()

    # monta events
    def _row_json(r, suffix):
        d = {c: r.get(f"{c}{suffix}", np.nan) for c in tracked_cols}
        # serializa de forma segura
        def _safe(v):
            if isinstance(v, (np.floating,)):
                return None if pd.isna(v) else float(v)
            if isinstance(v, (np.integer,)):
                return int(v)
            if pd.isna(v): return None
            return v
        d = {k:_safe(v) for k,v in d.items()}
        return json.dumps(d, ensure_ascii=False)

    events = []

    def _val_num(v):
        try:
            if pd.isna(v): return None
            return float(v)
        except Exception:
            return None

    def _changed_cols_vals(r, cols):
        out=[]
        for c in cols:
            a = r.get(f"{c}_new", np.nan)
            b = r.get(f"{c}_cur", np.nan)
            if (pd.isna(a) and pd.isna(b)) or (a == b):
                continue
            out.append(c)
        return out

    # INSERTS
    for _, r in inserts.iterrows():
        rec = {
            "entity": entity, "change_type": "INSERT",
            "run_id": run_id, "run_ts": run_ts,
            **{k: r[k] for k in keys},
            "old_hash": None, "new_hash": r.get("row_hash_new"),
            "changed_cols": ",".join(tracked_cols),
        }
        for c in tracked_cols:
            new_v = _val_num(r.get(f"{c}_new"))
            rec[f"{c}_old"] = None
            rec[f"{c}_new"] = new_v
            rec[f"{c}_net"] = new_v  # old=0
        events.append(rec)

    # UPDATES
    for _, r in updates.iterrows():
        changed = _changed_cols_vals(r, tracked_cols)
        rec = {
            "entity": entity, "change_type": "UPDATE",
            "run_id": run_id, "run_ts": run_ts,
            **{k: r[k] for k in keys},
            "old_hash": r.get("row_hash_cur"), "new_hash": r.get("row_hash_new"),
            "changed_cols": ",".join(changed),
        }
        for c in tracked_cols:
            old_v = _val_num(r.get(f"{c}_cur"))
            new_v = _val_num(r.get(f"{c}_new"))
            net_v = (new_v or 0.0) - (old_v or 0.0)
            rec[f"{c}_old"] = old_v
            rec[f"{c}_new"] = new_v
            rec[f"{c}_net"] = net_v
        events.append(rec)

    # DELETES (soft)
    for _, r in deletes.iterrows():
        rec = {
            "entity": entity, "change_type": "DELETE_SOFT",
            "run_id": run_id, "run_ts": run_ts,
            **{k: r[k] for k in keys},
            "old_hash": r.get("row_hash_cur"), "new_hash": None,
            "changed_cols": "",
        }
        for c in tracked_cols:
            old_v = _val_num(r.get(f"{c}_cur"))
            rec[f"{c}_old"] = old_v
            rec[f"{c}_new"] = None
            rec[f"{c}_net"] = - (old_v or 0.0)
        events.append(rec)

    events_df = pd.DataFrame(events) if events else pd.DataFrame(columns=[
        "entity","change_type","run_id","run_ts", *keys,
        "old_hash","new_hash","changed_cols", *[f"{c}_{suf}" for c in tracked_cols for suf in ("old","new","net")]
    ])
    _append_change_events(events_df)

    ins, upd, dele = len(inserts), len(updates), len(deletes)
    logfn(f"[{entity}] Diff: +{ins} inserts, +{upd} updates, +{dele} deletes")


# =========================
# Config de API / Caminhos
# =========================
API_ACCESS_ID = "89297662e92386720e192e56ffdc0d5e.access"
API_SECRET    = "b8b3cfabf25982a64a1074360f83b0dc143aa5bd75560abf5c901b0977364de4"
API_USERNAME  = "alberto.coppola@perenneinvestimentos.com.br"
API_PASSWORD  = "juNTr1QbtbY9NZ8ACrMF"
BASE_URL      = "https://perenne.bluedeck.com.br/api"

CSV_GROUPS_PATH    = "data/groups.csv"
CSV_POSITIONS_PATH = "data/positions.csv"
CSV_COSTS_PATH     = "data/costs_breakdown.csv"
CSV_FUNDS_NAME     = "data/funds_name.csv"
CSV_BENCHMARKS     = "data/benchmarks.csv"

# Beta curve
EXCEL_PATH   = r"Beta RF Curva.xlsx"
SHEET_NAME   = "Hist"
TARGET_COLS  = ["data", "NAV", "P&L dia", "daily ret."]
BETA_FEED_FN = Path("feed_data") / "beta_curva.csv"

# Feed Prices
fundos_ids = {
    "FIDC KYKLOS-SUB": 1222,
    "NC PAR FIP": 1223,
    "FIDC FRADINHO-SR": 1231,
    "FIDC Latache 1962": 1221,
    "ELEVA EDUCACAO I FIP MULTI": 1225,
    "HONEY ISLAND BY 4UM FIP MULTIESTRATEGIA": 1229,
}
subject_filters = [
    "FIDC KYKLOS-SUB",
    "FIDC FRADINHO-SR",
    "FIDC Latache 1962",
    "NC PAR FIP",
    "HONEY ISLAND BY 4UM",
    "HONEY ISLAND BY 4UM FIP MULTIESTRATEGIA"
]
PRICE_SOURCE_ID = 7


# =========================
# Utilidades de IO / GUI
# =========================
def _gui_log(text_widget: scrolledtext.ScrolledText, msg: str) -> None:
    try:
        text_widget.insert(tk.END, f"{msg}\n")
        text_widget.see(tk.END)
        text_widget.update_idletasks()
    except Exception:
        print(msg)


def read_csv_safe(path: Union[str, Path], **kwargs) -> pd.DataFrame:
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
    def _log(msg: str):
        try: (logfn or print)(msg)
        except Exception: print(msg)

    csv_path = Path(csv_path)
    if not isinstance(df_new, pd.DataFrame) or df_new.empty:
        _log(f"[SKIP] {csv_path.name}: df_new vazio — nada a atualizar.")
        return None

    if date_col not in df_new.columns:
        if getattr(df_new.index, "name", None) == date_col:
            df_new = df_new.reset_index()
        else:
            raise KeyError(f"'{date_col}' não está em df_new para {csv_path.name}.")

    df_new = df_new.copy()
    df_new[date_col] = pd.to_datetime(df_new[date_col], errors="coerce").dt.normalize()
    if df_new[date_col].isna().any():
        nbad = int(df_new[date_col].isna().sum())
        raise ValueError(f"{csv_path.name}: {nbad} linhas com data inválida em df_new[{date_col}]")

    if csv_path.exists() and csv_path.stat().st_size > 0:
        try: df_old = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError: df_old = pd.DataFrame()
    else:
        df_old = pd.DataFrame()

    if not df_old.empty:
        if date_col in df_old.columns:
            df_old = df_old.copy()
            df_old[date_col] = pd.to_datetime(df_old[date_col], errors="coerce").dt.normalize()
        else:
            df_old = pd.DataFrame(columns=df_new.columns)

    all_cols = df_old.columns.union(df_new.columns)
    df_old = df_old.reindex(columns=all_cols)
    df_new = df_new.reindex(columns=all_cols)

    new_dates = df_new[date_col].dropna().unique()
    kept_old = df_old[~df_old[date_col].isin(new_dates)] if (not df_old.empty and date_col in df_old.columns) else df_old

    out = pd.concat([kept_old, df_new], ignore_index=True)
    if unique_keys:
        subset = list(unique_keys)
        if date_col not in subset:
            subset.append(date_col)
        out = out.drop_duplicates(subset=subset, keep="last", ignore_index=True)

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
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    app = xw.App(visible=False, add_book=False)
    book = None
    try:
        book = app.books.open(str(path), update_links=False, read_only=True)
        sht = book.sheets[sheet]
        used_range = sht.used_range
        data = used_range.value
        df = pd.DataFrame(data[1:], columns=data[0])

        norm_cols = {c: str(c).strip().lower() for c in df.columns}
        reverse_map = {}
        for original, lowered in norm_cols.items():
            for wanted in target_cols:
                if lowered == wanted.strip().lower():
                    reverse_map[original] = wanted

        missing = [c for c in target_cols if c.lower() not in [v.lower() for v in reverse_map.values()]]
        if missing:
            raise KeyError(f"Colunas não encontradas: {missing}")

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
    logfn("Atualizando curva do Beta (lendo Excel)...")
    df = load_hist_columns().dropna(how='all').copy()

    df['portfolio_id']    = 1295
    df['book_name']       = 'Risco >> Renda Fixa'
    df['instrument_name'] = 'BETA CURVA'

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
# ETL núcleo
# =========================
def fetch_data(url: str, params: dict, access_id: str, secret: str, user_name: str, api_password: str) -> dict:
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

def prev_b3_business_day(ts: pd.Timestamp) -> pd.Timestamp:
    """Retorna o dia útil B3 imediatamente anterior a `ts` (data normalizada)."""
    ts = pd.Timestamp(ts).normalize()
    B3 = mcal.get_calendar('B3')
    # janela curta pra trás é suficiente e robusta (considera feriados B3)
    vd = B3.valid_days(start_date=ts - pd.Timedelta(days=10), end_date=ts)
    vd = pd.DatetimeIndex(vd).tz_localize(None)
    prev = vd[vd < ts]
    if len(prev):
        return prev.max().normalize()
    # fallback (quase nunca usado): dia útil "global"
    return (ts - pd.offsets.BDay(1)).normalize()


def run_etl_with_start_date(ts_start: pd.Timestamp, logfn=lambda m: None) -> None:
    # data escolhida pelo usuário (normalizada)
    user_start = pd.Timestamp(ts_start).normalize()
    # d-1 útil B3 para cálculo dos *_ontem*
    backfill_start = prev_b3_business_day(user_start)

    # a janela efetiva de consulta começa no backfill
    start_date = backfill_start

    # d-2 B3 (fim da janela), reaproveitando seu cálculo atual
    B3 = mcal.get_calendar('B3')
    B3_holidays = B3.holidays()
    feriados = [h for h in B3_holidays.holidays if start_date <= h <= pd.Timestamp.today().normalize()]
    bday_brasil = pd.offsets.CustomBusinessDay(holidays=feriados)
    d2_ts = (pd.Timestamp.today().normalize() - 2 * bday_brasil)
    d2_date = d2_ts.strftime('%Y-%m-%d')

    logfn(f"Data escolhida: {user_start.strftime('%Y-%m-%d')} | backfill B3: {backfill_start.strftime('%Y-%m-%d')}")
    logfn(f"Janela de processamento: {start_date.strftime('%Y-%m-%d')} até {d2_date}")

    # POSIÇÕES
    logfn("Baixando posições...")
    url = 'portfolio_position/positions/get'
    params = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": d2_date,
        "portfolio_group_ids": [1],
    }
    df_positions = fetch_data(url, params, API_ACCESS_ID, API_SECRET, API_USERNAME, API_PASSWORD)['objects']
    df_positions = pd.DataFrame(df_positions)

    # NAV por (portfolio_id, date)
    funds_nav = df_positions.loc[['portfolio_id','date','net_asset_value']].T.drop_duplicates()
    funds_nav.rename(columns={"net_asset_value":"portfolio_nav"}, inplace=True)

    # FUNDOS INTERNOS
    logfn("Listando fundos internos...")
    url_funds = 'portfolio_registration/portfolio_group/get'
    params_funds = {"get_composition": True}
    all_funds = fetch_data(url_funds, params_funds, API_ACCESS_ID, API_SECRET, API_USERNAME, API_PASSWORD)['objects']
    all_funds = pd.DataFrame(all_funds)
    all_funds = list(all_funds['8'].loc['composition_names'].values())

    funds_name = df_positions.loc[['portfolio_id','name']].T.drop_duplicates()
    funds_name.to_csv(CSV_FUNDS_NAME, index=False)

    # BENCHMARKS
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

    # ERROS PnL
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

    # REGISTROS & CUSTOS
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
                try: pnl_.append(np.float64(row.attribution['total']['financial_value']))
                except: pnl_.append(0.0)
            instrument_positions['dtd_ativo_fin'] = pnl_

            pnl_ = []
            for _, row in costs_position.iterrows():
                try: pnl_.append(np.float64(row.attribution['total']['financial_value']))
                except: pnl_.append(0.0)
            costs_position['dtd_custos_fin'] = pnl_

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
        registros['book_name'] = registros['book_name'].replace({
            "Risco >> HYPE >> Ação HYPE":  "Risco >> HYPE",
            "Risco >> HYPE >> Opção HYPE": "Risco >> HYPE"
        })

    if not costs_df.empty:
        costs_df['financial_value'] = pd.to_numeric(costs_df['financial_value'], errors='coerce')
        costs_df = costs_df[['financial_value','attribution','book_name','category_name',
                             'origin_portfolio_id','origin_accounting_transaction_id',
                             'dtd_custos_fin','overview_date']]

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

    registros = pd.merge(
        registros, funds_nav,
        left_on=['portfolio_id','overview_date'],
        right_on=['portfolio_id','date']
    ).drop(columns='date')
    registros.rename(columns={"net_asset_value":"portfolio_nav"}, inplace=True)

    # EXPLOSÃO
    def explodir_portfolio(portfolio_id, data, todas_posicoes, todos_custos, visitados=None,
                           notional=None, portfolio_origem_id=None, nivel=0):
        if visitados is None: visitados = set()
        if portfolio_id in visitados: return [], pd.DataFrame()
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
        if aum == 0: return [], pd.DataFrame()
        if notional is None: notional = aum
        mult = notional / aum

        if portfolio_origem_id is None:
            portfolio_origem_id = portfolio_id
        else:
            if not posicoes.empty:
                posicoes.loc[:, ['quantity','asset_value','exposure_value','dtd_ativo_fin']] = \
                    posicoes[['quantity','asset_value','exposure_value','dtd_ativo_fin']] * mult
            if not custos.empty:
                custos.loc[:, ['financial_value','dtd_custos_fin']] = \
                    custos[['financial_value','dtd_custos_fin']] * mult

        if not posicoes.empty: posicoes.loc[:, ['portfolio_id']] = portfolio_origem_id
        if not custos.empty:   custos.loc[:, ['root_portfolio']] = portfolio_origem_id

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

    # CONVERSÕES DE DATA / MERGE NAV
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

    # TRATAMENTOS / CPR
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

    # FEEDS EXTERNOS
    logfn("Lendo feeds externos (se existirem)...")
    try:
        # ---------- BRITECH ----------
        britechdf = pd.read_csv('feed_data/feed_britech.csv', encoding='latin').dropna(how='all')

        # normalizações
        britechdf[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']]=britechdf[['asset_value','dtd_ativo_pct','dtd_ativo_fin','exposure_value']].astype(float)
        britechdf['overview_date'] = pd.to_datetime(britechdf['overview_date'])


        # ---------- BETA CURVA ----------
        betacurve = pd.read_csv(BETA_FEED_FN, encoding='latin').dropna(how='all')
        betacurve['overview_date'] = pd.to_datetime(betacurve['overview_date'], errors='coerce').dt.normalize()
        for c in ['instrument_name','book_name']:
            if c in betacurve.columns:
                betacurve[c] = betacurve[c].astype(str).str.strip()
        for c in ['asset_value','exposure_value','dtd_ativo_fin']:
            if c in betacurve.columns:
                betacurve[c] = pd.to_numeric(betacurve[c], errors='coerce').fillna(0.0)

    except Exception as e:
        britechdf = pd.DataFrame()
        betacurve = pd.DataFrame()
        logfn(f"Aviso feeds: {e}")

    # MONTAGEM FINAL
    logfn("Montando df_final...")
    df_final = pd.DataFrame()
    if not britechdf.empty:
        for ptf in britechdf.portfolio_id.unique():
            filtered_britech = britechdf[britechdf.portfolio_id == ptf].copy()
            filtered_beta    = betacurve[betacurve.portfolio_id == ptf].copy() if not betacurve.empty else pd.DataFrame()

            # CPR
            df_cpr = pd.DataFrame()
            if not df_explodido.empty:
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

            cpr_britech = filtered_britech[filtered_britech['book_name'] == 'Risco >> Caixas e Provisionamentos'].copy()
            if not cpr_britech.empty:
                cpr_britech['overview_date'] = pd.to_datetime(cpr_britech['overview_date'])
                df_cpr = pd.concat([df_cpr, cpr_britech], ignore_index=True).reset_index(drop=True)

            # POSIÇÕES
            df_from_exploded = df_explodido[(df_explodido['book_name'].isin(list(filtered_britech.book_name.drop_duplicates()))) & 
                (df_explodido['portfolio_id']== ptf)].copy()
                
            # adicionando offshore a amostra
            df_offshore = df_explodido[(df_explodido['book_name'].str.lower().str.startswith('off'))& 
                            (df_explodido['portfolio_id']== ptf)]
            
            df_from_exploded = pd.concat([df_from_exploded,df_offshore],ignore_index=True).reset_index(drop=True)

            pos_britech = filtered_britech[filtered_britech['book_name'] != 'Risco >> Caixas e Provisionamentos'].copy()

            # normalização mínima (datas, numéricos e nome do instrumento sem espaços)
            for _df in (df_from_exploded, pos_britech, filtered_beta):
                if isinstance(_df, pd.DataFrame) and not _df.empty:
                    _df['overview_date'] = pd.to_datetime(_df['overview_date'], errors='coerce').dt.normalize()
                    if 'instrument_name' in _df.columns:
                        _df['instrument_name'] = _df['instrument_name'].astype(str).str.strip()
                    for c in ['asset_value','exposure_value','dtd_ativo_fin']:
                        if c in _df.columns:
                            _df[c] = pd.to_numeric(_df[c], errors='coerce').fillna(0.0)


            df_positions_f = df_from_exploded.sort_values(['portfolio_id','instrument_name','overview_date'])

            if not df_positions_f.empty:
                for c in ['asset_value','exposure_value','dtd_ativo_fin']:
                    if c in df_positions_f.columns:
                        df_positions_f[c] = pd.to_numeric(df_positions_f[c], errors='coerce').fillna(0.0)

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

            # conjunto final sem remover linha nenhuma
            df_positions_f = pd.concat(
                [df_positions_f, pos_britech, filtered_beta],
                ignore_index=True
            )
            df_concat = pd.concat([df_positions_f, df_cpr], ignore_index=True) \
                        if (not df_positions_f.empty or not df_cpr.empty) else df_positions_f

            if not df_concat.empty:
                df_concat['overview_date'] = pd.to_datetime(df_concat['overview_date'])
                df_concat = df_concat.fillna(0)
                df_final = pd.concat([df_final, df_concat], ignore_index=True)

    # SPLIT HIERARQUIA
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

    # PERSISTÊNCIA INCREMENTAL
    logfn("Atualizando CSVs incrementalmente...")

    # >>>>>>>>>>>>>>>>>>>>>>>>>> NOVO: recorte para salvar APENAS da data escolhida em diante
    #     (ignora o dia de backfill na persistência e no diff de positions)
    df_all_to_save = pd.DataFrame()
    if isinstance(df_all, pd.DataFrame) and not df_all.empty:
        df_all["overview_date"] = pd.to_datetime(df_all["overview_date"], errors="coerce").dt.normalize()
        df_all_to_save = df_all[df_all["overview_date"] >= user_start].copy()
    # <<<<<<<<<<<<<<<<<<<<<<<<<<

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
    if isinstance(df_all_to_save, pd.DataFrame) and not df_all_to_save.empty:
        df_all_to_save = df_all_to_save.copy()
        df_all_to_save["overview_date"] = pd.to_datetime(df_all_to_save["overview_date"], errors="coerce").dt.normalize()

        # --- DIFF POSITIONS (janela = [start_date .. d2_date], só dtd_ativo_fin, tolerância ±1, ignora feeds estáticos) ---
        try:
            prev_positions = read_csv_safe(CSV_POSITIONS_PATH)

            # janela do usuário (não deixa vazar 31/07 se o usuário escolheu 01/08)
            win_start = pd.to_datetime(start_date).normalize()
            win_end   = pd.to_datetime(d2_date).normalize()  # já existe no seu código
            THRESH = 1.0  # só contar UPDATE se |Δ dtd_ativo_fin| >= 1

            # recorta df_all para a janela
            dfw = df_all.copy()
            dfw["overview_date"] = pd.to_datetime(dfw["overview_date"], errors="coerce").dt.normalize()
            dfw = dfw[(dfw["overview_date"] >= win_start) & (dfw["overview_date"] <= win_end)]

            # nada pra comparar?
            if dfw.empty:
                logfn("[positions] Diff: janela vazia (sem datas na faixa).")
            else:
                POS_KEYS = ["overview_date", "portfolio_id", "instrument_name"]

                # novo agregado (somente dtd_ativo_fin) sem feeds estáticos
                new_slice = (
                    dfw.groupby(POS_KEYS, as_index=False)
                       .agg({"dtd_ativo_fin": "sum"})
                )

                # antigo agregado (recortado pela janela e sem feeds estáticos)
                old_slice = pd.DataFrame()
                if not prev_positions.empty and "overview_date" in prev_positions.columns:
                    prev = prev_positions.copy()
                    prev["overview_date"] = pd.to_datetime(prev["overview_date"], errors="coerce").dt.normalize()
                    prev = prev[(prev["overview_date"] >= win_start) & (prev["overview_date"] <= win_end)]
                    if "dtd_ativo_fin" in prev.columns:
                        old_slice = (
                            prev.groupby(POS_KEYS, as_index=False)
                                .agg({"dtd_ativo_fin": "sum"})
                        )

                # aplica tolerância: neutraliza updates com |Δ| < THRESH (só para o LOG)
                both = new_slice.merge(old_slice, on=POS_KEYS, how="outer", suffixes=("_new","_old"))
                mask_both = both["dtd_ativo_fin_new"].notna() & both["dtd_ativo_fin_old"].notna()
                delta = (both["dtd_ativo_fin_new"].fillna(0) - both["dtd_ativo_fin_old"].fillna(0))
                small = mask_both & (delta.abs() < THRESH)
                both.loc[small, "dtd_ativo_fin_new"] = both.loc[small, "dtd_ativo_fin_old"]

                # recria slices filtrados pro diff genérico
                new_slice_f = (
                    both[POS_KEYS + ["dtd_ativo_fin_new"]]
                    .rename(columns={"dtd_ativo_fin_new": "dtd_ativo_fin"})
                    .dropna(subset=["dtd_ativo_fin"])
                    .groupby(POS_KEYS, as_index=False)["dtd_ativo_fin"].sum()
                )
                old_slice_f = (
                    both[POS_KEYS + ["dtd_ativo_fin_old"]]
                    .rename(columns={"dtd_ativo_fin_old": "dtd_ativo_fin"})
                    .dropna(subset=["dtd_ativo_fin"])
                    .groupby(POS_KEYS, as_index=False)["dtd_ativo_fin"].sum()
                )
                diff_and_log(
                    "positions",
                    old_slice_f, new_slice_f,
                    keys=POS_KEYS,
                    tracked_cols=["dtd_ativo_fin"],
                    logfn=logfn
                )
        except Exception as _e:
            logfn(f"[WARN] Diff positions falhou: {_e}")

        upsert_by_date(
            CSV_POSITIONS_PATH,
            df_all_to_save,
            date_col="overview_date",
            unique_keys=None,
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
        # --- DIFF COSTS (comparativo por run) ---
        try:
            prev_costs = read_csv_safe(CSV_COSTS_PATH)
            new_dates_c = pd.to_datetime(costs_out["overview_date"], errors="coerce").dt.normalize().dropna().unique()
            old_slice_c = pd.DataFrame()
            if not prev_costs.empty and "overview_date" in prev_costs.columns:
                prev_costs["overview_date"] = pd.to_datetime(prev_costs["overview_date"], errors="coerce").dt.normalize()
                old_slice_c = prev_costs[prev_costs["overview_date"].isin(new_dates_c)].copy()

            COST_KEYS = ["overview_date", "portfolio_id", "instrument_name", "book_name"]
            # aqui os nomes são os do costs_out (antes de renomear): financial_value / dtd_custos_fin
            COST_TRACKED = [c for c in ["financial_value","dtd_custos_fin"] if c in costs_out.columns]

            new_slice_c = costs_out[costs_out["overview_date"].isin(new_dates_c)].copy()
            diff_and_log(
                "costs",
                old_slice_c, new_slice_c,
                keys=COST_KEYS,
                tracked_cols=COST_TRACKED,
                logfn=logfn
            )
        except Exception as _e:
            logfn(f"[WARN] Diff costs falhou: {_e}")
        upsert_by_date(
            CSV_COSTS_PATH,
            costs_out,
            date_col="overview_date",
            unique_keys=None,
            float_format="%.16f",
            logfn=logfn,
        )
    else:
        logfn("[SKIP] costs_breakdown.csv: costs_out vazio.")

    logfn("✔ ETL finalizado com sucesso.")


# =========================
# Feed Prices – helpers
# =========================
def email_matches_subjects(subject, subject_filters):
    if subject is None:
        return False
    subject = subject.lower()
    return any(s.lower() in subject for s in subject_filters)


def extract_date_from_subject(subject):
    match = re.search(r'(\d{2}[/-]\d{2}[/-]\d{4})', subject)
    if match:
        date_str = match.group(1).replace("-", "/")
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").date()
        except Exception:
            pass
    return None


def extract_cota_from_body(body: str, fundos: list) -> list:
    def convert_price(valor_str: str) -> float:
        if ',' in valor_str:
            return float(valor_str.replace('.', '').replace(',', '.'))
        return float(valor_str)

    def extract_original_patterns(body_text: str, fund_name: str) -> list:
        entries = []
        fund_esc = re.escape(fund_name)

        pattern1 = rf"{fund_esc}.*?:\s*\n?\s*(\d{{2}}/\d{{2}}/\d{{4}})\s*Cota:\s*([0-9\.,]+)"
        for dt, val_str in re.findall(pattern1, body_text, flags=re.IGNORECASE):
            try:
                entries.append((fund_name, dt, convert_price(val_str)))
            except Exception:
                pass

        pattern2 = rf"{fund_esc}[:\s]+([0-9\.,]+)"
        m2 = re.search(pattern2, body_text, flags=re.IGNORECASE)
        if m2:
            val_str = m2.group(1)
            has_decimal = False
            if ',' in val_str and val_str.rfind(',') < len(val_str) - 1: has_decimal = True
            if '.' in val_str and val_str.rfind('.') < len(val_str) - 1: has_decimal = True
            if has_decimal:
                try:
                    entries.append((fund_name, None, convert_price(val_str)))
                except Exception:
                    pass

        pattern3 = rf"{fund_esc}[^\n]*?(\d{{2}}/\d{{2}}/\d{{4}})[\t\s]+([0-9\.,]+)"
        for dt, val_str in re.findall(pattern3, body_text, flags=re.IGNORECASE):
            try:
                entries.append((fund_name, dt, convert_price(val_str)))
            except Exception:
                pass

        return entries

    resultados = []
    for fundo in fundos:
        if fundo.lower() == "honey island by 4um":
            continue

        fund_esc = re.escape(fundo)

        if fundo.lower().startswith("honey island by 4um fip"):
            pattern_fip = rf"{fund_esc}.*?(\d{{2}}/\d{{2}}/\d{{4}}).*?([0-9]+,[0-9]{{6,}})"
            matches = re.findall(pattern_fip, body, flags=re.IGNORECASE | re.DOTALL)
            if matches:
                for dt, val_str in matches:
                    try: resultados.append((fundo, dt, convert_price(val_str)))
                    except Exception: pass
            else:
                resultados.extend(extract_original_patterns(body, fundo))
        else:
            resultados.extend(extract_original_patterns(body, fundo))

    resultados_filtrados = []
    for f in {r[0] for r in resultados}:
        entradas = [r for r in resultados if r[0] == f]
        entradas_com_data = [r for r in entradas if r[1] is not None]
        if entradas_com_data:
            resultados_filtrados.extend(entradas_com_data)
        else:
            resultados_filtrados.extend(entradas)

    return resultados_filtrados


def read_all_emails(folder, subject_filters, min_date, folder_path="", resultados=None):
    if resultados is None: resultados = []
    folder_path = f"{folder_path}/{folder.Name}"
    try:
        messages = folder.Items
        for message in messages:
            try:
                if message.Class != 43:
                    continue
                subject = message.Subject or ""
                email_date = extract_date_from_subject(subject) or message.ReceivedTime.date()
                if email_date and email_date >= min_date.date():
                    if email_matches_subjects(subject, subject_filters):
                        corpo = message.Body
                        cotas = extract_cota_from_body(corpo, subject_filters)
                        for fundo, data_cota, valor in cotas:
                            final_date = data_cota if data_cota else str(email_date)
                            resultados.append({
                                "date": final_date,
                                "instrument_id": fundos_ids[fundo],
                                "price": valor
                            })
            except Exception:
                continue
        for subfolder in folder.Folders:
            read_all_emails(subfolder, subject_filters, min_date, folder_path, resultados)
    except Exception as e:
        print(f"Erro na pasta {folder_path}: {e}")
    return resultados


def sanitize_filename(filename):
    return re.sub(r'[\/:*?"<>|]', '_', filename)


def extract_cotas_eleva_from_pdf(pdf_path):
    reader = PdfReader(pdf_path)
    text = ''
    for page in reader.pages:
        t = page.extract_text()
        if t:
            text += t + '\n'
    cotas = []
    for match in re.finditer(r'(\d{2}/\d{2}/\d{4})\s+([0-9]+\.[0-9]+)', text):
        data = match.group(1)
        cota = float(match.group(2))
        cotas.append({"date": data, "price": cota})
    return cotas


def find_latest_email(folder, subject_filter, latest=None):
    try:
        messages = folder.Items
        messages.Sort("[ReceivedTime]", True)
        for message in messages:
            try:
                if message.Class != 43:
                    continue
                subject = (message.Subject or "")
                if subject_filter.lower() in subject.lower():
                    if (latest is None) or (message.ReceivedTime > latest.ReceivedTime):
                        latest = message
            except Exception:
                continue
        for subfolder in folder.Folders:
            latest = find_latest_email(subfolder, subject_filter, latest)
    except Exception:
        pass
    return latest


def buscar_cotas_eleva(outlook, min_date, subject_filter, instrument_id):
    temp_dir = str(Path.home() / "temp_pdf_eleva")
    os.makedirs(temp_dir, exist_ok=True)
    latest = None
    for i in range(outlook.Folders.Count):
        root_folder = outlook.Folders.Item(i+1)
        latest = find_latest_email(root_folder, subject_filter, latest)
    if not latest:
        print(f"Nenhum email encontrado com '{subject_filter}'.")
        return []
    print(f"Usando email recebido em: {latest.ReceivedTime}\nAssunto: {latest.Subject}")
    all_cotas = []
    for i in range(latest.Attachments.Count):
        att = latest.Attachments.Item(i+1)
        if att.FileName.lower().endswith(".pdf"):
            filename = sanitize_filename(att.FileName)
            pdf_path = os.path.join(temp_dir, filename)
            att.SaveAsFile(pdf_path)
            cotas = extract_cotas_eleva_from_pdf(pdf_path)
            for item in cotas:
                cota_date = datetime.strptime(item['date'], "%d/%m/%Y").date()
                if cota_date >= min_date.date():
                    all_cotas.append({
                        "date": item["date"],
                        "instrument_id": instrument_id,
                        "price": item["price"]
                    })
            os.remove(pdf_path)
    return all_cotas


def padronizar_data(data_str):
    if re.match(r"\d{2}/\d{2}/\d{4}", data_str):
        return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    if re.match(r"\d{4}-\d{2}-\d{2}", data_str):
        return data_str
    raise ValueError(f"Formato de data não reconhecido: {data_str}")


def criar_precos_ativos(prices, price_source_id, logfn=print):
    base_url = BASE_URL
    url_token = "auth/token"
    url_precos = "market_data/pricing/prices/create"

    data = {"username": API_USERNAME, "password": API_PASSWORD}
    client_headers = {
        "CF-Access-Client-Id": API_ACCESS_ID,
        "CF-Access-Client-Secret": API_SECRET
    }
    resp_token = requests.post(f"{base_url}/{url_token}", data=data, headers=client_headers)
    resp_token.raise_for_status()
    token_info = resp_token.json()
    access_token = token_info['access_token']
    token_type = token_info['token_type']

    headers = {
        "CF-Access-Client-Id": API_ACCESS_ID,
        "CF-Access-Client-Secret": API_SECRET,
        "Authorization": f"{token_type} {access_token}",
        "Content-Type": "application/json",
        "accept": "application/json"
    }
    payload = {"prices": prices, "price_source_id": price_source_id}
    url_final = f"{base_url}/{url_precos}"
    logfn(f"Enviando para: {url_final}")
    resp = requests.put(url_final, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.status_code, resp.json()


# =========================
# UI: prévia por ativo
# =========================
class PricesPreview(tk.Frame):
    """Notebook com uma aba por fundo e Treeview ordenável."""
    def __init__(self, master):
        super().__init__(master)
        self._dfs_by_tab: dict[str, pd.DataFrame] = {}
        self._trees: dict[str, ttk.Treeview] = {}

        bar = tk.Frame(self)
        bar.pack(fill="x", padx=2, pady=(0,6))
        self.lbl_summary = tk.Label(bar, text="No data loaded.")
        self.lbl_summary.pack(side="left")

        tk.Button(bar, text="Export CSV", command=self._export_csv).pack(side="right", padx=4)
        tk.Button(bar, text="Copy selected", command=self._copy_selected).pack(side="right", padx=4)

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True)

        style = ttk.Style()
        style.configure("Treeview", rowheight=22)

    def clear(self):
        for tab in self.nb.tabs():
            self.nb.forget(tab)
        self._dfs_by_tab.clear()
        self._trees.clear()
        self.lbl_summary.config(text="No data loaded.")

    def load(self, df_prices: pd.DataFrame):
        self.clear()
        if df_prices is None or df_prices.empty:
            self.lbl_summary.config(text="No rows.")
            return

        df_prices = df_prices.copy().sort_values(["fundo","date"]).reset_index(drop=True)
        total = len(df_prices)
        dmin = pd.to_datetime(df_prices["date"]).min().strftime("%Y-%m-%d")
        dmax = pd.to_datetime(df_prices["date"]).max().strftime("%Y-%m-%d")
        self.lbl_summary.config(text=f"{total} rows | {dmin} → {dmax} | {df_prices['fundo'].nunique()} funds")

        for fund, df_f in df_prices.groupby("fundo", sort=True):
            frm = tk.Frame(self.nb)
            self.nb.add(frm, text=fund)

            cols = ("date","price","instrument_id")
            tree = ttk.Treeview(frm, columns=cols, show="headings")
            vsb = ttk.Scrollbar(frm, orient="vertical", command=tree.yview)
            hsb = ttk.Scrollbar(frm, orient="horizontal", command=tree.xview)
            tree.configure(yscroll=vsb.set, xscroll=hsb.set)

            headers = {"date":"Date", "price":"Price", "instrument_id":"Instrument ID"}
            for c in cols:
                tree.heading(c, text=headers[c], command=lambda col=c, t=tree: self._sort_by(t, col))
                tree.column(c, width=140 if c!="instrument_id" else 120, anchor="center")

            for i, r in df_f.iterrows():
                tags = ("even",) if i % 2 == 0 else ("odd",)
                tree.insert("", "end", values=(str(r["date"]), f"{float(r['price']):.6f}", int(r["instrument_id"])), tags=tags)

            tree.tag_configure("even", background="#f7f7f7")
            tree.tag_configure("odd",  background="#ffffff")

            tree.grid(row=0, column=0, sticky="nsew")
            vsb.grid(row=0, column=1, sticky="ns")
            hsb.grid(row=1, column=0, sticky="ew")
            frm.grid_rowconfigure(0, weight=1)
            frm.grid_columnconfigure(0, weight=1)

            self._trees[fund] = tree
            self._dfs_by_tab[fund] = df_f.reset_index(drop=True)

            lbl = tk.Label(frm, anchor="w")
            lbl.grid(row=2, column=0, columnspan=2, sticky="w", pady=(4,0))
            dmin_f = pd.to_datetime(df_f["date"]).min().strftime("%Y-%m-%d")
            dmax_f = pd.to_datetime(df_f["date"]).max().strftime("%Y-%m-%d")
            lbl.config(text=f"{len(df_f)} rows | {dmin_f} → {dmax_f}")

    def _current_tab(self):
        tab_id = self.nb.select()
        if not tab_id:
            return None, None, None
        text = self.nb.tab(tab_id, "text")
        return tab_id, text, self._trees.get(text)

    def _copy_selected(self):
        tab_id, fund, tree = self._current_tab()
        if not tree:
            return
        rows = []
        for iid in tree.selection():
            rows.append("\t".join(str(v) for v in tree.item(iid, "values")))
        if rows:
            self.clipboard_clear()
            self.clipboard_append("\n".join(rows))

    def _export_csv(self):
        from datetime import datetime as _dt
        ts = _dt.now().strftime("%Y%m%d_%H%M%S")
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV","*.csv")],
            initialfile=f"prices_{ts}.csv"
        )
        if not path:
            return

        _, fund, _ = self._current_tab()
        if fund and self._dfs_by_tab.get(fund) is not None:
            export_all = messagebox.askyesno("Export CSV", "Export ALL funds? (No = only current tab)")
            df = pd.concat(self._dfs_by_tab.values(), ignore_index=True) if export_all else self._dfs_by_tab[fund]
        else:
            df = pd.concat(self._dfs_by_tab.values(), ignore_index=True)

        df.to_csv(path, index=False)
        messagebox.showinfo("Export CSV", f"Saved:\n{path}")

    def _sort_by(self, tree: ttk.Treeview, col: str):
        data = [(tree.set(k, col), k) for k in tree.get_children("")]
        def _to_key(v):
            try: return float(v)
            except Exception:
                try: return pd.to_datetime(v)
                except Exception: return v
        data.sort(key=lambda x: _to_key(x[0]))
        # toggle asc/desc
        heading = tree.heading(col, "text")
        desc = heading.endswith(" ↓")
        tree.heading(col, text=heading.split(" ")[0] + (" ↑" if desc else " ↓"))
        if desc: data.reverse()
        for idx, (_, k) in enumerate(data):
            tree.move(k, "", idx)

class ChangeEventsPreview(tk.Frame):
    """Mostra change_events em abas por entidade (positions/costs) com resumo."""
    COLS_BASE = ("run_ts","change_type","overview_date","portfolio_id","instrument_name","changed_cols","delta_dtd_ativo_fin")

    def __init__(self, master):
        super().__init__(master)
        self._dfs_by_tab: dict[str, pd.DataFrame] = {}
        self._trees: dict[str, ttk.Treeview] = {}

        bar = tk.Frame(self)
        bar.pack(fill="x", padx=2, pady=(0,6))
        self.lbl_summary = tk.Label(bar, text="No change events.")
        self.lbl_summary.pack(side="left")

        tk.Button(bar, text="Refresh", command=self.refresh_from_disk).pack(side="right", padx=4)
        tk.Button(bar, text="Export CSV", command=self._export_csv).pack(side="right", padx=4)
        tk.Button(bar, text="Copy selected", command=self._copy_selected).pack(side="right", padx=4)

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True)

        style = ttk.Style()
        style.configure("Treeview", rowheight=22)

    def clear(self):
        for tab in self.nb.tabs():
            self.nb.forget(tab)
        self._dfs_by_tab.clear()
        self._trees.clear()
        self.lbl_summary.config(text="No change events.")

    def load(self, df: pd.DataFrame):
        self.clear()
        if df is None or df.empty:
            self.lbl_summary.config(text="No change events.")
            return

        df = df.copy()
        # normaliza tipos
        for c in ("run_ts","overview_date"):
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        # delta de dtd_ativo_fin (se existir em old/new json)
        def _delta_dtd_ativo_fin(row):
            try:
                old = json.loads(row.get("old_row_json") or "{}")
                new = json.loads(row.get("new_row_json") or "{}")
                ov = float(old.get("dtd_ativo_fin") or 0.0)
                nv = float(new.get("dtd_ativo_fin") or 0.0)
                return round(nv - ov, 2)
            except Exception:
                return None
        if "old_row_json" in df.columns and "new_row_json" in df.columns:
            df["delta_dtd_ativo_fin"] = df.apply(_delta_dtd_ativo_fin, axis=1)
        else:
            df["delta_dtd_ativo_fin"] = None

        # foca no último run
        if "run_ts" in df.columns:
            last_ts = df["run_ts"].max()
            df = df[df["run_ts"] == last_ts].copy()

        # resumo
        total = len(df)
        by_type = df["change_type"].value_counts().to_dict()
        ts_txt = (df["run_ts"].iloc[0].strftime("%Y-%m-%d %H:%M:%S") if "run_ts" in df.columns and not df.empty else "-")
        self.lbl_summary.config(text=f"{total} mudanças no último run ({ts_txt}) | " +
                                    ", ".join(f"{k}:{v}" for k,v in by_type.items()))

        # tabs por entidade
        for entity, dfe in df.groupby("entity", sort=True):
            frm = tk.Frame(self.nb)
            self.nb.add(frm, text=entity)

            cols = [c for c in self.COLS_BASE if c in (set(self.COLS_BASE)|set(dfe.columns))]
            tree = ttk.Treeview(frm, columns=cols, show="headings")
            vsb = ttk.Scrollbar(frm, orient="vertical", command=tree.yview)
            hsb = ttk.Scrollbar(frm, orient="horizontal", command=tree.xview)
            tree.configure(yscroll=vsb.set, xscroll=hsb.set)

            headers = {
                "run_ts":"Run TS", "change_type":"Type", "overview_date":"Date",
                "portfolio_id":"Portfolio", "instrument_name":"Instrument",
                "changed_cols":"Changed Cols", "delta_dtd_ativo_fin":"Δ dtd_ativo_fin"
            }
            for c in cols:
                tree.heading(c, text=headers.get(c, c), command=lambda col=c, t=tree: self._sort_by(t, col))
                width = 140
                if c in ("instrument_name","changed_cols"): width = 220
                if c in ("delta_dtd_ativo_fin","portfolio_id"): width = 120
                tree.column(c, width=width, anchor="center")

            # duplo clique abre detalhe old/new
            tree.bind("<Double-1>", lambda e, t=tree, d=dfe: self._open_details(t, d))

            # ordena por change_type priorizando UPDATE
            order_map = {"UPDATE":0, "INSERT":1, "DELETE_SOFT":2}
            dfe["_ord"] = dfe["change_type"].map(order_map).fillna(9)
            dfe = dfe.sort_values(["_ord","overview_date","instrument_name"], kind="mergesort")

            for _, r in dfe.iterrows():
                vals = []
                for c in cols:
                    v = r.get(c)
                    if c == "run_ts" and pd.notna(v): v = v.strftime("%Y-%m-%d %H:%M:%S")
                    if c == "overview_date" and pd.notna(v): v = pd.to_datetime(v).strftime("%Y-%m-%d")
                    if c == "delta_dtd_ativo_fin" and v is not None: v = f"{v:,.2f}"
                    vals.append("" if v is None else str(v))
                tags = ("even",) if (len(tree.get_children("")) % 2 == 0) else ("odd",)
                tree.insert("", "end", values=tuple(vals), tags=tags)

            tree.tag_configure("even", background="#f7f7f7")
            tree.tag_configure("odd",  background="#ffffff")

            tree.grid(row=0, column=0, sticky="nsew")
            vsb.grid(row=0, column=1, sticky="ns")
            hsb.grid(row=1, column=0, sticky="ew")
            frm.grid_rowconfigure(0, weight=1)
            frm.grid_columnconfigure(0, weight=1)

            self._trees[entity] = tree
            self._dfs_by_tab[entity] = dfe.drop(columns=["_ord"], errors="ignore").reset_index(drop=True)

    def refresh_from_disk(self):
        try:
            df = read_csv_safe(DIFF_LOG_CSV)
            self.load(df)
        except Exception as e:
            messagebox.showwarning("Change Events", f"Erro ao ler {DIFF_LOG_CSV}:\n{e}")

    def _current_tab(self):
        tab_id = self.nb.select()
        if not tab_id: return None, None, None
        text = self.nb.tab(tab_id, "text")
        return tab_id, text, self._trees.get(text)

    def _copy_selected(self):
        _, ent, tree = self._current_tab()
        if not tree: return
        rows = ["\t".join(tree.item(i, "values")) for i in tree.selection()]
        if rows:
            self.clipboard_clear()
            self.clipboard_append("\n".join(rows))

    def _export_csv(self):
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV","*.csv")], initialfile="change_events_last_run.csv")
        if not path: return
        _, ent, _ = self._current_tab()
        if ent and self._dfs_by_tab.get(ent) is not None:
            export_all = messagebox.askyesno("Export CSV", "Export ALL entities? (No = only current tab)")
            df = pd.concat(self._dfs_by_tab.values(), ignore_index=True) if export_all else self._dfs_by_tab[ent]
        else:
            df = pd.concat(self._dfs_by_tab.values(), ignore_index=True) if self._dfs_by_tab else pd.DataFrame()
        df.to_csv(path, index=False)
        messagebox.showinfo("Export CSV", f"Saved:\n{path}")

    def _sort_by(self, tree: ttk.Treeview, col: str):
        data = [(tree.set(k, col), k) for k in tree.get_children("")]
        def _to_key(v):
            for caster in (float, pd.to_datetime):
                try: return caster(v)
                except Exception: pass
            return v
        data.sort(key=lambda x: _to_key(x[0]))
        heading = tree.heading(col, "text")
        desc = heading.endswith(" ↓")
        tree.heading(col, text=heading.split(" ")[0] + (" ↑" if desc else " ↓"))
        if desc: data.reverse()
        for idx, (_, k) in enumerate(data):
            tree.move(k, "", idx)

    def _open_details(self, tree: ttk.Treeview, df_tab: pd.DataFrame):
        sel = tree.selection()
        if not sel: return
        vals = tree.item(sel[0], "values")
        # acha a linha correspondente pelo timestamp e instrument/date
        try:
            cols = [c for c in self.COLS_BASE if c in df_tab.columns]
            row_dict = dict(zip(cols, vals))
            # pega jsons pelo índice selecionado (mais simples/robusto)
            idx = tree.index(sel[0])
            rec = df_tab.iloc[idx]
            old_js = json.dumps(json.loads(rec.get("old_row_json") or "{}"), indent=2, ensure_ascii=False)
            new_js = json.dumps(json.loads(rec.get("new_row_json") or "{}"), indent=2, ensure_ascii=False)
        except Exception:
            old_js, new_js = "{}", "{}"

        win = tk.Toplevel(self)
        win.title("Change details")
        win.geometry("760x520")
        pan = ttk.Panedwindow(win, orient="horizontal")
        pan.pack(fill="both", expand=True, padx=8, pady=8)

        txt_old = scrolledtext.ScrolledText(pan, wrap="word", font=("Consolas", 10))
        txt_new = scrolledtext.ScrolledText(pan, wrap="word", font=("Consolas", 10))
        txt_old.insert("1.0", old_js); txt_new.insert("1.0", new_js)
        txt_old.configure(state="disabled"); txt_new.configure(state="disabled")
        pan.add(txt_old, weight=1); pan.add(txt_new, weight=1)


# =========================
# GUI principal
# =========================
def main():
    root = tk.Tk()
    root.title("Perenne – ETL & Feed Prices")
    root.geometry("1080x760")

    nb = ttk.Notebook(root)
    nb.pack(fill="both", expand=True)

    # ---------- Aba ETL ----------
    tab_etl = ttk.Frame(nb)
    nb.add(tab_etl, text="ETL")

    frm_etl = tk.Frame(tab_etl, padx=10, pady=10)
    frm_etl.pack(fill="both", expand=True)

    tk.Label(frm_etl, text="Data inicial (ETL):").grid(row=0, column=0, sticky="w")
    ent_data_etl = DateEntry(
        frm_etl, width=12, background="darkblue", foreground="white",
        borderwidth=2, date_pattern="dd-mm-yyyy"
    )
    ent_data_etl.grid(row=0, column=1, padx=10)
    ent_data_etl.set_date(datetime.strptime("01-06-2025", "%d-%m-%Y"))

    txt_log_etl = scrolledtext.ScrolledText(frm_etl, height=28)
    txt_log_etl.grid(row=2, column=0, columnspan=5, sticky="nsew", pady=(10,0))

    sep = ttk.Separator(frm_etl, orient="horizontal")
    sep.grid(row=3, column=0, columnspan=5, sticky="ew", pady=(6,6))

    changes = ChangeEventsPreview(frm_etl)
    changes.grid(row=4, column=0, columnspan=5, sticky="nsew")

    def _refresh_changes_last_run():
        try:
            if not DIFF_LOG_CSV.exists():
                changes.load(pd.DataFrame())
                return
            df = pd.read_csv(DIFF_LOG_CSV, engine="python", on_bad_lines="skip")
            if df.empty:
                changes.load(pd.DataFrame()); return
            df["run_ts"] = pd.to_datetime(df["run_ts"], errors="coerce")
            last_ts = df["run_ts"].max()
            changes.load(df[df["run_ts"] == last_ts].copy())
        except Exception as e:
            _gui_log(txt_log_etl, f"[WARN] Não consegui carregar change_events: {e}")


    frm_etl.grid_rowconfigure(2, weight=1)  # log
    frm_etl.grid_rowconfigure(4, weight=1)  # change table
    for c in range(5):
        frm_etl.grid_columnconfigure(c, weight=1)

    def on_process_etl():
        try:
            d_user = ent_data_etl.get_date()
            ts_start = pd.Timestamp(d_user)

            do_update_beta = messagebox.askyesno(
                "Atualizar curva do Beta?",
                "Deseja atualizar agora a curva do Beta a partir do Excel?"
            )

            btn_etl["state"] = "disabled"
            _gui_log(txt_log_etl, f"Iniciando ETL a partir de {ts_start.strftime('%Y-%m-%d')}...\n")

            def worker():
                try:
                    if do_update_beta:
                        try:
                            update_beta_curve(logfn=lambda m: _gui_log(txt_log_etl, m))
                        except Exception as e:
                            _gui_log(txt_log_etl, f"Aviso ao atualizar Beta: {e}")

                    run_etl_with_start_date(ts_start, logfn=lambda m: _gui_log(txt_log_etl, m))
                    txt_log_etl.after(0, _refresh_changes_last_run)   # atualiza a tabela com o último run
                    messagebox.showinfo("ETL", "Processamento finalizado com sucesso.")
                except Exception as e:
                    err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    _gui_log(txt_log_etl, "✖ ERRO NO ETL:")
                    _gui_log(txt_log_etl, err)
                    messagebox.showerror("ETL", f"Falha no processamento:\n{e}")
                finally:
                    btn_etl["state"] = "normal"

            threading.Thread(target=worker, daemon=True).start()
        except Exception as e:
            messagebox.showerror("Entrada inválida", str(e))

    btn_etl = tk.Button(frm_etl, text="Processar ETL", command=on_process_etl)
    btn_etl.grid(row=0, column=2, padx=8)

    frm_etl.grid_rowconfigure(2, weight=1)
    for c in range(5):
        frm_etl.grid_columnconfigure(c, weight=1)

    # ---------- Aba Feed Prices ----------
    tab_feed = ttk.Frame(nb)
    nb.add(tab_feed, text="Feed Prices")

    frm_feed = tk.Frame(tab_feed, padx=10, pady=10)
    frm_feed.pack(fill="both", expand=True)

    tk.Label(frm_feed, text="Buscar e-mails a partir de:").grid(row=0, column=0, sticky="w")
    ent_data_feed = DateEntry(
        frm_feed, width=12, background="darkblue", foreground="white",
        borderwidth=2, date_pattern="dd-mm-yyyy"
    )
    ent_data_feed.grid(row=0, column=1, padx=10)
    ent_data_feed.set_date(datetime.today())

    btn_fetch = tk.Button(frm_feed, text="Buscar preços", width=14)
    btn_upload = tk.Button(frm_feed, text="Subir para Bluedeck", width=18, state="disabled")

    btn_fetch.grid(row=0, column=2, padx=8)
    btn_upload.grid(row=0, column=3, padx=8)

    txt_log_feed = scrolledtext.ScrolledText(frm_feed, height=14)
    txt_log_feed.grid(row=2, column=0, columnspan=6, sticky="nsew", pady=(10,0))

    # preview abaixo do log
    preview = PricesPreview(frm_feed)
    preview.grid(row=3, column=0, columnspan=6, sticky="nsew", pady=(6,0))

    frm_feed.grid_rowconfigure(2, weight=1)
    frm_feed.grid_rowconfigure(3, weight=1)
    for c in range(6):
        frm_feed.grid_columnconfigure(c, weight=1)

    # estado interno da aba Feed
    feed_state = {"prices": [], "df": pd.DataFrame()}

    def fetch_prices_worker(min_date: datetime):
        try:
            # COM precisa ser inicializado neste thread
            pythoncom.CoInitializeEx(pythoncom.COINIT_APARTMENTTHREADED)
            _gui_log(txt_log_feed, f"Iniciando busca no Outlook a partir de {min_date.strftime('%d/%m/%Y')}...")

            outlook = gencache.EnsureDispatch("Outlook.Application").GetNamespace("MAPI")

            prices = []
            # fundos normais
            for i in range(outlook.Folders.Count):
                root_folder = outlook.Folders.Item(i+1)
                res = read_all_emails(root_folder, subject_filters, min_date)
                prices.extend(res)
            # ELEVA via PDF
            eleva_id = fundos_ids["ELEVA EDUCACAO I FIP MULTI"]
            eleva_subject = "[BTG Pactual] - Relatório de Cotas - ELEVA EDUCAÇAO I FIP MULTI"
            prices += buscar_cotas_eleva(outlook, min_date, eleva_subject, eleva_id)

            if not prices:
                _gui_log(txt_log_feed, "Nenhum preço encontrado no período.")
                return

            # Padroniza datas
            for p in prices:
                p["date"] = padronizar_data(p["date"])

            id_para_nome = {v: k for k, v in fundos_ids.items()}
            df_prices = pd.DataFrame(prices)
            df_prices["fundo"] = df_prices["instrument_id"].map(id_para_nome)
            df_prices = df_prices[["date", "fundo", "instrument_id", "price"]]
            df_prices = df_prices.sort_values(["fundo","date","instrument_id"]).reset_index(drop=True)

            feed_state["prices"] = prices
            feed_state["df"] = df_prices

            _gui_log(txt_log_feed, "\nTabela que será enviada para o sistema:")
            _gui_log(txt_log_feed, df_prices.to_string(index=False))
            _gui_log(txt_log_feed, f"\nTotal de linhas: {len(df_prices)}")

            # render no main thread
            txt_log_feed.after(0, lambda df=df_prices: preview.load(df))
            btn_upload["state"] = "normal"

        except Exception as e:
            err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            _gui_log(txt_log_feed, "✖ ERRO NA BUSCA DE PREÇOS:")
            _gui_log(txt_log_feed, err)
        finally:
            try:
                pythoncom.CoUninitialize()
            except Exception:
                pass
            btn_fetch["state"] = "normal"

    def on_fetch_prices():
        try:
            min_date = ent_data_feed.get_date()
            btn_fetch["state"] = "disabled"
            btn_upload["state"] = "disabled"
            txt_log_feed.delete("1.0", tk.END)
            preview.clear()
            threading.Thread(
                target=fetch_prices_worker,
                args=(datetime.combine(min_date, datetime.min.time()),),
                daemon=True
            ).start()
        except Exception as e:
            messagebox.showerror("Entrada inválida", str(e))

    def on_upload_prices():
        try:
            if not feed_state["prices"]:
                messagebox.showwarning("Feed Prices", "Nenhum dado para enviar.")
                return

            df_preview = feed_state["df"]
            if df_preview is not None and not df_preview.empty:
                _gui_log(txt_log_feed, "\nResumo do envio:")
                _gui_log(txt_log_feed, df_preview.tail(10).to_string(index=False))

            if not messagebox.askyesno("Confirmar envio", f"Enviar {len(feed_state['prices'])} registros para o Bluedeck?"):
                _gui_log(txt_log_feed, "Envio cancelado pelo usuário.")
                return

            btn_upload["state"] = "disabled"

            def worker_upload():
                try:
                    status, resposta = criar_precos_ativos(feed_state["prices"], PRICE_SOURCE_ID, logfn=lambda m: _gui_log(txt_log_feed, m))
                    if status == 200:
                        _gui_log(txt_log_feed, "✔ Envio concluído com sucesso!")
                        messagebox.showinfo("Feed Prices", "Success!")
                    else:
                        _gui_log(txt_log_feed, f"⚠ Retorno inesperado: status={status} body={resposta}")
                        messagebox.showwarning("Feed Prices", f"Retorno inesperado: {status}")
                except Exception as e:
                    err = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    _gui_log(txt_log_feed, "✖ ERRO NO ENVIO:")
                    _gui_log(txt_log_feed, err)
                    messagebox.showerror("Feed Prices", f"Falha no envio:\n{e}")
                finally:
                    btn_upload["state"] = "normal"

            threading.Thread(target=worker_upload, daemon=True).start()

        except Exception as e:
            messagebox.showerror("Feed Prices", str(e))

    btn_fetch.configure(command=on_fetch_prices)
    btn_upload.configure(command=on_upload_prices)

    root.mainloop()


if __name__ == "__main__":
    main()
