import argparse
import os
import sys
from datetime import date, timedelta

import requests
import pandas as pd

# Códigos SGS (PTAX venda) — conforme instrução do desafio
SGS_SERIES = {
    "USD": 10813,
    "EUR": 21619,
    "GBP": 21623,
    "JPY": 21621,
    "ARS": 3549,
    "CHF": 21625,
}

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"


def ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def fetch_sgs(codigo: int, start: date, end: date, timeout: int = 20) -> list[dict]:
    url = BASE_URL.format(codigo=codigo)
    params = {"formato": "json", "dataInicial": ddmmyyyy(start), "dataFinal": ddmmyyyy(end)}

    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            raise ValueError("Resposta inesperada: JSON não é lista.")
        return data
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Erro de conexão/HTTP ao consultar série {codigo}: {e}") from e


def parse_args():
    p = argparse.ArgumentParser(description="Coleta cotações (SGS/BCB) e exporta CSV para Power BI.")
    p.add_argument("--moedas", default="USD,EUR,GBP,JPY",
                   help="Moedas separadas por vírgula. Ex: USD,EUR,GBP,JPY")
    p.add_argument("--days", type=int, default=None, help="Período em dias (7/30/90).")
    p.add_argument("--years", type=int, default=2, help="Período em anos (padrão=2).")
    p.add_argument("--out", default="data/cotacoes.csv", help="Saída CSV (padrão=data/cotacoes.csv).")
    p.add_argument("--excel", action="store_true", help="Também exporta Excel.")
    return p.parse_args()


def main():
    args = parse_args()

    moedas = [m.strip().upper() for m in args.moedas.split(",") if m.strip()]
    invalidas = [m for m in moedas if m not in SGS_SERIES]
    if invalidas:
        print(f"Moedas inválidas: {invalidas}. Válidas: {list(SGS_SERIES.keys())}")
        sys.exit(1)

    end = date.today()
    if args.days is not None:
        start = end - timedelta(days=args.days)
    else:
        start = end - timedelta(days=365 * args.years)  # simples e suficiente pro desafio

    rows = []
    for moeda in moedas:
        codigo = SGS_SERIES[moeda]
        data = fetch_sgs(codigo, start, end)

        for item in data:
            dt = item.get("data")
            val = item.get("valor")
            if dt is None or val is None:
                continue

            # valor vem como string
            val = str(val).replace(",", ".")
            try:
                val_f = float(val)
            except ValueError:
                continue

            rows.append({"date": dt, "currency": moeda, "value_brl": val_f})

    if not rows:
        print("Nenhum dado retornado. Verifique período/moedas.")
        sys.exit(2)

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values(["currency", "date"]).reset_index(drop=True)

    # útil para cards/indicadores no Power BI
    df["prev_value"] = df.groupby("currency")["value_brl"].shift(1)
    df["pct_change_day"] = (df["value_brl"] / df["prev_value"] - 1) * 100

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False, encoding="utf-8")

    if args.excel:
        xlsx_path = args.out.replace(".csv", ".xlsx")
        df.to_excel(xlsx_path, index=False)

    print(f"OK! Linhas: {len(df)} | Arquivo: {args.out}")


if __name__ == "__main__":
    main()
