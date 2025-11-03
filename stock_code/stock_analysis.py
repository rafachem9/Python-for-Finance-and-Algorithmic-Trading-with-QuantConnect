#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script de Análisis de Acciones convertido desde un Jupyter Notebook.

Este script descarga datos históricos de acciones del IBEX 35, S&P 500 e Índices/ETFs
usando la API de yfinance. Realiza análisis de rentabilidad, volatilidad,
ratios (Sharpe, P/E, P/B), y calcula Alpha y Beta.

Los resultados del análisis se guardan e
n archivos CSV en el directorio 'data'.
También genera y muestra gráficos de volatilidad y rentabilidad acumulada.
"""

import matplotlib.pyplot as plt
import pandas as pd
import os

from etl.functions import call_yf_api_historic, extraction_historic, analysis_stock_hist, save_extraction_historic_parquet, get_total_rank
from etl.variables import ibex35_tickers, tickers_sp500, start_period, end_period, DATA_DIR, tickers_index

# Configuración de Pandas
pd.set_option('display.max_columns', None)


def main():
    """
    Función principal para ejecutar los análisis.
    """

    # --- ANÁLISIS IBEX 35 ---
    print("Iniciando análisis del IBEX 35...")

    bechmark_ibex35 = call_yf_api_historic(start_period, end_period, '^IBEX')
    ibex35_df = extraction_historic(start_period, end_period, ibex35_tickers)

    print("\nGuardando datos históricos del IBEX 35 en Parquet...")
    SUBFOLDER_DIR_IBEX = os.path.join(DATA_DIR, 'ibex35_historic')
    save_extraction_historic_parquet(ibex35_df, SUBFOLDER_DIR_IBEX)

    print("\nRealizando análisis del IBEX 35...")
    ibex35_analysed_df = analysis_stock_hist(ibex35_df, ibex35_tickers, bechmark_ibex35)

    print("Calculando rankings para IBEX 35...")
    ibex35_analysed_df = get_total_rank(ibex35_analysed_df, 'rank_per', [80, 40, 20])
    ibex35_analysed_df = get_total_rank(ibex35_analysed_df, 'rank_dividend', [30, 70, 30])

    ibex35_analysed_df = ibex35_analysed_df.sort_values(by="rank_per", ascending=False)
    ibex35_filename = os.path.join(DATA_DIR, 'ibex35_analysed_df.csv')
    ibex35_analysed_df.to_csv(ibex35_filename, index=False)

    print(f"\n--- Resultados Top 30 IBEX 35 (por rank_per) ---")
    print(ibex35_analysed_df.head(30))
    print(f"Análisis del IBEX 35 guardado en: {ibex35_filename}")

    # --- ANÁLISIS S&P 500 ---
    print("\nIniciando análisis del S&P 500...")

    bechmark_sp500 = call_yf_api_historic(start_period, end_period, '^GSPC')
    sp500_df = extraction_historic(start_period, end_period, tickers_sp500)

    print("\nGuardando datos históricos del S&P 500 en Parquet...")
    SUBFOLDER_DIR_SP500 = os.path.join(DATA_DIR, 'sp500_historic')
    save_extraction_historic_parquet(sp500_df, SUBFOLDER_DIR_SP500)

    print("\nRealizando análisis del S&P 500...")
    sp500_analysed_df = analysis_stock_hist(sp500_df, tickers_sp500, bechmark_sp500)

    print("Calculando rankings para S&P 500...")
    sp500_analysed_df = get_total_rank(sp500_analysed_df, 'rank_per', [80, 40, 20])
    sp500_analysed_df = get_total_rank(sp500_analysed_df, 'rank_dividend', [30, 70, 30])

    sp500_analysed_df = sp500_analysed_df.sort_values(by="rank_per", ascending=False)
    sp500_filename = os.path.join(DATA_DIR, 'sp500_analysed_df.csv')
    sp500_analysed_df.to_csv(sp500_filename, index=False)

    print(f"\n--- Resultados Top 50 S&P 500 (por rank_per) ---")
    print(sp500_analysed_df.head(50))
    print(f"Análisis del S&P 500 guardado en: {sp500_filename}")

    # --- GRÁFICO VOLATILIDAD S&P 500 ---
    print("\nGenerando gráfico de volatilidad S&P 500...")
    volatilities_sp500 = {
        ticker: data["Daily Return"].std() for ticker, data in sp500_df.items() if not data.empty
    }
    vol_df_sp500 = pd.DataFrame.from_dict(volatilities_sp500, orient='index', columns=["Volatilidad"])
    vol_df_sp500.sort_values("Volatilidad", ascending=False).plot(kind='bar', figsize=(12, 6), legend=False)

    plt.title("Volatilidad Diaria (Std Dev de Daily Return) - S&P 500 2025")
    plt.ylabel("Volatilidad")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # --- ANÁLISIS ÍNDICES/ETFS ---
    print("\nIniciando análisis de Índices/ETFs...")

    bechmark_index_sp500 = call_yf_api_historic(start_period, end_period, '^GSPC')
    index_hist_df = extraction_historic(start_period, end_period, tickers_index)

    # El análisis estaba comentado en el notebook original, se mantiene así.
    # print("\nRealizando análisis de Índices/ETFs...")
    # index_analysed_df = analysis_stock_hist(index_hist_df, tickers_index, bechmark_index_sp500)
    # print(index_analysed_df)

    # --- GRÁFICOS ÍNDICES/ETFS ---
    print("\nGenerando gráficos de Índices/ETFs...")
    plt.figure(figsize=(14, 7))
    for ticker, data in index_hist_df.items():
        if not data.empty:
            plt.plot(data.index, data["Cumulative Return"].rolling(window=5).mean(), label=ticker)

    plt.title("Rentabilidad Acumulada (Índices/ETFs) - 2025")
    plt.xlabel("Fecha")
    plt.ylabel("Rentabilidad Acumulada")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    volatilities_index = {
        ticker: data["Daily Return"].std() for ticker, data in index_hist_df.items() if not data.empty
    }
    vol_df_index = pd.DataFrame.from_dict(volatilities_index, orient='index', columns=["Volatilidad"])
    vol_df_index.sort_values("Volatilidad", ascending=False).plot(kind='bar', figsize=(12, 6), legend=False)

    plt.title("Volatilidad Diaria (Std Dev de Daily Return) - Índices/ETFs 2025")
    plt.ylabel("Volatilidad")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    print("\n--- Análisis completado ---")


if __name__ == "__main__":
    main()