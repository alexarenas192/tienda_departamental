#!/usr/bin/env python3
"""
etl_tienda.py
Script ETL para el caso de estudio: Tienda Departamental (MegaStore)
Requisitos: pandas

Este script toma 'ventas_tienda_raw.csv', aplica limpieza y genera 'ventas_tienda_clean.csv'.
"""

import pandas as pd
import numpy as np

RAW = "ventas_tienda_raw.csv"
CLEAN = "ventas_tienda_clean.csv"

def parse_dates(x):
    return pd.to_datetime(x, dayfirst=False, errors='coerce')

def main():
    df = pd.read_csv(RAW)
    # mantener copia de auditoría de fechas crudas
    df['sale_date_raw'] = df['sale_date']

    # Normalizar fechas
    df['sale_date'] = df['sale_date'].apply(parse_dates)

    # Convertir unit_price a numérico y forzar positivo
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    # Precio base por producto (ejemplo, ajustar si usa catálogo real)
    precio_base = {
        101: 299.0,102: 499.0,103: 899.0,104: 7499.0,105: 799.0,
        106: 1299.0,107: 449.0,108: 699.0,109: 1599.0,110: 2599.0
    }
    df['unit_price'] = df.apply(lambda r: precio_base.get(r['product_id'], np.nan) if pd.isna(r['unit_price']) else abs(r['unit_price']), axis=1)

    # Limpiar ciudades
    df['customer_city'] = df['customer_city'].astype(str).str.strip().str.title()
    df['customer_city'] = df['customer_city'].replace({'Desconocido': np.nan})

    # Eliminar duplicados por sale_id
    df = df.drop_duplicates(subset=['sale_id'], keep='first')

    # Reemplazar caracteres extraños en nombres de producto
    df['product_name'] = df['product_name'].str.replace('@', 'a', regex=False)

    # Recalcular total_amount
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    df['total_amount_calc'] = (df['unit_price'] * df['quantity']).round(2)
    df['total_amount'] = df['total_amount'].fillna(df['total_amount_calc'])

    # Capear outliers en unit_price (percentil 99)
    cap = df.loc[df['sale_id'] != 9999, 'unit_price'].quantile(0.99)
    df.loc[(df['unit_price'] > cap) & (df['sale_id'] != 9999), 'unit_price'] = cap
    df['total_amount'] = (df['unit_price'] * df['quantity']).round(2)

    # Eliminar registros con datos críticos faltantes
    df_clean = df.dropna(subset=['sale_date', 'product_id', 'quantity']).copy()

    # Reordenar columnas
    final_cols = [
        'sale_id','sale_date','product_id','product_name','category','unit_price','quantity','total_amount',
        'customer_id','customer_name','customer_city','employee_id','store_id','sale_date_raw'
    ]
    df_clean = df_clean[final_cols]

    df_clean.to_csv(CLEAN, index=False, encoding='utf-8')
    print("ETL completado. Archivo generado:", CLEAN)

if __name__ == "__main__":
    main()
