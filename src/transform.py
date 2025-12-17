import pandas as pd
import numpy as np

"""
Módulo de transformaciones del ETL.

Contiene funciones para transformar dimensiones y tablas de hechos
siguiendo un modelo dimensional (estrella).
"""



# =========================================================
# DimDate
# =========================================================

def transform_dim_date(start_date='2005-01-01', end_date='2014-12-31'):

    """
    Genera la dimensión tiempo (DimDate).

    Args:
        start_date (str): Fecha inicial (YYYY-MM-DD).
        end_date (str): Fecha final (YYYY-MM-DD).

    Returns:
        pd.DataFrame: DataFrame con la dimensión fecha."""
        
    print("Generando DimDate...")

    df = pd.DataFrame({
        "FullDate": pd.date_range(start_date, end_date)
    })

    df["DateKey"] = df["FullDate"].dt.strftime("%Y%m%d").astype(int)
    df["Day"] = df["FullDate"].dt.day
    df["Month"] = df["FullDate"].dt.month
    df["Year"] = df["FullDate"].dt.year
    df["MonthName"] = df["FullDate"].dt.month_name()
    df["DayName"] = df["FullDate"].dt.day_name()

    return df


# =========================================================
# DimProduct
# =========================================================

def transform_dim_product(df):
    print("Transformando DimProduct...")

    df = df.copy()
    df.insert(0, "ProductKey", range(1, len(df) + 1))

    return df


# =========================================================
# DimCustomer
# =========================================================

def transform_dim_customer(df):
    print("Transformando DimCustomer...")

    df = df.copy()
    df.insert(0, "CustomerKey", range(1, len(df) + 1))

    return df


# =========================================================
# DimSalesTerritory
# =========================================================

def transform_dim_sales_territory(df):
    print("Transformando DimSalesTerritory...")

    df = df.copy()
    df.insert(0, "SalesTerritoryKey", range(1, len(df) + 1))

    return df


# =========================================================
# DimCurrency
# =========================================================

def transform_dim_currency(df):
    print("Transformando DimCurrency...")

    df = df.copy()
    df.insert(0, "CurrencyKey", range(1, len(df) + 1))

    return df


# =========================================================
# DimPromotion
# =========================================================

def transform_dim_promotion(df):
    print("Transformando DimPromotion...")

    df = df.copy()
    df.insert(0, "PromotionKey", range(1, len(df) + 1))

    return df


# =========================================================
# FactInternetSales
# =========================================================

def transform_fact_internet_sales(df):
    print("Transformando FactInternetSales...")

    df = df.copy()

    df["OrderDateKey"] = pd.to_datetime(df["orderdate"]).dt.strftime("%Y%m%d").astype(int)
    df["ShipDateKey"] = pd.to_datetime(df["shipdate"]).dt.strftime("%Y%m%d").astype(int)
    df["DueDateKey"] = pd.to_datetime(df["duedate"]).dt.strftime("%Y%m%d").astype(int)

    df.drop(columns=["orderdate", "shipdate", "duedate"], inplace=True)

    return df


# =========================================================
# FactResellerSales
# =========================================================

def transform_fact_reseller_sales(df):
    print("Transformando FactResellerSales...")

    df = df.copy()

    df["OrderDateKey"] = pd.to_datetime(df["orderdate"]).dt.strftime("%Y%m%d").astype(int)
    df["ShipDateKey"] = pd.to_datetime(df["shipdate"]).dt.strftime("%Y%m%d").astype(int)
    df["DueDateKey"] = pd.to_datetime(df["duedate"]).dt.strftime("%Y%m%d").astype(int)

    df.drop(columns=["orderdate", "shipdate", "duedate"], inplace=True)

    return df
