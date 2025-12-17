import pandas as pd
from src.utils import get_db_engine

"""
Módulo de extracción (Extract) del proceso ETL.

Este script contiene las funciones encargadas de extraer datos desde la
base de datos fuente (PostgreSQL) para alimentar dimensiones y tablas
de hechos del Data Warehouse, siguiendo un modelo dimensional (estrella).
"""


def _execute_query(query, table_name, engine):
    """
Ejecuta una consulta SQL y retorna el resultado como DataFrame.

Args:
    query (str): Consulta SQL a ejecutar.
    table_name (str): Nombre lógico de la tabla/dimensión/hecho.
    engine (sqlalchemy.Engine): Engine de conexión a la base de datos.

Returns:
    pd.DataFrame | None: DataFrame con los datos extraídos o None si falla.
"""

    try:
        print(f"Extrayendo datos de {table_name}...")
        df = pd.read_sql(query, engine)
        print(f"-> Se extrajeron exitosamente {len(df)} filas de {table_name}.")
        return df
    except Exception as e:
        print(f"-> Error al extraer de {table_name}: {e}")
        return None


# ========================
# DIMENSIONES
# ========================



def extract_dim_product(engine):
    """    
    Extrae la dimensión Producto desde la base de datos fuente.

    Args:
        engine (sqlalchemy.Engine): Engine de conexión a la base de datos.

    Returns:
        pd.DataFrame | None: Dimensión Producto."""

 
    
    query = """
        SELECT 
            p.productid AS ProductKey,
            p.name AS ProductName,
            p.productnumber,
            p.color,
            p.standardcost,
            p.listprice,
            pc.name AS Category,
            psc.name AS Subcategory
        FROM production.product p
        LEFT JOIN production.productsubcategory psc
            ON p.productsubcategoryid = psc.productsubcategoryid
        LEFT JOIN production.productcategory pc
            ON psc.productcategoryid = pc.productcategoryid
    """
    return _execute_query(query, "DimProduct", engine)


def extract_dim_customer(engine):
    query = """
        SELECT
            c.customerid AS CustomerKey,
            p.firstname,
            p.lastname,
            c.accountnumber
        FROM sales.customer c
        LEFT JOIN person.person p
            ON c.personid = p.businessentityid
    """
    return _execute_query(query, "DimCustomer", engine)


def extract_dim_sales_territory(engine):
    query = """
        SELECT
            territoryid AS SalesTerritoryKey,
            name AS TerritoryName,
            countryregioncode,
            "group" AS TerritoryGroup
        FROM sales.salesterritory
    """
    return _execute_query(query, "DimSalesTerritory", engine)


def extract_dim_currency(engine):
    query = """
        SELECT
            currencycode AS CurrencyKey,
            name AS CurrencyName
        FROM sales.currency
    """
    return _execute_query(query, "DimCurrency", engine)


def extract_dim_promotion(engine):
    query = """
        SELECT
            specialofferid AS PromotionKey,
            description,
            discountpct,
            type,
            category,
            startdate,
            enddate
        FROM sales.specialoffer
    """
    return _execute_query(query, "DimPromotion", engine)


# ========================
# HECHOS
# ========================

def extract_fact_internet_sales(engine):
    query = """
        SELECT 
            soh.customerid AS CustomerKey,
            sod.productid AS ProductKey,
            soh.territoryid AS SalesTerritoryKey,
            cr.tocurrencycode AS CurrencyKey,
            sod.specialofferid AS PromotionKey,
            soh.orderdate,
            soh.shipdate,
            soh.duedate,
            sod.orderqty,
            sod.unitprice,
            sod.unitpricediscount,
            sod.linetotal,
            soh.taxamt,
            soh.freight,
            soh.totaldue
        FROM sales.salesorderdetail sod
        JOIN sales.salesorderheader soh
            ON sod.salesorderid = soh.salesorderid
        LEFT JOIN sales.currencyrate cr
            ON soh.currencyrateid = cr.currencyrateid
        WHERE soh.onlineorderflag = TRUE
    """
    return _execute_query(query, "FactInternetSales", engine)


def extract_fact_reseller_sales(engine):
    query = """
        SELECT 
            st.businessentityid AS ResellerKey,
            sod.productid AS ProductKey,
            soh.territoryid AS SalesTerritoryKey,
            cr.tocurrencycode AS CurrencyKey,
            sod.specialofferid AS PromotionKey,
            soh.orderdate,
            soh.shipdate,
            soh.duedate,
            sod.orderqty,
            sod.unitprice,
            sod.unitpricediscount,
            sod.linetotal,
            soh.taxamt,
            soh.freight,
            soh.totaldue
        FROM sales.salesorderdetail sod
        JOIN sales.salesorderheader soh
            ON sod.salesorderid = soh.salesorderid
        LEFT JOIN sales.customer c
            ON soh.customerid = c.customerid
        LEFT JOIN sales.store st
            ON c.storeid = st.businessentityid
        LEFT JOIN sales.currencyrate cr
            ON soh.currencyrateid = cr.currencyrateid
        WHERE c.storeid IS NOT NULL
    """
    return _execute_query(query, "FactResellerSales", engine)


# ========================
# PRUEBA LOCAL
# ========================

if __name__ == "__main__":
    engine = get_db_engine("SOURCE_DB")

    df = extract_dim_product(engine)
    if df is not None:
        print(df.head())
