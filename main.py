"""
Script principal de orquestación del proceso ETL.

Este módulo coordina la ejecución completa del flujo ETL:
- Extract: obtención de datos desde la base de datos fuente.
- Transform: aplicación de reglas de negocio y modelado dimensional.
- Load: carga de dimensiones y tablas de hechos en el Data Warehouse.

El proceso sigue un modelo dimensional tipo estrella.
"""

from src.utils import get_db_engine
from src.extract import (
    extract_dim_product,
    extract_dim_customer,
    extract_dim_sales_territory,
    extract_dim_currency,
    extract_dim_promotion,
    extract_fact_internet_sales,
    extract_fact_reseller_sales
)
from src.transform import (
    transform_dim_date,
    transform_dim_product,
    transform_dim_customer,
    transform_dim_sales_territory,
    transform_dim_currency,
    transform_dim_promotion,
    transform_fact_internet_sales,
    transform_fact_reseller_sales
)
from src.load import load_data


def main():
    """
    Ejecuta el proceso ETL completo.

    Establece conexiones a las bases de datos fuente y destino,
    extrae los datos necesarios, aplica las transformaciones
    correspondientes y carga los resultados en el Data Warehouse."""


    print("Iniciando proceso ETL...")

    source_engine = get_db_engine("SOURCE_DB")
    dest_engine = get_db_engine("DESTINATION_DB")

    if source_engine is None or dest_engine is None:
        print("Error en la conexión a las bases de datos.")
        return

    # =========================
    # DIMENSIONES
    # =========================
    print("\n--- Cargando DimDate ---")
    dim_date = transform_dim_date()
    load_data(dim_date, "dim_date", dest_engine, if_exists="replace")

    print("\n--- Cargando DimProduct ---")
    dim_product_raw = extract_dim_product(source_engine)
    dim_product = transform_dim_product(dim_product_raw)
    load_data(dim_product, "dim_product", dest_engine, if_exists="replace")

    print("\n--- Cargando DimCustomer ---")
    dim_customer_raw = extract_dim_customer(source_engine)
    dim_customer = transform_dim_customer(dim_customer_raw)
    load_data(dim_customer, "dim_customer", dest_engine, if_exists="replace")

    print("\n--- Cargando DimSalesTerritory ---")
    dim_territory_raw = extract_dim_sales_territory(source_engine)
    dim_territory = transform_dim_sales_territory(dim_territory_raw)
    load_data(dim_territory, "dim_sales_territory", dest_engine, if_exists="replace")

    print("\n--- Cargando DimCurrency ---")
    dim_currency_raw = extract_dim_currency(source_engine)
    dim_currency = transform_dim_currency(dim_currency_raw)
    load_data(dim_currency, "dim_currency", dest_engine, if_exists="replace")

    print("\n--- Cargando DimPromotion ---")
    dim_promotion_raw = extract_dim_promotion(source_engine)
    dim_promotion = transform_dim_promotion(dim_promotion_raw)
    load_data(dim_promotion, "dim_promotion", dest_engine, if_exists="replace")

    # =========================
    # HECHOS
    # =========================
    print("\n--- Cargando FactInternetSales ---")
    fact_internet_raw = extract_fact_internet_sales(source_engine)
    fact_internet = transform_fact_internet_sales(fact_internet_raw)
    load_data(fact_internet, "fact_internet_sales", dest_engine, if_exists="replace")

    print("\n--- Cargando FactResellerSales ---")
    fact_reseller_raw = extract_fact_reseller_sales(source_engine)
    fact_reseller = transform_fact_reseller_sales(fact_reseller_raw)
    load_data(fact_reseller, "fact_reseller_sales", dest_engine, if_exists="replace")

    print("\n✅ ETL COMPLETADA CON ÉXITO")


if __name__ == "__main__":
    main()
