# @author Natália Rabelo, Thais Moylany e Darah Leite
#
# Pré-requisitos: 
# python 3.11  - obtido na microsoft store
# pandas module - digitar linha de comando: pip install pandas
# sqlalchemy module - digitar linha de comando: pip install sqlalchemy
# MySQLdb module - digitar linha de comando: pip install mysql-connector-python
#
# Para compilar e executar: python etl.py
from dateutil.parser import parse

import numpy as np

import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Variáveis globais de configuração do banco de dados
username = 'root'
password = '1234'
ip = 'localhost'
port = '3306'
database = 'star5'

# Função para carregar CSV
def load_csv(file_name):
    try:
        df = pd.read_csv(file_name)
        return df
    except FileNotFoundError:
        print(f"Erro ao carregar o arquivo {file_name}. Arquivo não encontrado.")
        return None
    except Exception as e:
        print(f"Erro ao carregar o arquivo {file_name}. Erro: {e}")
        return None

# Função para conectar ao banco de dados
def create_connection():
    try:
        engine = create_engine('mysql+mysqlconnector://'+ username +':' + password + '@'+ ip +':'+ port +'/' + database, echo=False)
        return engine
    except SQLAlchemyError as e:
        print(f"Erro ao conectar ao banco de dados. Erro: {e}")
        return None

# Função para carregar os dados para o esquema star
def load_to_star_schema(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(f"Erro ao carregar dados para a tabela {table_name}. Erro: {e}")
    except Exception as e:
        print(f"Erro inesperado ao carregar dados para a tabela {table_name}. Erro: {e}")

# Função para obter id das dimensões
def get_dimension_ids(sales_date, customer_id, promo_id, product_id, sales_rep_id, engine):
    # Parâmetros para consulta
    params = {
        'sales_date': sales_date,
        'customer_id': customer_id,
        'promo_id': promo_id,
        'product_id': product_id,
        'sales_rep_id': sales_rep_id
    }
    
    # Queries individuais para cada dimensão
    queries = {
        'product_dim_id': "SELECT product_dim_id FROM dimension_product WHERE product_id = :product_id",
        'promotion_dim_id': "SELECT promotion_dim_id FROM dimension_promotion WHERE promo_id = :promo_id",
        'customer_dim_id': "SELECT customer_dim_id FROM dimension_customer WHERE customer_id = :customer_id",
        'sales_rep_dim_id': "SELECT sales_rep_dim_id FROM dimension_sales_rep WHERE employee_id = :sales_rep_id", 
        'date_dim_id': "SELECT date_dim_id FROM dimension_date WHERE sales_date = :sales_date",
    }

    result = {}

    # Executa as consultas individuais e armazena os resultados
    for dim_id, query in queries.items():
        query_result = pd.read_sql_query(text(query), engine, params=params)
        if not query_result.empty:
            result[dim_id] = query_result.iloc[0, 0]
        else:
            result[dim_id] = None 

    return result

# Função para carregar os dados da fact_sales
def load_fact_sales(engine):
    try:
        orders_export = load_csv('orders_export.csv')
        Orders_Store_NYC = load_csv('Orders_Store_NYC.csv')
        orders = pd.concat([orders_export, Orders_Store_NYC]).reset_index(drop=True)
        orders = orders[['CUSTOMER_ID', 'ORDER_TOTAL', 'PROMO_ID', 'PRODUCT_ID', 'QUANTITY', 'ORDER_DATE', 'SALES_REP_ID']].rename(columns={
            'CUSTOMER_ID': 'customer_id', 'ORDER_TOTAL': 'dollars_sold',
            'PROMO_ID': 'promo_id', 'PRODUCT_ID': 'product_id', 'QUANTITY': 'quantity_sold', 'ORDER_DATE' : 'sales_date',
            'SALES_REP_ID' : 'sales_rep_id'
        })

        orders['sales_date'] = orders['sales_date'].apply(lambda x: parse(x).strftime('%Y-%m-%d'))

        orders = orders.reset_index(drop=True)
        
        dim_ids = orders.apply(lambda row: get_dimension_ids(row['sales_date'], row['customer_id'], row['promo_id'], row['product_id'], row['sales_rep_id'], engine), axis=1)
        orders_with_dim_ids = pd.DataFrame(dim_ids.tolist())
        orders_with_dim_ids['dollars_sold'] = orders['dollars_sold']
        orders_with_dim_ids['quantity_sold'] = orders['quantity_sold']

        orders_with_dim_ids = orders_with_dim_ids.dropna()

        load_to_star_schema(orders_with_dim_ids, 'fact_sales', engine)
        print('carreguei fact_sales')

    except Exception as e:
        print(f"Erro ao carregar dados para a tabela fact_sales: {e}")


# Função para carregar os dados da dimension_customer
def load_customers(engine):
    customers_export = load_csv('customers_export.csv')
    Customers_Store_NYC = load_csv('Customers_Store_NYC.csv')
    customers = pd.concat([customers_export, Customers_Store_NYC]).reset_index(drop=True)
    customers = customers[['CUSTOMER_ID', 'CUST_FIRST_NAME', 'CUST_LAST_NAME', 'STREET_ADDRESS',
                            'POSTAL_CODE', 'CITY', 'STATE_PROVINCE', 'COUNTRY_ID', 'COUNTRY_NAME',
                            'REGION_ID', 'NLS_LANGUAGE', 'NLS_TERRITORY', 'CREDIT_LIMIT', 'CUST_EMAIL',
                            'PRIMARY_PHONE_NUMBER', 'ACCOUNT_MGR_ID', 'LOCATION_GTYPE', 'LOCATION_SRID',
                            'LOCATION_X', 'LOCATION_Y']].rename(columns={
            'CUSTOMER_ID': 'customer_id', 'CUST_FIRST_NAME': 'cust_first_name', 'CUST_LAST_NAME': 'cust_last_name',
            'STREET_ADDRESS': 'street_add', 'POSTAL_CODE': 'postal_code', 'CITY': 'city', 'STATE_PROVINCE': 'state',
            'COUNTRY_ID': 'country_id', 'COUNTRY_NAME': 'country_name', 'REGION_ID': 'region_id',
            'NLS_LANGUAGE': 'nls_language', 'NLS_TERRITORY': 'nls_territory', 'CREDIT_LIMIT': 'credit_limit',
            'CUST_EMAIL': 'cust_email', 'PRIMARY_PHONE_NUMBER': 'phone', 'ACCOUNT_MGR_ID': 'account_mgr_id',
            'LOCATION_GTYPE': 'location_type', 'LOCATION_SRID': 'location_grid', 'LOCATION_X': 'location_x',
            'LOCATION_Y': 'location_y'
    })

    customers = customers.dropna()

    load_to_star_schema(customers, 'dimension_customer', engine)
    print('carreguei dimension_customer')

# Função para carregar os dados da dimension_product
def load_products(engine):
    products_export = load_csv('products_export.csv')
    products_export = products_export[['PRODUCT_ID', 'PRODUCT_NAME', 'MIN_PRICE', 'LIST_PRICE', 'PRODUCT_STATUS',
                                        'SUPPLIER_ID', 'WARRANTY_PERIOD', 'WEIGHT_CLASS', 'PRODUCT_DESCRIPTION',
                                        'CATEGORY_ID', 'CATALOG_URL', 'PARENT_CATEGORY_ID', 'SUB_CATEGORY_NAME', 'SUB_CATEGORY_DESCRIPTION', 'CATEGORY_NAME']].rename(columns={
            'PRODUCT_ID': 'product_id', 'PRODUCT_NAME': 'product_name', 'MIN_PRICE': 'min_price',
            'LIST_PRICE': 'list_price', 'PRODUCT_STATUS': 'product_status', 'SUPPLIER_ID': 'supplier_id',
            'WARRANTY_PERIOD': 'warranty_period', 'WEIGHT_CLASS': 'weight_class', 'PRODUCT_DESCRIPTION': 'product_description',
            'CATEGORY_ID': 'category_id', 'CATALOG_URL': 'catalog_url', 'PARENT_CATEGORY_ID': 'parent_category_id',
            'CATEGORY_NAME': 'category_name', 'SUB_CATEGORY_NAME': 'parent_category_name', 'SUB_CATEGORY_DESCRIPTION' : 'parent_category_description', 'CATEGORY_NAME' : 'category_name'
        })

    products_export = products_export.dropna()

    load_to_star_schema(products_export, 'dimension_product', engine)
    print('carreguei dimension_product')

# Função para carregar os dados da dimension_promotion
def load_promotions(engine):
    promotions_export = load_csv('promotions_export.csv')
    promotions_export = promotions_export[['PROMO_ID', 'PROMO_NAME']].rename(columns={
            'PROMO_ID': 'promo_id', 'PROMO_NAME': 'promo_name'
    })

    promotions_export = promotions_export.dropna()

    load_to_star_schema(promotions_export, 'dimension_promotion', engine)
    print('carreguei dimension_promotion')

# Função para carregar os dados da dimension_sales_rep
def load_salesrep(engine):
    salesrep_export = load_csv('salesrep_export.csv')

    # Conversão do formato da data
    salesrep_export['HIRE_DATE'] = pd.to_datetime(salesrep_export['HIRE_DATE'], format='%d-%b-%y')
    salesrep_export['HIRE_DATE'] = salesrep_export['HIRE_DATE'].dt.strftime('%Y-%m-%d')

    # Resetar o índice
    salesrep_export = salesrep_export.reset_index(drop=True)

    salesrep_export = salesrep_export[['EMPLOYEE_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL',
                                        'PHONE_NUMBER', 'HIRE_DATE', 'JOB_ID', 'SALARY',
                                        'COMMISSION_PCT', 'MANAGER_ID', 'DEPARTMENT_ID']].rename(columns={
            'EMPLOYEE_ID': 'employee_id', 'FIRST_NAME': 'first_name', 'LAST_NAME': 'last_name',
            'EMAIL': 'email', 'PHONE_NUMBER': 'phone_number', 'HIRE_DATE': 'hire_date', 'JOB_ID': 'job_id',
            'SALARY': 'salary', 'COMMISSION_PCT': 'commission_pct', 'MANAGER_ID': 'manager_id',
            'DEPARTMENT_ID': 'department_id'
    })

    salesrep_export = salesrep_export.dropna()

    load_to_star_schema(salesrep_export, 'dimension_sales_rep', engine)
    print('carreguei dimension_sales_rep')

# Função para carregar os dados da dimension_date
def load_date(engine):
    orders_export = load_csv('orders_export.csv')
    Orders_Store_NYC = load_csv('Orders_Store_NYC.csv')
    orders = pd.concat([orders_export, Orders_Store_NYC]).reset_index(drop=True)
    orders['ORDER_DATE'] = pd.to_datetime(orders['ORDER_DATE'], format='%d-%b-%y')
    dates = orders[['ORDER_DATE']].drop_duplicates().reset_index(drop=True).rename(columns={'ORDER_DATE': 'sales_date'})
    dates['sales_day_of_year'] = dates['sales_date'].dt.dayofyear
    dates['sales_month'] = dates['sales_date'].dt.month
    dates['sales_year'] = dates['sales_date'].dt.year
    dates['sales_quarter'] = dates['sales_date'].dt.quarter
    dates['sales_month_name'] = dates['sales_date'].dt.month_name()
    load_to_star_schema(dates, 'dimension_date', engine)
    print('carreguei dimension_date')

# main()
try:
    engine = create_connection()

    # Processando os clientes
    load_customers(engine)
    # Processando os produtos
    load_products(engine)
    # Processando as promoções
    load_promotions(engine)
    # Processando os representantes de vendas
    load_salesrep(engine)
    # Processando as datas
    load_date(engine)
    # Processando os pedidos
    load_fact_sales(engine)

except Exception as e:
    print(f"Erro ao processar os dados: {e}")



