# Projeto ETL 

O projeto ETL consiste em uma implementação de um ETL a partir da leitura de dados de taxi de NY.

* Data: 19/09/2023
* Versão atual: 1.0 

## 1. Pré-Requisitos


* Python 3 - obtido na microsoft store
* [MySQL Workbanch na última versão](https://www.mysql.com/products/workbench/)
* Pandas module - digitar linha de comando: pip install pandas
* Sqlalchemy module - digitar linha de comando: pip install sqlalchemy
* MySQLdb module - digitar linha de comando: pip install mysql-connector-python

## 2. Estrutura deste Repositório

Este repositório está estruturado da seguinte forma:

* etl.py
* create_scripts_star.sql
* queries.sql

### 2. Instruções de compilação e execução

Antes de executar, certifique-se de que possui uma instância de banco de dados MySql e que tenha sido executado o script de criação de tabelas esquema star localizado em  `create_scripts_star.sql` rodando e certifique-se de inserir o nome do banco de dados e das credenciais nas constantes localizadas no código fonte. 

Execute a seguinte linha de comando :

```
python etl.py 
```
