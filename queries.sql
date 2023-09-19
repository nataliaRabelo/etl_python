-- @author Natália Rabelo, Thais Moylany e Darah Leite
-- SQL compatível com MYSQL.
-- Como compilar e executar: mysql -u queries -p database_name < queries.sql

-- Qual o valor vendido por tipo de promoção por mês/quadrimestre/ano?
SELECT DP.PROMO_NAME, DD.SALES_MONTH, DD.SALES_QUARTER, DD.SALES_YEAR, SUM(FS.DOLLARS_SOLD) AS TOTAL_SOLD
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_PROMOTION DP ON FS.PROMOTION_DIM_ID = DP.PROMOTION_DIM_ID
GROUP BY DP.PROMO_NAME, DD.SALES_MONTH, DD.SALES_QUARTER, DD.SALES_YEAR;

-- Qual a quantidade vendida por tipo de promoção por mês/quadrimestre/ano?

SELECT DP.PROMO_NAME, DD.SALES_MONTH, DD.SALES_QUARTER, DD.SALES_YEAR, SUM(FS.QUANTITY_SOLD) AS TOTAL_QUANTITY_SOLD
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_PROMOTION DP ON FS.PROMOTION_DIM_ID = DP.PROMOTION_DIM_ID
GROUP BY DP.PROMO_NAME, DD.SALES_MONTH, DD.SALES_QUARTER, DD.SALES_YEAR;

-- Qual o valor em promoção por vendedor por ano e qual o percentual do vendedor do total de vendas?

SELECT DS.EMPLOYEE_ID, DS.FIRST_NAME, DS.LAST_NAME, DP.PROMO_NAME, DD.SALES_YEAR, SUM(FS.DOLLARS_SOLD) AS TOTAL_SOLD,
       (SUM(FS.DOLLARS_SOLD) / (SELECT SUM(DOLLARS_SOLD) FROM FACT_SALES FS2
                                JOIN DIMENSION_DATE DD2 ON FS2.DATE_DIM_ID = DD2.DATE_DIM_ID
                                JOIN DIMENSION_PROMOTION DP2 ON FS2.PROMOTION_DIM_ID = DP2.PROMOTION_DIM_ID
                                WHERE DD2.SALES_YEAR = DD.SALES_YEAR AND DP2.PROMO_ID IS NOT NULL)) * 100 AS PERCENTAGE_OF_TOTAL
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_SALES_REP DS ON FS.SALES_REP_DIM_ID = DS.SALES_REP_DIM_ID
JOIN DIMENSION_PROMOTION DP ON FS.PROMOTION_DIM_ID = DP.PROMOTION_DIM_ID
WHERE DP.PROMO_ID IS NOT NULL
GROUP BY DS.EMPLOYEE_ID, DS.FIRST_NAME, DS.LAST_NAME, DP.PROMO_NAME, DD.SALES_YEAR;

-- Quais os 10 produtos mais vendidos em promoção por mês/ano?

SELECT DP.PRODUCT_NAME, DD.SALES_MONTH, DD.SALES_YEAR, SUM(FS.QUANTITY_SOLD) AS TOTAL_SOLD
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_PRODUCT DP ON FS.PRODUCT_DIM_ID = DP.PRODUCT_DIM_ID
JOIN DIMENSION_PROMOTION DPR ON FS.PROMOTION_DIM_ID = DPR.PROMOTION_DIM_ID
WHERE DPR.PROMO_ID IS NOT NULL
GROUP BY DP.PRODUCT_NAME, DD.SALES_MONTH, DD.SALES_YEAR
ORDER BY TOTAL_SOLD DESC
LIMIT 10;


-- Quais as categorias produtos mais vendidos em promoção por mês/ano?

SELECT DP.CATEGORY_NAME, DP.PRODUCT_NAME, DD.SALES_MONTH, DD.SALES_YEAR, SUM(FS.QUANTITY_SOLD) AS TOTAL_SOLD
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_PRODUCT DP ON FS.PRODUCT_DIM_ID = DP.PRODUCT_DIM_ID
JOIN DIMENSION_PROMOTION DPR ON FS.PROMOTION_DIM_ID = DPR.PROMOTION_DIM_ID
WHERE DPR.PROMO_ID IS NOT NULL
GROUP BY DP.CATEGORY_NAME, DP.PRODUCT_NAME, DD.SALES_MONTH, DD.SALES_YEAR
ORDER BY TOTAL_SOLD DESC;


-- Quais os percentuais de categorias vendidas por mês/ano considerando preço?

SELECT DP.CATEGORY_NAME, DD.SALES_MONTH, DD.SALES_YEAR, 
       SUM(FS.DOLLARS_SOLD) AS TOTAL_SALES, 
       (SUM(FS.DOLLARS_SOLD) / 
           (SELECT SUM(DOLLARS_SOLD) 
            FROM FACT_SALES FS2
            JOIN DIMENSION_DATE DD2 ON FS2.DATE_DIM_ID = DD2.DATE_DIM_ID
            WHERE DD2.SALES_YEAR = DD.SALES_YEAR AND DD2.SALES_MONTH = DD.SALES_MONTH)
        ) * 100 AS PERCENTAGE_OF_TOTAL
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_PRODUCT DP ON FS.PRODUCT_DIM_ID = DP.PRODUCT_DIM_ID
GROUP BY DP.CATEGORY_NAME, DD.SALES_MONTH, DD.SALES_YEAR;

-- Quais os percentuais de categorias vendidas por mês/ano considerando quantidade?

SELECT DP.CATEGORY_NAME, DD.SALES_MONTH, DD.SALES_YEAR, 
       SUM(FS.QUANTITY_SOLD) AS TOTAL_QUANTITY, 
       (SUM(FS.QUANTITY_SOLD) / 
           (SELECT SUM(QUANTITY_SOLD) 
            FROM FACT_SALES FS2
            JOIN DIMENSION_DATE DD2 ON FS2.DATE_DIM_ID = DD2.DATE_DIM_ID
            WHERE DD2.SALES_YEAR = DD.SALES_YEAR AND DD2.SALES_MONTH = DD.SALES_MONTH)
        ) * 100 AS PERCENTAGE_OF_TOTAL
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_PRODUCT DP ON FS.PRODUCT_DIM_ID = DP.PRODUCT_DIM_ID
GROUP BY DP.CATEGORY_NAME, DD.SALES_MONTH, DD.SALES_YEAR;

-- Quais os 10 clientes que mais compram promoção por mês/ano?

SELECT DC.CUSTOMER_ID, DC.CUST_FIRST_NAME, DC.CUST_LAST_NAME, DP.PROMO_NAME, DD.SALES_MONTH, DD.SALES_YEAR, SUM(FS.QUANTITY_SOLD) AS TOTAL_SOLD
FROM FACT_SALES FS
JOIN DIMENSION_DATE DD ON FS.DATE_DIM_ID = DD.DATE_DIM_ID
JOIN DIMENSION_CUSTOMER DC ON FS.CUSTOMER_DIM_ID = DC.CUSTOMER_DIM_ID
JOIN DIMENSION_PROMOTION DP ON FS.PROMOTION_DIM_ID = DP.PROMOTION_DIM_ID
WHERE DP.PROMO_ID IS NOT NULL
GROUP BY DC.CUSTOMER_ID, DC.CUST_FIRST_NAME, DC.CUST_LAST_NAME, DP.PROMO_NAME, DD.SALES_MONTH, DD.SALES_YEAR
ORDER BY TOTAL_SOLD DESC
LIMIT 10;