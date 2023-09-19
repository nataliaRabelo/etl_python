-- author Natália Rabelo, Thais Moylany e Darah Leite
-- Como compilar e executar: mysql -u username -p database_name < create_scripts_star.sql

CREATE TABLE dimension_date (
    date_dim_id INT AUTO_INCREMENT PRIMARY KEY,
    sales_date DATE,
    sales_day_of_year INT,
    sales_month INT,
    sales_year INT,
    sales_quarter INT,
    sales_month_name VARCHAR(255)
);

CREATE TABLE dimension_promotion (
    promotion_dim_id INT AUTO_INCREMENT PRIMARY KEY,
    promo_id INT,
    promo_name VARCHAR(255)
);

CREATE TABLE dimension_product (
    product_dim_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(255),
    product_description TEXT,
    category_id INT,
    category_name VARCHAR(255),
    weight_class INT,
    warranty_period INT,
    supplier_id INT,
    product_status VARCHAR(50),
    list_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    catalog_url VARCHAR(255),
    parent_category_id INT,
    parent_category_name VARCHAR(255),
    parent_category_description TEXT
);

CREATE TABLE dimension_customer (
    customer_dim_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    cust_first_name VARCHAR(255),
    cust_last_name VARCHAR(255),
    street_add VARCHAR(255),
    postal_code VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country_id VARCHAR(255),
    country_name VARCHAR(255),
    region_id INT,
    nls_language VARCHAR(50),
    nls_territory VARCHAR(50),
    credit_limit DECIMAL(10,2),
    cust_email VARCHAR(255),
    phone VARCHAR(30),
    account_mgr_id INT,
    location_type VARCHAR(255),
    location_grid VARCHAR(255),
    location_x DECIMAL(10,2),
    location_y DECIMAL(10,2)
);

CREATE TABLE dimension_sales_rep (
    sales_rep_dim_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(30),
    hire_date DATE,
    job_id VARCHAR(30),
    salary DECIMAL(10,2),
    commission_pct DECIMAL(5,2),
    manager_id INT,
    department_id INT
);

CREATE TABLE fact_sales (
    product_dim_id INT,
    promotion_dim_id INT,
    customer_dim_id INT,
    sales_rep_dim_id INT,
    date_dim_id INT,
    dollars_sold DECIMAL(10,2),
    quantity_sold INT,
    PRIMARY KEY (product_dim_id, promotion_dim_id, customer_dim_id, sales_rep_dim_id, date_dim_id),
    FOREIGN KEY (product_dim_id) REFERENCES dimension_product(product_dim_id),
    FOREIGN KEY (promotion_dim_id) REFERENCES dimension_promotion(promotion_dim_id),
    FOREIGN KEY (customer_dim_id) REFERENCES dimension_customer(customer_dim_id),
    FOREIGN KEY (sales_rep_dim_id) REFERENCES dimension_sales_rep(sales_rep_dim_id),
    FOREIGN KEY (date_dim_id) REFERENCES dimension_date(date_dim_id)
);

