import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
PAYMENT_DATA= config['S3']['PAYMENT_DATA']
ADDRESS_DATA= config['S3']['ADDRESS_DATA']
CITY_DATA= config['S3']['CITY_DATA']
COUNTRY_DATA= config['S3']['COUNTRY_DATA']
CUSTOMER_DATA= config['S3']['CUSTOMER_DATA']
FILM_DATA= config['S3']['FILM_DATA']
INVENTORY_DATA= config['S3']['INVENTORY_DATA']
LANGUAGE_DATA= config['S3']['LANGUAGE_DATA']
RENTAL_DATA= config['S3']['RENTAL_DATA']
STAFF_DATA= config['S3']['STAFF_DATA']
STORE_DATA= config['S3']['STORE_DATA']


staging_payment_table_drop = "DROP TABLE IF EXISTS staging_payment"
staging_address_table_drop = "DROP TABLE IF EXISTS staging_address"
staging_city_table_drop = "DROP TABLE IF EXISTS staging_city"
staging_country_table_drop = "DROP TABLE IF EXISTS staging_country"
staging_customer_table_drop = "DROP TABLE IF EXISTS staging_customer"
staging_film_table_drop = "DROP TABLE IF EXISTS staging_film"
staging_inventory_table_drop = "DROP TABLE IF EXISTS staging_inventory"
staging_language_table_drop = "DROP TABLE IF EXISTS staging_language"
staging_rental_table_drop = "DROP TABLE IF EXISTS staging_rental"
staging_staff_table_drop = "DROP TABLE IF EXISTS staging_staff" 
staging_store_table_drop = "DROP TABLE IF EXISTS staging_store"
date_table_drop = "DROP TABLE IF EXISTS dates cascade"
customer_table_drop = "DROP TABLE IF EXISTS customer cascade"
movie_table_drop = "DROP TABLE IF EXISTS movie  cascade"
store_table_drop = "DROP TABLE IF EXISTS store cascade"
sales_fact_drop = "DROP TABLE IF EXISTS sales_fact cascade"



staging_payment_table_create = ("""CREATE TABLE IF NOT EXISTS staging_payment(
    payment_id      INTEGER,
    customer_id     SMALLINT,
    staff_id        SMALLINT,
    rental_id       INTEGER,
    amount          NUMERIC,
    payment_date    TIMESTAMP
)
""")

staging_address_table_create = ("""CREATE TABLE IF NOT EXISTS staging_address(
    address_id      INTEGER,
    address         TEXT,
    address2        TEXT,
    district        TEXT,
    city_id         SMALLINT,
    postal_code     TEXT,
    phone           TEXT,
    last_update     TIMESTAMP
)
""")

staging_city_table_create = ("""CREATE TABLE IF NOT EXISTS staging_city(
    city_id         INTEGER,
    city            TEXT,
    country_id      SMALLINT,
    last_update     TIMESTAMP
)
""")


staging_country_table_create = ("""CREATE TABLE IF NOT EXISTS staging_country(
    country_id      INTEGER,
    country         TEXT,
    last_update     TIMESTAMP
)
""")

staging_customer_table_create = ("""CREATE TABLE IF NOT EXISTS staging_customer(
    customer_id     INTEGER,
    store_id        SMALLINT,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT,
    address         SMALLINT,
    activebool      BOOLEAN,
    create_date     DATE,
    last_update     TIMESTAMP,
    active          INTEGER
)
""")

staging_film_create_table = ("""CREATE TABLE IF NOT EXISTS staging_film(
    film_id         INTEGER,
    title           TEXT,
    description     TEXT,
    release_year    INTEGER,
    language_id     SMALLINT,
    original_language_id    SMALLINT,
    rental_duration         SMALLINT,
    rental_rate             NUMERIC,
    length                  SMALLINT,
    replacement_cost        NUMERIC,
    rating                  TEXT,
    last_update             TIMESTAMP,
    special_features        VARCHAR,
    fulltext                VARCHAR
)
""")

staging_inventory_create_table = ("""CREATE TABLE IF NOT EXISTS staging_inventory(
    inventory_id        INTEGER,
    film_id             SMALLINT,
    store_id            SMALLINT,
    last_update         TIMESTAMP
)
""")

staging_language_create_table = ("""CREATE TABLE IF NOT EXISTS staging_language(
    language_id         INTEGER,
    name                CHAR(20),
    last_update         TIMESTAMP
)
""")

staging_rental_create_table = ("""CREATE TABLE IF NOT EXISTS staging_rental(
    rental_id           INTEGER,
    rental_date         TIMESTAMP,
    inventory_id        INTEGER,
    customer_id         SMALLINT,
    return_date         TIMESTAMP,
    staff_id            SMALLINT,
    last_update         TIMESTAMP
)
""")

staging_staff_create_table = ("""CREATE TABLE IF NOT EXISTS staging_staff(
    staff_id        INTEGER,
    first_name      TEXT,
    last_name       TEXT,
    address_id      SMALLINT,
    email           TEXT,
    store_id        SMALLINT,
    active          BOOLEAN,
    username        TEXT,
    password        TEXT,
    last_update     TIMESTAMP,
    picture         text
)
""")

staging_store_create_table = ("""CREATE TABLE IF NOT EXISTS staging_store(
    store_id            INTEGER,
    manager_staff_id    SMALLINT,
    address_id          SMALLINT,
    last_update         TIMESTAMP
)
""")

date_table_create = ("""CREATE TABLE dates(
  date_key          INTEGER NOT NULL PRIMARY KEY,
  date              DATE NOT NULL,
  year              SMALLINT NOT NULL,
  quarter           SMALLINT NOT NULL,
  month             SMALLINT NOT NULL,
  day               SMALLINT NOT NULL,
  week              SMALLINT NOT NULL,
  is_weekend        BOOLEAN
)
""")


customer_table_create = ("""CREATE TABLE customer(
  customer_key smallint PRIMARY KEY,
  customer_id  smallint NOT NULL,
  first_name   varchar(45) NOT NULL,
  last_name    varchar(45) NOT NULL,
  email        varchar(50),
  address      varchar(50) NOT NULL,
  address2     varchar(50),
  district     varchar(20) NOT NULL,
  city         varchar(50) NOT NULL,
  country      varchar(50) NOT NULL,
  postal_code  varchar(10),
  phone        varchar(20) NOT NULL,
  active       smallint NOT NULL,
  create_date  timestamp NOT NULL
)
""")

movie_table_create = ("""CREATE TABLE movie(
  movie_key          smallint PRIMARY KEY,
  film_id            smallint NOT NULL,
  title              varchar(255) NOT NULL,
  description        text,
  release_year       text,
  language           varchar(20) NOT NULL,
  original_language  varchar(20),
  rental_duration    smallint NOT NULL,
  length             smallint NOT NULL,
  rating             varchar(5) NOT NULL,
  special_features   VARCHAR NOT NULL
)
""")

store_table_create = ("""CREATE TABLE store(
  store_key           smallint PRIMARY KEY,
  store_id            smallint NOT NULL,
  address             varchar(50) NOT NULL,
  address2            varchar(50),
  district            varchar(20) NOT NULL,
  city                varchar(50) NOT NULL,
  country             varchar(50) NOT NULL,
  postal_code         varchar(10),
  manager_first_name  varchar(45) NOT NULL,
  manager_last_name   varchar(45) NOT NULL
)
""")

sales_fact_table_create = ("""CREATE TABLE sales_fact(
  sales_key        INT IDENTITY(1, 1),
  date_key         INT NOT NULL REFERENCES dates(date_key),
  customer_key     INT NOT NULL REFERENCES customer(customer_key),
  movie_key        INT NOT NULL REFERENCES movie(movie_key),
  store_key        INT NOT NULL REFERENCES store(store_key),
  sales_amount     decimal(5,2) NOT NULL,
  primary key(sales_key)
)
""")


staging_payment_copy = ("""copy staging_payment
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(PAYMENT_DATA, IAM_ROLE)

staging_address_copy = ("""copy staging_address
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(ADDRESS_DATA, IAM_ROLE)

staging_city_copy = ("""copy staging_city
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(CITY_DATA, IAM_ROLE)

staging_country_copy = ("""copy staging_country
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(COUNTRY_DATA, IAM_ROLE)

staging_customer_copy = ("""copy staging_customer
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(CUSTOMER_DATA, IAM_ROLE)

staging_film_copy = ("""copy staging_film
                            from {} 
                            IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            DELIMITER '|'
                            ACCEPTINVCHARS
                            ESCAPE
                            ;
""").format(FILM_DATA, IAM_ROLE)

staging_inventory_copy = ("""copy staging_inventory
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(INVENTORY_DATA, IAM_ROLE)

staging_language_copy = ("""copy staging_language
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(LANGUAGE_DATA, IAM_ROLE)

staging_rental_copy = ("""copy staging_rental
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(RENTAL_DATA, IAM_ROLE)

staging_staff_copy = ("""copy staging_staff
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(STAFF_DATA, IAM_ROLE)

staging_store_copy = ("""copy staging_store
                            from {} IGNOREHEADER 1
                            iam_role {}
                            region 'us-east-2'
                            csv;
""").format(STORE_DATA, IAM_ROLE)


date_table_insert = ("""INSERT INTO dates (date_key, date, year, quarter, month, day, week)
SELECT 
       DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
       date(payment_date)                                           AS date,
       EXTRACT(year FROM payment_date)                              AS year,
       EXTRACT(quarter FROM payment_date)                           AS quarter,
       EXTRACT(month FROM payment_date)                             AS month,
       EXTRACT(day FROM payment_date)                               AS day,
       EXTRACT(week FROM payment_date)                              AS week
FROM staging_payment;
""")

customer_table_insert = ("""INSERT INTO customer (customer_key, customer_id, first_name, last_name, email, address, 
                         address2, district, city, country, postal_code, phone, active, 
                         create_date)
SELECT c.customer_id as customer_key, 
    c.customer_id, 
    c.first_name,
    c.last_name, 
    c.email, 
    a.address, 
    a.address2,
    a.district, 
    ci.city, 
    co.country, 
    a.postal_code, 
    a.phone, 
    c.active, 
    c.create_date
FROM staging_customer c
JOIN staging_address a  ON (c.address = a.address_id)
JOIN staging_city ci    ON (a.city_id = ci.city_id)
JOIN staging_country co ON (ci.country_id = co.country_id)
""")

movie_table_insert = ("""INSERT INTO movie (movie_key, film_id, title, description, release_year, language, 
                      original_language, rental_duration, length, rating, special_features)
SELECT f.film_id      AS movie_key,
       f.film_id,
       f.title,
       f.description,
       f.release_year,
       l.name         AS language,
       orig_lang.name AS original_language,
       f.rental_duration,
       f.length,
       f.rating,
       f.special_features
FROM staging_film f
JOIN staging_language l              ON (f.language_id=l.language_id)
LEFT JOIN staging_language orig_lang ON (f.original_language_id = orig_lang.language_id)
""")

store_table_insert = ("""INSERT INTO store (store_key, store_id, address, address2, district, city, country, postal_code, manager_first_name, 
                      manager_last_name)
SELECT s.store_id    AS store_key,
       s.store_id,
       a.address,
       a.address2,
       a.district,
       c.city,
       co.country,
       a.postal_code,
       st.first_name AS manager_first_name,
       st.last_name  AS manager_last_name
FROM staging_store s
JOIN staging_staff st   ON (s.manager_staff_id = st.staff_id)
JOIN staging_address a  ON (s.address_id = a.address_id)
JOIN staging_city c     ON (a.city_id = c.city_id)
JOIN staging_country co ON (c.country_id = co.country_id)
""")

sales_fact_insert = ("""INSERT INTO sales_fact (date_key, customer_key, movie_key, store_key, sales_amount)
SELECT TO_CHAR(p.payment_date :: DATE, 'yyyyMMDD')::integer AS date_key ,
       p.customer_id                                        AS customer_key,
       i.film_id                                            AS movie_key,
       i.store_id                                           AS store_key,
       p.amount                                             AS sales_amount
FROM staging_payment p
JOIN staging_rental r    ON ( p.rental_id = r.rental_id )
JOIN staging_inventory i ON ( r.inventory_id = i.inventory_id )
""")

create_table_queries = [staging_payment_table_create,
                        staging_address_table_create,
                        staging_city_table_create,
                        staging_country_table_create,
                        staging_customer_table_create,
                        staging_film_create_table,
                        staging_inventory_create_table,
                        staging_language_create_table,
                        staging_rental_create_table,
                        staging_staff_create_table,
                        staging_store_create_table,
                        date_table_create,
                        customer_table_create,
                        movie_table_create,
                        store_table_create,
                        sales_fact_table_create]


drop_table_queries = [staging_payment_table_drop,
                        staging_address_table_drop,
                        staging_city_table_drop,
                        staging_country_table_drop,
                        staging_customer_table_drop,
                        staging_film_table_drop,
                        staging_inventory_table_drop,
                        staging_language_table_drop,
                        staging_rental_table_drop,
                        staging_staff_table_drop,
                        staging_store_table_drop,
                        date_table_drop,
                        customer_table_drop,
                        movie_table_drop,
                        store_table_drop,
                        sales_fact_drop]

copy_table_queries = [staging_payment_copy,
                        staging_address_copy,
                        staging_city_copy,
                        staging_country_copy,
                        staging_customer_copy,
                        staging_film_copy,
                        staging_language_copy,
                        staging_inventory_copy,
                        staging_rental_copy,
                        staging_staff_copy,
                        staging_store_copy]

insert_table_queries = [date_table_insert,
                        customer_table_insert,
                        movie_table_insert,
                        store_table_insert,
                        sales_fact_insert]
