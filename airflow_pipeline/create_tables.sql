CREATE TABLE public.staging_payment(
    payment_id      INTEGER,
    customer_id     SMALLINT,
    staff_id        SMALLINT,
    rental_id       INTEGER,
    amount          NUMERIC,
    payment_date    TIMESTAMP
);

CREATE TABLE public.staging_address(
    address_id      INTEGER,
    address         TEXT,
    address2        TEXT,
    district        TEXT,
    city_id         SMALLINT,
    postal_code     TEXT,
    phone           TEXT,
    last_update     TIMESTAMP
);

CREATE TABLE public.staging_city(
    city_id         INTEGER,
    city            TEXT,
    country_id      SMALLINT,
    last_update     TIMESTAMP
);


CREATE TABLE public.staging_country(
    country_id      INTEGER,
    country         TEXT,
    last_update     TIMESTAMP
);

CREATE TABLE public.staging_customer(
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
);


CREATE TABLE public.staging_film(
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
);

CREATE TABLE public.staging_inventory(
    inventory_id        INTEGER,
    film_id             SMALLINT,
    store_id            SMALLINT,
    last_update         TIMESTAMP
);

CREATE TABLE public.staging_language(
    language_id         INTEGER,
    name                CHAR(20),
    last_update         TIMESTAMP
);

CREATE TABLE public.staging_rental(
    rental_id           INTEGER,
    rental_date         TIMESTAMP,
    inventory_id        INTEGER,
    customer_id         SMALLINT,
    return_date         TIMESTAMP,
    staff_id            SMALLINT,
    last_update         TIMESTAMP
);

CREATE TABLE public.staging_staff(
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
);

CREATE TABLE public.staging_store(
    store_id            INTEGER,
    manager_staff_id    SMALLINT,
    address_id          SMALLINT,
    last_update         TIMESTAMP
);

CREATE TABLE public.dates(
  date_key          INTEGER NOT NULL PRIMARY KEY,
  date              DATE NOT NULL,
  year              SMALLINT NOT NULL,
  quarter           SMALLINT NOT NULL,
  month             SMALLINT NOT NULL,
  day               SMALLINT NOT NULL,
  week              SMALLINT NOT NULL,
  is_weekend        BOOLEAN
);


CREATE TABLE public.customer(
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
);

CREATE TABLE public.movie(
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
);

CREATE TABLE public.store(
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
);

CREATE TABLE public.sales_fact(
  sales_key        INT IDENTITY(1, 1),
  date_key         INT NOT NULL REFERENCES dates(date_key),
  customer_key     INT NOT NULL REFERENCES customer(customer_key),
  movie_key        INT NOT NULL REFERENCES movie(movie_key),
  store_key        INT NOT NULL REFERENCES store(store_key),
  sales_amount     decimal(5,2) NOT NULL,
  primary key(sales_key)
);
