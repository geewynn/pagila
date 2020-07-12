class SqlQueries:
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