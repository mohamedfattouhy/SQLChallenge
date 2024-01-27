"""This file contains my solutions about some SQL queries
    using the famous painting database"""

from sqlalchemy import create_engine, text
from configuration.config import (
    DATABASE_USER,
    DATABASE_PASSWORD,
    DATABASE_HOST,
    DATABASE_PORT,
    DATABASE_NAME,
)


# Create a database connection with SQLAlchemy (MySQL Server 8.0)
engine = create_engine(
    f"mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
)
conn = engine.connect()  # Connection to the database


# ------------- Query 1 -------------
# Fetch all the paintings which are not displayed on any museums
query1 = conn.execute(
    text(
        "\
        select distinct name from work\
        where museum_id is NULL;\
        ")
    )

# ------------- Query 2 -------------
# Are there museums without any paintings ?
query2 = conn.execute(
    text(
        "\
        select distinct museum_id from museum\
        where museum_id NOT IN (select distinct museum_id from work);\
        ")
    )

# ------------- Query 3 -------------
# How many paintings have an asking price of more than their regular price ?
query3 = conn.execute(
    text(
        "\
        select distinct work_id from product_size\
        where sale_price > regular_price;\
        ")
    )

# ------------- Query 4 -------------
# Identify the paintings whose asking price is less than 50% of its regular price
query4 = conn.execute(
    text(
        "\
        select distinct name from product_size as ps\
        join work as w\
        where (ps.work_id = w.work_id\
        and sale_price < 0.5*regular_price);\
        ")
    )

# ------------- Query 5 -------------
# Which canva size costs the most ?
query5 = conn.execute(
    text(
        "\
        select distinct cs.size_id, sale_price from canvas_size as cs\
        join product_size as ps\
        where (cs.size_id = ps.size_id)\
        order by sale_price desc\
        limit 1;\
        ")
    )


# ------------- Query 6 -------------
# Delete duplicate records from work, product_size,
# subject and image_link tables

# Here's my solution for the table product_size. The same logic applies to the other tables.
# Of course, you'll need to check for duplicates.
# This can be done by checking whether the following number is
# greater than 0 or not (only if the table has no primary key !):
# select (select count(*) as cnt from table_name) - (select count(*) cnt_distinct from (select distinct * from table_name) x) as diff;
query6 = conn.execute(
    text(
        "\
         create table product_size_no_duplicate as\
                \
            select ps1.work_id, ps1.size_id, ps1.sale_price, ps1.regular_price\
            from (select *, row_number() over (partition by work_id, size_id, sale_price, regular_price) as rn\
            from product_size) as ps1\
                \
            join\
                \
            (select *, row_number() over (partition by work_id, size_id, sale_price, regular_price)  as rn\
            from product_size) as ps2\
                \
            on (ps1.work_id = ps2.work_id\
            and ps1.size_id = ps2.size_id\
            and ps1.sale_price = ps2.sale_price\
            and ps1.regular_price = ps2.regular_price\
            and (ps1.rn=1 and ps2.rn=1));\
                \
        delete from product_size;\
            \
        insert into product_size\
        select * from product_size_no_duplicate;\
            \
        drop table product_size_no_duplicate;\
    "
    )
)
