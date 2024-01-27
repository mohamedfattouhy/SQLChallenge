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

# NOTE:
# If the code above doesn't give exactly the right number of rows,
# it's because at least one column contains NULLs. Simply replace
# them with another value, such as -99.


# ------------- Query 7 -------------
# Identify the museums with invalid city information in the given dataset

# For this question, I used regular expressions to retrieve
# lines containing numeric characters for the 'city' column
query7 = conn.execute(
    text(
        "select * from museum\
         where regexp_like(city, '^[0-9]+$');  # '^[0-9]+$' means a string containing only numbers\
        "
    )
)


# ------------- Query 9 -------------
# Fetch the top 10 most famous painting subject

query9 = conn.execute(
    text(
        "\
        select subject, count(subject) as cnt_subject from subject\
        group by subject\
        order by cnt_subject desc\
        limit 10;\
    "
    )
)

# ------------- Query 10 -------------
# Identify the museums which are open on both Sunday and Monday
# Display museum name, city

# The idea is to create a column worth 1 when the museum is open
# on Monday and/or Sunday (0 otherwise), and to group the data
# by museum by summing this column
query10 = conn.execute(
    text(
        "with \
                museum_1 as (select museum_id,\
                    case when day in ('Sunday', 'Monday') then 1\
                    else 0 end as top_sunday_monday\
                    FROM museum_hours),\
        \
                museum_2 as (select museum_id,\
                    sum(top_sunday_monday) as open_sunday_monday from museum_1\
                    group by museum_id)\
    \
    select m.name, m.city from museum_2 as m2\
    join \
    museum as m\
    on (m2.museum_id=m.museum_id)\
    where open_sunday_monday = 2;\
    "
    )
)
