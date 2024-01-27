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
