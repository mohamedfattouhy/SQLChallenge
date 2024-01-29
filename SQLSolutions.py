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
        """
        select distinct name from work
        where museum_id is null;
        """
    )
)

# ------------- Query 2 -------------
# Are there museums without any paintings ?
query2 = conn.execute(
    text(
        """
        select distinct museum_id from museum
        where museum_id NOT IN (select distinct museum_id from work);
        """
    )
)

# ------------- Query 3 -------------
# How many paintings have an asking price of more than their regular price ?
query3 = conn.execute(
    text(
        """
        select distinct work_id from product_size
        where sale_price > regular_price;
        """
    )
)

# ------------- Query 4 -------------
# Identify the paintings whose asking price is less than 50% of its regular price
query4 = conn.execute(
    text(
        """
        select distinct name from product_size as ps
        join work as w
        where (ps.work_id = w.work_id
        and sale_price < 0.5*regular_price);
        """
    )
)

# ------------- Query 5 -------------
# Which canva size costs the most ?
query5 = conn.execute(
    text(
        """
        select distinct cs.size_id, sale_price from canvas_size as cs
        join product_size as ps
        where (cs.size_id = ps.size_id)
        order by sale_price desc
        limit 1;
        """
    )
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
        """
        create table product_size_no_duplicate as

            select ps1.work_id, ps1.size_id, ps1.sale_price, ps1.regular_price
            from (select *, row_number() over (partition by work_id, size_id, sale_price, regular_price) as rn
            from product_size) as ps1

            join

            (select *, row_number() over (partition by work_id, size_id, sale_price, regular_price)  as rn
            from product_size) as ps2

            on (ps1.work_id = ps2.work_id
            and ps1.size_id = ps2.size_id
            and ps1.sale_price = ps2.sale_price
            and ps1.regular_price = ps2.regular_price
            and (ps1.rn=1 and ps2.rn=1));

        delete from product_size;

        insert into product_size
        select * from product_size_no_duplicate;

        drop table product_size_no_duplicate;
    """
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
        """"
        select * from museum
         where regexp_like(city, '^[0-9]+$');  # '^[0-9]+$' means a string containing only numbers
        """
    )
)

# ------------- Query 8 -------------
# Museum_Hours table has 1 invalid entry. Identify it and remove it.

query8 = conn.execute(
    text(
        """"
        delete from museum_hours
        where open >= str_to_date('12:01:PM','%h:%i:%p');
    """
    )
)

# ------------- Query 9 -------------
# Fetch the top 10 most famous painting subject

query9 = conn.execute(
    text(
        """"
        select subject, count(subject) as cnt_subject from subject
        group by subject
        order by cnt_subject desc
        limit 10;
    """
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
        """
        with
            museum_1 as (select museum_id,
                case when day in ('Sunday', 'Monday') then 1
                else 0 end as top_sunday_monday
                FROM museum_hours),

            museum_2 as (select museum_id,
                sum(top_sunday_monday) as open_sunday_monday from museum_1
                group by museum_id)

        select m.name, m.city from museum_2 as m2
        join
        museum as m
        on (m2.museum_id=m.museum_id)
        where open_sunday_monday = 2;
    """
    )
)

# ------------- Query 11 -------------
#  How many museums are open every single day ?

query11 = conn.execute(
    text(
        """
        select count(*) as cnt_open_every_day from(
            select museum_id, count(day) as cnt_day from museum_hours
            group by museum_id
            having cnt_day = 7
        ) x;
        """
    )
)

# ------------- Query 12 -------------
# Which are the top 5 most popular museum ? (Popularity is defined based on most
# no of paintings in a museum)

query12 = conn.execute(
    text(
        """
        select top_5_museum.museum_id, top_5_museum.cnt_paintings, m.name,  m.city

        from (select museum_id, count(name) as cnt_paintings from work
        where museum_id is not null
        group by museum_id
        order by cnt_paintings desc
        limit 5) as top_5_museum

        join

        museum as m

        on (top_5_museum.museum_id = m.museum_id);
        """
    )
)

# ------------- Query 13 -------------
# Who are the top 5 most popular artist ? (Popularity is defined based on most no of
# paintings done by an artist)

query13 = conn.execute(
    text(
        """
        select top_5_artist.artist_id, top_5_artist.cnt_paintings,
        a.full_name, a.nationality, a.style

        from (select artist_id, count(name) as cnt_paintings from work
        group by artist_id
        order by cnt_paintings desc
        limit 5) as top_5_artist

        join

        artist as a

        on (top_5_artist.artist_id = a.artist_id);
        """
    )
)

# ------------- Query 14 -------------
# Display the 3 least popular canva sizes

query14 = conn.execute(
    text(
        """
        with count_work_by_size as (
            select size_id, count(work_id) as cnt_work from product_size
            where size_id = round(size_id)  # size_id has to be an integer
            group by size_id),

            count_rnk as (
            select *, dense_rank() over(order by cnt_work asc) as rnk from count_work_by_size)

        select * from count_rnk as cr
        where cr.rnk <= 3;  # We keep only ranks below 3
        """
    )
)


# ------------- Query 15 -------------
# Which museum is open for the longest during a day. Dispay museum name, state
# and hours open and which day ?

# We start by calculating how long each museum is open on different days
# Then we create a column of rank to select the museum with the longest opening day
query15 = conn.execute(
    text(
        """
        with museum_hours_1 as (select museum_id, day, open, close,
                         str_to_date(close,'%h:%i:%p') - str_to_date(open,'%h:%i:%p') as duration_in_hour
                         from museum_hours),

            museum_hours_2 as (select *,
                         rank() over (order by duration_in_hour desc) as rnk
                         from museum_hours_1)

        select name, state as city, day, open, close,
               duration_in_hour as 'duration_in_hour (hh mi ss)'
        from museum_hours_2 as m2
        join museum as m
        on (m.museum_id = m2.museum_id)

        where rnk = 1;  # We keep the longest day
    """
    )
)

# ------------- Query 16 -------------
# Which museum has the most no of most popular painting style ?

# We start by calculating the most popular style
# Then we find out which museum exhibits the most paintings in this style
query16 = conn.execute(
    text(
        """
        with most_pop_painting as (
                    select style from (
                    select style, count(name) as cnt_style from work
                    group by style
                    order by cnt_style desc
                    limit 1) as x),

            museum_most_pop_painting as (
                    select museum_id, count(*) as cnt_most_pop_painting, min(style) as style from work
                    where style in (select style from most_pop_painting) and museum_id is not null
                    group by museum_id
                    order by cnt_most_pop_painting desc
                    limit 1)

        select m.museum_id, m.name, m.city, museum_most_pop_painting.style,
        museum_most_pop_painting.cnt_most_pop_painting

        from museum_most_pop_painting
        join
        museum as m

        on (museum_most_pop_painting.museum_id = m.museum_id);
    """
    )
)


# ------------- Query 14 -------------
# Display the 3 least popular canva sizes

query14 = conn.execute(
    text(
        """
        with count_work_by_size as (
            select size_id, count(work_id) as cnt_work from product_size
            where size_id = round(size_id)  # size_id has to be an integer
            group by size_id),

            count_rnk as (
            select *, dense_rank() over(order by cnt_work asc) as rnk from count_work_by_size)

        select * from count_rnk as cr
        where cr.rnk <= 3;  # We keep only ranks below 3
        """
    )
)


# ------------- Query 15 -------------
# Which museum is open for the longest during a day. Dispay museum name, state
# and hours open and which day ?

# We start by calculating how long each museum is open on different days
# Then we create a column of rank to select the museum with the longest opening day
query15 = conn.execute(
    text(
        """
        with museum_hours_1 as (select museum_id, day, open, close,
                         str_to_date(close,'%h:%i:%p') - str_to_date(open,'%h:%i:%p') as duration_in_hour
                         from museum_hours),

            museum_hours_2 as (select *,
                         rank() over (order by duration_in_hour desc) as rnk
                         from museum_hours_1)

        select name, state as city, day, open, close,
               duration_in_hour as 'duration_in_hour (hh mi ss)'
        from museum_hours_2 as m2
        join museum as m
        on (m.museum_id = m2.museum_id)

        where rnk = 1;  # We keep the longest day
    """
    )
)

# ------------- Query 16 -------------
# Which museum has the most no of most popular painting style ?

# We start by calculating the most popular style
# Then we find out which museum exhibits the most paintings in this style
query16 = conn.execute(
    text(
        """
        with most_pop_painting as (
                    select style from (
                    select style, count(name) as cnt_style from work
                    group by style
                    order by cnt_style desc
                    limit 1) as x),

            museum_most_pop_painting as (
                    select museum_id, count(*) as cnt_most_pop_painting, min(style) as style from work
                    where style in (select style from most_pop_painting) and museum_id is not null
                    group by museum_id
                    order by cnt_most_pop_painting desc
                    limit 1)

        select m.museum_id, m.name, m.city, museum_most_pop_painting.style,
        museum_most_pop_painting.cnt_most_pop_painting

        from museum_most_pop_painting
        join
        museum as m

        on (museum_most_pop_painting.museum_id = m.museum_id);
    """
    )
)

# ------------- Query 17 -------------
# Identify the artists whose paintings are displayed in multiple countries

# We calculate the number of countries in which each artist has paintings displayed,
# then keep only those whose paintings are displayed at least in two countries.
query17 = conn.execute(
    text(
        """
        with artist_country as (
            select artist_id, count(distinct country) as cnt_country from (
                select w.artist_id, m.country
                from museum m
                join
                work w
                on (m.museum_id = w.museum_id)) as x
            group by artist_id
            order by cnt_country desc)


    select ac.artist_id, ac.cnt_country, a.full_name,
    a.nationality, a.style

    from artist_country ac
    join
    artist a

    on (ac.artist_id = a.artist_id
    and ac.cnt_country >= 2)  # We select artists who are displayed at least in two different countries
    order by ac.cnt_country desc;
    """
    )
)

# ------------- Query 18 -------------
# Display the country and the city with most no of museums.
# Output 2 seperate columns to mention the city and country.
# If there are multiple value, seperate them with comma.


query18 = conn.execute(
    text(
        """
        with
            museum_by_city as (select city, count(distinct museum_id) as cnt_museum_by_city,
            rank() over (order by count(distinct museum_id) desc) as rnk from museum
            group by city),

            museum_by_country as (select country, count(distinct museum_id) as cnt_museum_by_country,
            rank() over (order by count(distinct museum_id) desc) as rnk  from museum
            group by country)

        select group_concat(city) as cities, max(cnt_museum_by_city) as cnt_museum_city,
        group_concat(country, ' ') as country, max(cnt_museum_by_country) as cnt_museum_country
        from museum_by_city, museum_by_country

        where museum_by_city.rnk = 1
        and museum_by_country.rnk = 1;
    """
    )
)

# ------------- Query 19 -------------
# Identify the artist and the museum where the most expensive and least expensive
# painting is placed. Display the artist name, sale_price, painting name, museum
# name, museum city and canvas label

# We calculate which paint is the cheapest and which is the most expensive,
# then add the necessary information as we go along.
query19 = conn.execute(
    text(
        """
        with least_and_most_expensive as (
            select work_id, sale_price, size_id from product_size
            where sale_price = (select min(sale_price) from product_size)
            union
            select work_id, sale_price, size_id from product_size
            where sale_price = (select max(sale_price) from product_size)
            ),

            add_id as (
                select least_and_most_expensive.*,
                w.name as painting_name, w.artist_id, w.museum_id
                from least_and_most_expensive
                join
                work as w
                on (least_and_most_expensive.work_id = w.work_id)
            ),

            add_artist_name as (
                select add_id.*, artist.full_name
                from add_id
                join
                artist
                on (add_id.artist_id = artist.artist_id)
            ),

            add_museum_informations as (
                select add_artist_name.*, museum.name as museum_name, museum.city
                from add_artist_name
                join
                museum
                on (add_artist_name.museum_id = museum.museum_id)
            ),

            add_canvas_label as (
                select add_museum_informations.*, canvas_size.label
                from add_museum_informations
                join
                canvas_size
                on (add_museum_informations.size_id = canvas_size.size_id)
            )

        select painting_name, sale_price, full_name,
        museum_name, city, label from add_canvas_label;
    """
    )
)


# ------------- Query 20 -------------
# Which country has the 5th highest no of paintings ?

# We calculate which paint is the cheapest and which is the most expensive,
# then add the necessary information as we go along.
query20 = conn.execute(
    text(
        """
        with
            museum_with_country as (
            select w.*, m.country from work as w
            join museum as m
            on (w.museum_id = m.museum_id)),

            cnt_museum_by_country as (
            select country,
                count(name) as cnt_painting, rank() over(order by count(name)desc) as rnk
                from museum_with_country
                group by country)

        select * from cnt_museum_by_country
        where rnk = 5;
"""
    )
)

# ------------- Query 21 -------------
# Which are the 3 most popular and 3 least popular painting styles ?

# We calculate the 3 most popular and the 3 least popular painting styles,
# then we join the results together
query21 = conn.execute(
    text(
        """
        select least_popular.* from (select style, count(style) as cnt_style, 'least popular' as popularity from work
        where style <> ''
        group by style
        order by cnt_style asc
        limit 3) as least_popular

        union

        select most_popular.* from (select style, count(style) as cnt_style, 'most popular' as popularity from work
        where style <> ''
        group by style
        order by cnt_style desc
        limit 3) as most_popular;
    """
    )
)

# ------------- Query 22 -------------
# Which artist has the most no of Portraits paintings outside USA ?
# Display artist name, no of paintings and the artist nationality.

# All information about the paintings (artist name, museum name,
# work name, etc.) is gathered in a single table.
# Then, from thistable, we calculate the number of portraits paintings per artist outside the USA.
# Finally, we keep the one with the most.
query22 = conn.execute(
    text(
        """
        with

            painting_information as (
                select distinct a.artist_id, a.full_name, a.nationality,
                w.work_id, w.museum_id, w.name,
                m.country, s.subject

                from artist a
                join
                work w
                on (a.artist_id = w.artist_id)
                join subject s
                on (w.work_id = s.work_id)
                join
                museum m
                on (w.museum_id = m.museum_id)),

            cnt_by_artist_and_country as (select full_name, nationality, country,
                count(*) over(partition by artist_id) as cnt from painting_information
                where country <> 'USA'
                and subject = 'Portraits'),

            cnt_outside_usa as (select full_name, nationality, cnt,
                rank() over(order by cnt desc) as rnk from cnt_by_artist_and_country)

        select distinct full_name, nationality, cnt as most_portrait_painting_outside_usa
        from cnt_outside_usa
        where rnk = 1;
        """
    )
)
