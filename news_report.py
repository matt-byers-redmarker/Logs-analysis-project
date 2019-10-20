#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import psycopg2
import datetime

DBNAME = "news"

articles_query = """
select
path,
count(*),
articles.title
from log, articles
where status = '200 OK'
and path != '/'
and path like '%' || articles.slug || '%'
group by path, articles.title
order by count(*) desc
limit 3;"""

authors_query = """
select
authors.name as author,
count(*) as total_views
from log, articles, authors
where status = '200 OK'
and path != '/'
and path like '%' || articles.slug || '%'
and articles.author = authors.id
group by authors.name
order by total_views desc;
"""

errors_query = """
select *
from daily_error_rate
where rate >=1;"""


def get_popular_articles():
    # Returns the most popular three articles of all time
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(articles_query)
    question_1 = c.fetchall()
    print("Q: What are the most popular three articles of all time?\n")
    for row in question_1:
        print('{} — {} views'.format(row[2], row[1]))
    db.close()


def get_popular_authors():
    # Returns the most popular three articles of all time
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(authors_query)
    question_2 = c.fetchall()
    print("\nQ: Who are the most popular article authors of all time?\n")
    for row in question_2:
        print('{} — {} views'.format(row[0], row[1]))
    db.close()


def get_errors():
    # Returns the most popular three articles of all time
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(errors_query)
    question_3 = c.fetchall()
    print("\nQ: On which day/s did more than 1% of requests lead to errors?\n")
    for row in question_3:
        print('{0:%B} {0:%d}, — '.format(row[0]) + '{} errors'.format(row[3]))
    db.close()


# Run the functions
get_popular_articles()
get_popular_authors()
get_errors()
