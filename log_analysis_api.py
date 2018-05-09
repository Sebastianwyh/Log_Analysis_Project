#!/usr/bin/env python

import psycopg2

DBNAME = "news"


def run_query(query):
    """Connects to the database, runs the query passed to it,
    and returns the results"""
    db = psycopg2.connect('dbname=' + DBNAME)
    c = db.cursor()
    c.execute(query)
    rows = c.fetchall()
    db.close()
    return rows


# Build Query1
query1 = "select title,count(title) as views from articles,log "\
         "where log.path = concat('/article/', slug) " \
         "group by title order by views desc limit 3;"

# Build Query2
query2 = "select authors.name, count(*) as views from articles " \
         "inner join authors on articles.author = authors.id " \
         "inner join " \
         "log on concat('/article/', articles.slug) = log.path " \
         "group by authors.name order by views desc;"

# Build Query3
query3 = "select * from " \
         "(select x.day, " \
         "round(cast((y.times*100) " \
         "as numeric)/ cast(x.times as numeric), 2) " \
         "as errorpercent from (select date(time) " \
         "as day, count(*) " \
         "as times from log group by day)" \
         " as x inner join (select date(time) as day, count(*) " \
         "as times from log " \
         "where status like '%404%' group by day) " \
         "as y on x.day = y.day) as z " \
         "where errorpercent > 1.0;"


'''Print most read three articles'''


def three_top_articles():
    # run the query1
    results = run_query(query1)
    # print title
    print('----Most Popular Three Articles Of All Time----')
    for i in results:
        title = i[0]
        views = "----" + str(i[1]) + " views"
        print(title + views)


'''Print the most popular ahtuor'''


def most_popular_authors():
    # run the query2
    results = run_query(query2)
    # print title
    print('\n----The Most Popular Author Of All Time----')
    for i in results:
        title = i[0]
        views = "----" + str(i[1]) + " views"
        print(title + views)


'''Print the days with most percent of errors'''


def days_more_than_1_errors():
    # run the query3
    results = run_query(query3)
    # print title
    print('\n----These Days Got More Than 1% Errors----')
    for i in results:
        errors = str(i[0]) + ' ' + str(i[1]) + "%" + " errors"
        print(errors)


# Print while running the program
print('Wait a moment...\n')
three_top_articles()
most_popular_authors()
days_more_than_1_errors()
