#!/usr/bin/env python
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

DBNAME = "news"

q1 = '''
  SELECT t2.title, t1.page_views
  FROM (
    SELECT
      regexp_split_to_array(path, '/') as path_array,
      array_length(regexp_split_to_array(path, '/'), 1) as path_array_length,
      (regexp_split_to_array(path, '/'))[3] as slug,
      COUNT(id) as page_views
    FROM log
    WHERE array_length(regexp_split_to_array(path, '/'), 1) = 3
    GROUP BY path
  ) t1
  JOIN articles t2
  ON t1.slug = t2.slug
  ORDER BY t1.page_views DESC
  LIMIT 3
'''

q2 = '''
  SELECT t3.name, SUM(t1.page_views) as page_views
  FROM (
    SELECT
      regexp_split_to_array(path, '/') as path_array,
      array_length(regexp_split_to_array(path, '/'), 1) as path_array_length,
      (regexp_split_to_array(path, '/'))[3] as slug,
      COUNT(id) as page_views
    FROM log
    WHERE array_length(regexp_split_to_array(path, '/'), 1) = 3
    GROUP BY path
  ) t1
  JOIN articles t2
  ON t1.slug = t2.slug
  JOIN authors t3
  ON t2.author = t3.id
  GROUP BY t3.name
  ORDER BY SUM(t1.page_views) DESC
  LIMIT 3
'''

q3 = '''
  SELECT TO_CHAR(t1.date, 'Mon DD,YYYY') as date,
    error_count*1.00/request_count as error_rate
  FROM (
    SELECT
      date_trunc('day',time) as date,
      COUNT(status) as request_count,
      SUM(CASE
        WHEN status LIKE '4%' OR status LIKE '5%' THEN 1
        ELSE 0
      END) AS error_count
    FROM log
    GROUP BY 1
  ) t1
  WHERE error_count*1.00/request_count > 0.01
'''


def get_log(q):
    db = psycopg2.connect(dbname=DBNAME)
    # c = db.cursor()
    # c.execute(q)
    # log = c.fetchall()
    log = sqlio.read_sql_query(q, db)
    db.close()
    return log


a1 = get_log(q1)
a2 = get_log(q2)
a3 = get_log(q3)

print('Articles with the most page views:')
for i in range(a1.shape[0]):
    print('>>' + a1.loc[i, 'title'] + ' - ' +
          str(a1.loc[i, 'page_views']) + ' views')

print('\nAuthors with the most page views:')
for i in range(a2.shape[0]):
    print('>>' + a2.loc[i, 'name'] + ' - ' +
          str(a2.loc[i, 'page_views']) + ' views')

print('\nDates with errors more than 1%:')
for i in range(a3.shape[0]):
    print(a3.loc[i, 'date'] + ' ' +
          str(round(a3.loc[i, 'error_rate']*100, 2)) + '%')
