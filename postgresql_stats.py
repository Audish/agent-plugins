#!/usr/bin/env python
#
# Cloudkick plugin for monitoring PostgreSQL server status.
#
# Author: Yaniv Aknin, largely based on Steve Hoffmann's version
#
# Requirements:
# - Python PostgreSQL adapter (http://initd.org/psycopg/)
#

import sys
import warnings
import psycopg2
import argparse

warnings.filterwarnings('ignore', category = DeprecationWarning)

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', default='postgres')
    parser.add_argument('-H', '--host')
    parser.add_argument('-p', '--password')
    parser.add_argument('db')
    options = parser.parse_args(argv[1:])
    metrics = retrieve_metrics(options.db, options.user, options.host, options.password)
    print_metrics(metrics)

def open_db(database, user, host = None, password = None):
  dsn = "dbname=%s user=%s" % (database, user)

  if host:
    dsn += ' host=%s' % (host)

  if password:
    dsn += ' password=%s' % (password)

  try:
    return psycopg2.connect(dsn)
  except psycopg2.OperationalError, e:
    print 'status err %s' % (e.message[:48].strip())
    sys.exit(1)

def retrieve_metrics(database, user, host, password):
  conn = open_db(database, user, host, password)
  cur = conn.cursor()

  stats = dict()

  cur.execute("SELECT count(1) FROM pg_stat_activity WHERE datname='%s'" % (database,));
  for row in cur:
     stats['conns'] = ('int', row[0])

  cur.execute("SELECT count(1) FROM pg_stat_activity WHERE current_query != '<IDLE>' and datname='%s'" % (database,))
  for row in cur:
     stats['active_queries'] = ('int', row[0])

  cur.execute("SELECT count(1) FROM pg_stat_activity WHERE waiting=true and datname='%s'" % (database,))
  for row in cur:
     stats['waiting_queries'] = ('int', row[0])

  cur.execute("SELECT checkpoints_timed, checkpoints_req, buffers_alloc FROM pg_stat_bgwriter")
  row = cur.fetchone()
  stats['expected_checkpoints'] = ('gauge', row[0])
  stats['actual_checkpoints'] = ('gauge', row[1])
  stats['buffers_alloc'] = ('gauge', row[2])

  int_cols = "xact_commit", "xact_rollback", "blks_read", "tup_fetched","tup_inserted", "tup_updated", \
             "tup_deleted"
  cur.execute("SELECT " + ', ' . join(int_cols) + ", (blks_read - blks_hit) / (blks_read+0.000001)"
              " AS blk_miss_pct FROM pg_stat_database WHERE datname='%s'" % (database,))

  for row in cur:
     colno = 0
     for key in int_cols:
       stats[key] = ('gauge', row[colno])
       colno += 1
     if 0 <= row[colno] <= 1:
       stats['blk_miss_pct'] = ('float', row[colno])

  cur.close()
  conn.close()

  return stats

def print_metrics(metrics):
  print "status ok postgresql_stats success"
  for (key, stat) in metrics.iteritems():
     print "metric %s %s %s" % (key, stat[0], stat[1])


if __name__ == '__main__':
    main(sys.argv)
