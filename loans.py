import sys
import os

# define logging helpers
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(os.path.join(os.path.dirname(__file__),
                    "./lib/python2.7/site-packages"))

import pg8000
import datetime
import simplejson as json

# set pramstyle to numeric
pg8000.paramstyle = "numeric"

LOANS_TABLE = "loans"
LOANS_STATUS_COL = "loan_status"
LOANS_STATUS_VAL_BIDDING = "'bidding'"

# sort conditions
LOAN_SORT_SECURITY = "security DESC"
LOAN_SORT_WEIGHT = "sort_weight DESC"
LOAN_SORT_FUNDED_PERCENTAGE = "funded_percentage_cache DESC"

def get_live_loans(event, context):
    # get db connection object
    conn = get_db();
    curr = conn.cursor()
    sql_cols = ["id",
                "loan_status",
                "loan_id_out",
                "grade",
                "interest_rate_out",
                "target_amount_out",
                "funding_amount_to_complete_cache",
                "tenure_out",
                "security",
	            "collateral_out",
	            "collateral_description_out",
	            "funding_duration",
	            "funding_start_date",
	            "funding_end_date",
	            "funded_percentage_cache",
	            "sort_weight"]

    sql_sorts = [LOAN_SORT_SECURITY,
                LOAN_SORT_WEIGHT,
                LOAN_SORT_FUNDED_PERCENTAGE]

    sql_str = read(LOANS_TABLE,
                sql_cols,
                sql_sorts,
                **{LOANS_STATUS_COL : LOANS_STATUS_VAL_BIDDING})
    try:
        curr.execute(sql_str)
    except:
        logger.error("Error: database query error")
        raise Exception("Error: database query error")

    response = {
         "statusCode": 200,
         "body":  json.dumps(curr.fetchall(), default=date_handler)
    }
    curr.close() #close connection *important
    return response

def get_loan_details(event, context):

    if not event['id'] or not event['loan_id']:
        logger.error("Error: params unidentified")
        raise Exception("Error: params unidentified")

    conn = get_db();
    curr = conn.cursor()

    sql_cols = ["id",
                "loan_status",
                "loan_id_out",
                "grade",
                "interest_rate_out",
                "target_amount_out",
                "funding_amount_to_complete_cache",
                "tenure_out",
                "security",
	            "collateral_out",
	            "collateral_description_out",
	            "funding_duration",
	            "funding_start_date",
	            "funding_end_date",
	            "funded_percentage_cache",
	            "sort_weight"]

    sql_str = read(LOANS_TABLE,
                sql_cols,
                None,
                **{LOANS_STATUS_COL : LOANS_STATUS_VAL_BIDDING,
                'id' : ':1',
                'loan_id_out' : ':2'})

    try:
        curr.execute(sql_str, (event['id'], str(event['loan_id']),))
    except Exception as e:
        logger.error("Error: database query error", e.message, e.args)
        raise Exception("Error: database query error")

    response = {
         "statusCode": 200,
         "body":  json.dumps(curr.fetchone(), default=date_handler)
    }
    curr.close()
    return response

def get_db():
    try:
        conn = pg8000.connect(database="lending_platform_development",
        user="postgres",
        host="p2p-dev-ryan.cijpvborulmv.ap-southeast-1.rds.amazonaws.com",
        password="devpassword")
    except Exception as e:
        logger.error('Error: connecting to database failed.', e.message, e.args)
        raise Exception('Error: database connection failed.')
    return conn


def read(table, cols, orderby, **kwargs):
    """ Generates SQL for a SELECT statement matching the kwargs passed. """
    sql = list()
    cols_str = ", ".join(cols)
    sql.append("SELECT %s FROM %s " % (cols_str, table))
    if kwargs:
        sql.append("WHERE " + " AND ".join("%s = %s" % (k, v) for k, v in kwargs.iteritems()))
    if orderby is not None:
        sql.append("ORDER BY " + ", ".join("%s" % v for v in orderby))
    sql.append(";")
    return "".join(sql)


# def upsert(table, **kwargs):
#     """ update/insert rows into objects table (update if the row already exists)
#         given the key-value pairs in kwargs """
#     keys = ["%s" % k for k in kwargs]
#     values = ["'%s'" % v for v in kwargs.values()]
#     sql = list()
#     sql.append("INSERT INTO %s (" % table)
#     sql.append(", ".join(keys))
#     sql.append(") VALUES (")
#     sql.append(", ".join(values))
#     sql.append(") ON DUPLICATE KEY UPDATE ")
#     sql.append(", ".join("%s = '%s'" % (k, v) for k, v in kwargs.iteritems()))
#     sql.append(";")
#     return "".join(sql)
#
#
# def delete(table, **kwargs):
#     """ deletes rows from table where **kwargs match """
#     sql = list()
#     sql.append("DELETE FROM %s " % table)
#     sql.append("WHERE " + " AND ".join("%s = '%s'" % (k, v)
#                     for k, v in kwargs.iteritems()))
#     sql.append(";")
#     return "".join(sql)

def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        raise TypeError

if __name__ == "__main__":
    #test functions
    print get_live_loans(None, None)
    event = {'id' : 2679,
    'loan_id' : 'CWD-010890002'}
    print get_loan_details(event, None)
