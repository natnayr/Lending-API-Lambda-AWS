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

REPAYMENT_SCHEDULE_TABLE = "repayment_schedule_ins"


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
                "loan_id_out AS loan_id",
                "grade",
                "currency_out AS currency",
                "interest_rate_out AS interest_rate",
                "target_amount_out AS target_amount",
                "tenure_out AS tenure",
                "frequency_out AS frequency",
                "security",
	            "collateral_out AS collateral",
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
                None,
                sql_sorts,
                **{LOANS_STATUS_COL : LOANS_STATUS_VAL_BIDDING})
    try:
        curr.execute(sql_str)
    except:
        logger.error("Error: database query error")
        raise Exception("Error: database query error")

    try:
        description = [column[0] for column in curr.description]
        result = []
        for row in curr.fetchall():
            for i, x in enumerate(row):
                if isinstance(x, datetime.date):
                    row[i] = x.__str__()
            result.append(dict(zip(description, row)))
    except:
        logger.error("Error: sorting and string converting error")
        raise Exception("Error: sorting and string converting error")

    # response = json.dumps(result, default=date_handler)
    response = result

    curr.close() #close connection *important
    return response

def get_loan_details(event, context):

    if not event['loan_id'] or len(event) != 1:
        logger.error("Error: params incorrect")
        raise Exception("Error: params incorrect")

    conn = get_db();
    curr = conn.cursor()

    sql_cols = ["lns.id",
                "lns.loan_status",
                "lns.loan_id_out AS loan_id",
                "lns.grade",
                "lns.currency_out AS currency",
                "lns.interest_rate_out AS interest_rate",
                "lns.target_amount_out AS target_amount",
                "lns.funding_amount_to_complete_cache",
                "lns.frequency_out AS frequency",
                "lns.tenure_out AS tenure",
                "lns.security",
	            "lns.collateral_out AS collateral",
	            "lns.collateral_description_out AS collateral_description",
	            "lns.funding_duration",
	            "lns.funding_start_date",
	            "lns.funding_end_date",
	            "lns.funded_percentage_cache",
                "lns.start_date_out AS start_date",
                "lns.loan_type",
	            "lns.sort_weight",
                "min(rps.expected_date) AS first_repayment",
                "max(rps.expected_date) as last_repayment"]

    table_sql = (REPAYMENT_SCHEDULE_TABLE +" rps INNER JOIN " +
                    LOANS_TABLE + " lns ON lns.id = rps.loan_id")

    sql_group_by = ["lns.id"]

    sql_str = read(table_sql,
                sql_cols,
                sql_group_by,
                None,
                **{LOANS_STATUS_COL : LOANS_STATUS_VAL_BIDDING,
                'lns.loan_id_out' : ':1 '})


    try:
        curr.execute(sql_str, (str(event['loan_id']),))
    except Exception as e:
        logger.error("Error: database query error", e.message, e.args)
        raise Exception("Error: database query error")

    try:
        description = [column[0] for column in curr.description]
        row = curr.fetchone()
        for i, x in enumerate(row):
            if isinstance(x, datetime.date):
                row[i] = x.__str__()
        result = dict(zip(description, row))
    except Exception as e:
        logger.error("Error: sorting error", e.message, e.args)
        raise Exception("Error: sorting error")

    response = result



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


def read(table, cols, groupby, orderby, **kwargs):
    """ Generates SQL for a SELECT statement matching the kwargs passed. """
    sql = list()
    cols_str = ", ".join(cols)
    sql.append("SELECT %s FROM %s " % (cols_str, table))
    if kwargs:
        sql.append("WHERE " + " AND ".join("%s = %s" % (k, v) for k, v in kwargs.iteritems()))
    if groupby is not None:
        sql.append("GROUP BY " + ", ".join("%s" % v for v in groupby))
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

# Date Converter to string
# def date_handler(obj):
#     if isinstance(obj, datetime.date):
#             return obj.__str__()

if __name__ == "__main__":
    #test functions
    print get_live_loans(None, None)
    event = {'loan_id' : 'CWD0013286'}
    print get_loan_details(event, None)
