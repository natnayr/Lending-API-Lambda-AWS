import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "./lib/python2.7/site-packages"))

import pg8000

def hello(event, context):
    try:
        conn = pg8000.connect(database="lending_platform_development", user="postgres", host="p2p-dev-ryan.cijpvborulmv.ap-southeast-1.rds.amazonaws.com", password="devpassword")
    except:
        print "I am unable to connect to the database"
    curr = conn.cursor()

    try:
        curr.execute("""SELECT id, collateral_in FROM loans LIMIT 100""")
    except:
        print "cant try database.."

    response = {
         "statusCode": 200,
         "body":  json.dumps(curr.fetchall())
    }

    return response


if __name__ == "__main__":
    print hello("", "")
