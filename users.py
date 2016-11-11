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
