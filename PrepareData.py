import psycopg2
import json
import sys

# datetime for time outputs as you go along because big file
from datetime import datetime

from configparser import ConfigParser

# ----------------------------------------------------------------------------------------------------
# This is a modified version of the data preparation logic from the original tutorial:
# https://pythonprogramming.net/chatbot-deep-learning-python-tensorflow/
#
# The original logic processed the data in blocks of 1000 records, but resulted in a low match ratio.
# This is because it looks-up matching parent records from the DB, but the records in the batch of 1000
# have not been stored yet in the DB.
# ----------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------
# Initialize variables
# ----------------------------------------------------------------------------------------------------

# Code to indicate a missing/null value
NULL_VAL = -999

# Minimum score to filter out comments
MIN_SCORE = -100

# If True, will remove all existing DB records at the start
DELETE_EXISTING_RECORDS = False

# Interval to log the number of records being processed
LOG_INTERVAL = 10000

# If True, will log each DB transaction (used for debugging)
LOG_DB_TRANS = False

# ----------------------------------------------------------------------------------------------------
# Initialize database connection
# ----------------------------------------------------------------------------------------------------


def config(filename='db.cfg', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        db_params = parser.items(section)
        for param in db_params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


params = config()

print('Connecting to the PostgreSQL database...')
connection = psycopg2.connect(**params)
c = connection.cursor()


# ----------------------------------------------------------------------------------------------------
# Database functions
# ----------------------------------------------------------------------------------------------------

def create_table():
    c.execute(
        """CREATE TABLE IF NOT EXISTS replies (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT,
            comment TEXT, subreddit TEXT,unix INT, score INT)""")
    connection.commit()

    if DELETE_EXISTING_RECORDS:
        c.execute("""TRUNCATE TABLE replies""")  # Clear any existing records


def execute_sql(sql):
    try:
        c.execute(sql)
        connection.commit()
    except Exception as e:
        print('SQL Error', str(e))


def sql_replace_comment(commentid, parentid, comment, subreddit, time, score):
    sql = """UPDATE replies SET comment_id = '{}', comment = '{}', subreddit = '{}', unix = {}, score = {}
            WHERE parent_id ='{}';""".format(
        commentid, comment, subreddit, int(time), score, parentid)
    execute_sql(sql)


def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score):
    sql = """INSERT INTO replies (parent_id, comment_id, parent, comment, subreddit, unix, score)
            VALUES ('{}','{}','{}','{}','{}',{},{});""".format(
        parentid, commentid, parent, comment, subreddit, int(time), score)
    execute_sql(sql)


def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    sql = """INSERT INTO replies (parent_id, comment_id, comment, subreddit, unix, score)
            VALUES ('{}','{}','{}','{}',{},{});""".format(
        parentid, commentid, comment, subreddit, int(time), score)
    execute_sql(sql)


def get_existing_comment_data(cid):
    try:
        sql = "SELECT comment FROM replies WHERE comment_id = '{}' LIMIT 1".format(cid)
        c.execute(sql)
        result = c.fetchone()
        if result is not None:
            return format_data(result[0])
        else:
            return NULL_VAL
    except Exception as e:
        print(str(e))
        return NULL_VAL


def find_existing_score_for_parent(pid):
    try:
        sql = "SELECT score FROM replies WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result is not None:
            return result[0]
        else:
            return NULL_VAL
    except Exception as e:
        print(str(e))
        return NULL_VAL


# ----------------------------------------------------------------------------------------------------
# Data formatting and validation functions
# ----------------------------------------------------------------------------------------------------

# Replace special characters
def format_data(data):
    # getting rid of symbol \n which symbolizes new line, and \r for return, and double quotes for single quotes
    # data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    data = data.replace('\n', '').replace('\r', '').replace('"', "'").replace("'", "''")
    return data


# Check if the comment content is acceptable
def acceptable(data):
    # Skip if >1000 words, or empty
    if len(data.split(' ')) > 1000 or len(data) < 1:
        return False
    # Skip if >32000 characters
    elif len(data) > 32000:
        return False
    # Skip deleted comments
    elif data == '[deleted]':
        return False
    # Skip removed comments
    elif data == '[removed]':
        return False
    else:
        return True


def log_data(transaction, row_counter, parent_id, comment_id, score):
    if LOG_DB_TRANS:
        print(transaction, row_counter, parent_id, comment_id, score)

# ----------------------------------------------------------------------------------------------------
# File processing function
# ----------------------------------------------------------------------------------------------------


def process_file():
    row_counter = 0
    paired_rows = 0
    with open('testdata.txt', encoding="utf8") as f:
        for row in f:
            row_counter = row_counter + 1
            try:
                data = json.loads(row)

                parent_id = data['parent_id'].split('_')[1]
                body = format_data(data['body'])
                created_utc = data['created_utc']
                score = data['score']
                comment_id = data['id']
                subreddit = data['subreddit']

                if score >= MIN_SCORE:  # Filter out comments below our min score threshold
                    if acceptable(body):  # Filter out comments that are not acceptable

                        parent_comment = get_existing_comment_data(parent_id)

                        # Update parent if we have a better score than existing comment
                        existing_comment_score = find_existing_score_for_parent(parent_id)
                        if existing_comment_score != NULL_VAL:
                            if score > existing_comment_score:
                                log_data("RPL", row_counter, parent_id, comment_id, score)
                                sql_replace_comment(comment_id, parent_id, body, subreddit, created_utc, score)

                        else:
                            if parent_comment != NULL_VAL:
                                log_data("INS_P", row_counter, parent_id, comment_id, score)
                                sql_insert_has_parent(comment_id, parent_id, parent_comment, body, subreddit,
                                                      created_utc, score)
                                paired_rows += 1
                            else:
                                log_data("INS_NP", row_counter, parent_id, comment_id, score)
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

                if row_counter % LOG_INTERVAL == 0:
                    print('Rows Read: {}, Paired: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))
                    sys.stdout.flush()  # Do not buffer log lines, show them one by one

            except Exception as e:
                print(str(e))

        print('Total Rows Read: {}, Paired: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))


# ----------------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    create_table()
    process_file()
