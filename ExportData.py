import psycopg2
from configparser import ConfigParser

# ----------------------------------------------------------------------------------------------------
# Exports the data from the specified tables and splits the content into corresponding file:
# - {table}.from: sequential array of "parent" comments
# - {table}.to: sequential array of "response" comments
# ----------------------------------------------------------------------------------------------------


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

curs = connection.cursor()

# ----------------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------------


# Target output folder
folder = "data/"

# Do not append data, overwrite existing files
file_mode = 'w'

# Array of tables for export
tables = ['replies_2019_08_5M']

# Filter comments that have >= min_score
min_score = 1

# Log the progress every {log_interval} rows processed
log_interval = 100000

for table in tables:

    print("Exporting table [{}]".format(table))

    counter = 0

    curs.execute("""SELECT parent,comment FROM {} WHERE parent is NOT NULL and score >= {}""".format(table, min_score))

    with open(folder + '{}.from'.format(table), file_mode, encoding='utf8') as f:
        with open(folder + '{}.to'.format(table), file_mode, encoding='utf8') as t:
            for row in curs:
                counter += 1
                f.write(row[0]+'\n')
                t.write(row[1]+'\n')
                if counter % log_interval == 0:
                    print(counter, 'rows completed so far')
