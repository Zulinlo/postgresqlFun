import sys
import psycopg2

# define any local helper functions here

# set up some globals

usage = "Usage: q1.py [N]"
db = None

# process command-line args

argc = len(sys.argv)
n = sys.argv[1] if argc > 1 else str(10)

if not n.isnumeric() or int(n) < 1:
    print(usage)
    sys.exit()

# manipulate database

try:
    db = psycopg2.connect("dbname=imdb")
    cursor = db.cursor()
    query = """
            select count(*) as movies_directed, n.name from crew_roles cr
            join movies m on m.id = cr.movie_id
            join names n on n.id = cr.name_id
            where cr.role = 'director'
            group by n.id
            order by count(*) desc, n.name asc 
            limit %s
            """
    cursor.execute(query, [n])
    for tuple in cursor.fetchall():
        print(str(tuple[0]) + " " + tuple[1])
except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()
