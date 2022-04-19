import sys
import psycopg2

# define any local helper functions here

# set up some globals

usage = "Usage: q3.py 'MovieTitlePattern' [Year]"
db = None

# process command-line args

argc = len(sys.argv)
if argc < 2 or (argc > 2 and not sys.argv[2].isnumeric()):
    print(usage)
    sys.exit()

movie = sys.argv[1]
movieYear = sys.argv[2] if argc > 2 else None

# manipulate database

try:
    db = psycopg2.connect("dbname=imdb")
    cursor = db.cursor()
    movies = None
    if movieYear is None:
        query = """
                select m.rating, m.title, m.start_year, m.id from movies m
                where m.title ~* %s
                order by m.rating desc, m.start_year asc, m.title asc
                """
        cursor.execute(query, [movie])
        movies = cursor.fetchall()
    else:
        query = """
                select m.rating, m.title, m.start_year, m.id from movies m
                where m.title ~* %s and m.start_year = %s
                order by m.rating desc, m.start_year asc, m.title asc
                """
        cursor.execute(query, [movie, movieYear])
        movies = cursor.fetchall()
    if len(movies) == 0:
        print(f"No movie matching '{movie}'{f' {movieYear}' if movieYear is not None else ''}")
        sys.exit()
    elif len(movies) == 1:
        print(f"{movies[0][1]} ({movies[0][2]})")
        print("===============")
        query = """
                select n.name, ar.played from names n
                join acting_roles ar on ar.name_id = n.id
                join principals p on p.name_id = n.id
                where p.movie_id = %s and ar.movie_id = p.movie_id
                order by p.ordering, ar.played
                """
        cursor.execute(query, [movies[0][3]])
        actors = cursor.fetchall()
        print("Starring")
        for tuple in actors:
            print(f" {tuple[0]} as {tuple[1]}")

        query = """
                select n.name, cr.role from names n
                join crew_roles cr on cr.name_id = n.id
                join principals p on p.name_id = n.id
                where p.movie_id = %s and cr.movie_id = p.movie_id
                order by p.ordering, cr.role
                """
        cursor.execute(query, [movies[0][3]])
        actors = cursor.fetchall()
        print("and with")
        for tuple in actors:
            print(f" {tuple[0]}: {tuple[1].title()}")
    else:
        print(f"Movies matching '{movie}'{f' {movieYear}' if movieYear is not None else ''}")
        print("===============")
        for tuple in movies:
            print(f"{str(tuple[0])} {tuple[1]} ({tuple[2]})")
except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()
