import sys
import psycopg2

# define any local helper functions here

# set up some globals

usage = "Usage: q2.py 'PartialMovieTitle'"
db = None

# process command-line args

argc = len(sys.argv)
if argc < 2:
    print(usage)
    sys.exit()

movie = sys.argv[1] 

# manipulate database

try:
    db = psycopg2.connect("dbname=imdb")
    cursor = db.cursor()
    query = """
            select m.rating, m.title, m.start_year, m.id from movies m
            where m.title ~* %s
            order by m.rating desc, m.start_year asc, m.title asc
            """
    cursor.execute(query, [movie])
    movies = cursor.fetchall()
    if len(movies) == 0:
        print(f"No movie matching '{movie}'")
        sys.exit()
    elif len(movies) == 1:
        query = """
                select a.local_title, a.region, a.language, a.extra_info from aliases a
                where a.movie_id = %s
                order by a.ordering
                """
        cursor.execute(query, [movies[0][3]])
        aliases = cursor.fetchall()
        print(f"{movies[0][1]} ({movies[0][2]}){' was also released as' if len(aliases) > 0 else ' has no alternative releases'}")
        for tuple in aliases:
            output = f"'{tuple[0]}'"
            if tuple[1] is not None and tuple[2] is not None:
                output += f" (region: {tuple[1].strip()}, language: {tuple[2].strip()})"
            elif tuple[1] is not None:
                output += f" (region: {tuple[1].strip()})"
            elif tuple[2] is not None:
                output += f" (language: {tuple[2].strip()})"
            elif tuple[3] is not None:
                output += f" ({tuple[3].strip()})"
            print(output)
    else:
        print(f"Movies matching '{movie}'")
        print("===============")
        for tuple in movies:
            print(f"{str(tuple[0])} {tuple[1]} ({tuple[2]})")
except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()
