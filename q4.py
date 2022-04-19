import sys
import psycopg2

# define any local helper functions here

# set up some globals

usage = "Usage: q4.py 'NamePattern' [Year]"
db = None

# process command-line args

argc = len(sys.argv)
if argc < 2 or (argc > 2 and not sys.argv[2].isnumeric()):
    print(usage)
    sys.exit()

name = sys.argv[1]
birthYear = sys.argv[2] if argc > 2 else None

# manipulate database

try:
    db = psycopg2.connect("dbname=imdb")
    cursor = db.cursor()
    people = None
    if birthYear is None:
        query = """
                select n.id, n.name, n.birth_year, n.death_year from names n
                where n.name ~* %s
                order by n.name, n.birth_year, n.id 
                """
        cursor.execute(query, [name])
        people = cursor.fetchall()
    else:
        query = """
                select n.id, n.name, n.birth_year, n.death_year from names n
                where n.name ~* %s and n.birth_year = %s
                order by n.name, n.birth_year, n.id 
                """
        cursor.execute(query, [name, birthYear])
        people = cursor.fetchall()
    if len(people) == 0:
        print(f"No name matching '{name}'{f' {birthYear}' if birthYear is not None else ''}")
        sys.exit()
    elif len(people) == 1:
        output = f"Filmography for {people[0][1]}"
        if people[0][2] is None:
            output += " (???)"
        else:
            output += f" ({people[0][2]}-{f'{people[0][3]})' if people[0][3] is not None else ')'}"
        print(output)
        print("===============")
        query = """
                select avg(m.rating) from principals p
                join movies m on m.id = p.movie_id
                where p.name_id = %s
                """
        cursor.execute(query, [people[0][0]])
        personalRating = cursor.fetchall()
        print(f"Personal Rating: {round(personalRating[0][0], 1) if personalRating[0][0] is not None else 0}")

        query = """
                select mg.genre from movie_genres mg
                join movies m on m.id = mg.movie_id
                join principals p on m.id = p.movie_id
                where p.name_id = %s
                group by mg.genre
                order by count(*) desc, mg.genre
                limit 3
                """
        cursor.execute(query, [people[0][0]])
        genres = cursor.fetchall()
        print("Top 3 Genres:")
        for genre in genres:
            print(f" {genre[0]}")
        print("===============")
        query = """
                select m.title, m.start_year, m.id from movies m
                join principals p on m.id = p.movie_id
                where p.name_id = %s
                order by m.start_year, m.title
                """
        cursor.execute(query, [people[0][0]])
        movies = cursor.fetchall()
        for movie in movies:
            print(f"{movie[0]} ({movie[1]})")
            query = """
                    select played from acting_roles
                    where name_id = %s and movie_id = %s
                    order by played
                    """
            cursor.execute(query, [people[0][0], movie[2]])
            actingRoles = cursor.fetchall()
            for actingRole in actingRoles:
                print(f" playing {actingRole[0]}")

            query = """
                    select role from crew_roles 
                    where name_id = %s and movie_id = %s
                    order by role 
                    """
            cursor.execute(query, [people[0][0], movie[2]])
            crewRoles = cursor.fetchall()
            for crewRole in crewRoles:
                print(f" as {' '.join((crewRole[0][0].upper() + crewRole[0][1:]).split('_'))}")
    else:
        print(f"Names matching '{name}'{f' {birthYear}' if birthYear is not None else ''}")
        print("===============")
        for tuple in people:
            output = tuple[1]
            if tuple[2] is None:
                output += " (???)"
            else:
                output += f" ({tuple[2]}-{f'{tuple[3]})' if tuple[3] is not None else ')'}"
            print(output)
except psycopg2.Error as err:
	print("DB error: ", err)
finally:
	if db:
		db.close()

