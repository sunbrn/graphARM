import re
import time
import sys

FILE_PATH = sys.argv[1]
#FILE_PATH = "/Users/abernasconi/Documents/neo4j-community-3.4.1/import/original/breve/"
#FILE_PATH = "/Users/abernasconi/Documents/neo4j-community-3.4.1/import/small_example/"
#FILE_PATH = "/Users/abernasconi/Documents/neo4j-community-3.4.1/import/original/"

flag_title = sys.argv[2]
TITLE_FILE_NAME = sys.argv[3]
TITLE_HEADER = sys.argv[4]
#flag_title = False
#TITLE_FILE_NAME = "title.basics"
#TITLE_FILE_NAME = "title.basics_breve"
#TITLE_HEADER = "tconst:ID,:LABEL,primaryTitle,originalTitle,isAdult:int,startYear,endYear,runtimeMinutes,:LABEL\n"

flag_name = sys.argv[5]
NAME_FILE_NAME = sys.argv[6]
NAME_HEADER = sys.argv[7]
#flag_name = False
#NAME_FILE_NAME = "name.basics"
#NAME_FILE_NAME = "name.basics_breve"
#NAME_HEADER = "nconst:ID,primaryName,birthYear,deathYear,:LABEL\n"

flag_role = sys.argv[8]
ROLE_FILE_NAME = sys.argv[9]
ROLE_HEADER = sys.argv[10]
#flag_role = False
#ROLE_FILE_NAME = "title.principals"
#ROLE_FILE_NAME = "title.principals_breve"
#ROLE_HEADER = ":START_ID,ordering:int,:END_ID,:TYPE,job,characters:string[]\n"

num_cost = sys.argv[11]
#num_cost = 100000




def str_to_bool(s):
    if s == 'True':
        return True
    elif s == 'False':
        return False
    else:
        raise ValueError # evil ValueError that doesn't tell you what the wrong value was





if str_to_bool(flag_title):
    start_time_title = time.time()
    print "Starting conversion for " + TITLE_FILE_NAME + "..."
    with open(FILE_PATH + TITLE_FILE_NAME + ".csv", "r") as f:
        with open(FILE_PATH + TITLE_FILE_NAME + "_after.csv", "w") as f_after:
            f_after.write(TITLE_HEADER+"\n")
            next(f)
            for num, line in enumerate(f, 1):
                [tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes,
                 genres] = line.split("\t")
                primaryTitle = "\"" + re.sub("\"", "'", primaryTitle.rstrip()) + "\""
                originalTitle = "\"" + re.sub("\"", "'", originalTitle.rstrip()) + "\""
                genres = re.sub(",", ";", genres.rstrip())
                newLine = [tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes,
                           genres]
                if num % int(num_cost) == 0:
                    print(num)
                f_after.write(','.join(newLine) + "\n")
    print "{} {} {}".format("Elapsed time for " + TITLE_FILE_NAME + " conversion: ", time.time() - start_time_title, "sec")



if str_to_bool(flag_name):
    start_time_name = time.time()
    print "Starting conversion for " + NAME_FILE_NAME + "..."
    with open(FILE_PATH + NAME_FILE_NAME + ".csv", "r") as f:
        with open(FILE_PATH + NAME_FILE_NAME + "_after.csv", "w") as f_after:
            f_after.write(NAME_HEADER+"\n")
            next(f)
            for num, line in enumerate(f, 1):
                try:
                    [nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles] = line.split("\t")
                    primaryName = re.sub("\"", "'", primaryName.rstrip())
                    primaryName = re.sub(",", "-", primaryName.rstrip())
                    primaryProfession = re.sub(",", ";", primaryProfession.rstrip())
                    newLine = [nconst, primaryName, birthYear, deathYear, primaryProfession]
                    if num % int(num_cost) == 0:
                        print(num)
                    f_after.write(','.join(newLine) + "\n")
                except ValueError as e:
                    print "Value error({0})".format(e.message)

    print "{} {} {}".format("Elapsed time for " + NAME_FILE_NAME + " conversion: ", time.time() - start_time_name, "sec")

if str_to_bool(flag_role):
    start_time_role = time.time()
    print "Starting conversion for " + ROLE_FILE_NAME + "..."
    with open(FILE_PATH + ROLE_FILE_NAME + ".csv", "r") as f:
        with open(FILE_PATH + ROLE_FILE_NAME + "_after.csv", "w") as f_after:
            f_after.write(ROLE_HEADER+"\n")
            next(f)
            for num, line in enumerate(f, 1):
                [tconst, ordering, nconst, category, job, characters] = line.split("\t")
                job = re.sub(",", "-", job.rstrip())
                job = re.sub("\"", "'", job.rstrip())
                characters = characters[:-1]  # remove useless \n ad the end of each line
                if re.match('\[.*\]', characters):  # in case characters contains values (different from \N)
                    characters = characters[1:-1]  # take away square parenthesis at the borders
                    characters = re.sub("\"", "", characters.rstrip())  # take away double quotes
                    characters = re.sub(",", ";", characters.rstrip())  # substitute , with ;
                    characters = "\"" + characters + "\""  # put characters inside ""
                newLine = [tconst, ordering, nconst, category, job, characters]
                if num % int(num_cost) == 0:
                    print(num)
                f_after.write(','.join(newLine) + "\n")
    print "{} {} {}".format("Elapsed time for " + ROLE_FILE_NAME + " conversion: ", time.time() - start_time_role, "sec")
