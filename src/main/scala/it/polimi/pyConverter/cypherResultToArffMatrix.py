import csv

FILE_PATH = "/Users/abernasconi/Downloads/"
IN_ACT = "att.csv"
OUT = "matrix.arff"

IN_MOV = "mov.csv"
#OUT_MOV = "film_out.csv"

IN_REL = "rel.csv"

act_list = list()
with open(FILE_PATH + IN_ACT, "r") as actor_in:
    with open(FILE_PATH + OUT, "w") as arff_out:
        next(actor_in)
        arff_out.write("@relation attori\n")
        for line in csv.reader(actor_in):
            actor = line[0]
            # memorize columns
            act_list.append(actor)
            # print header of file arff
            arff_out.write("@attribute \'" + actor.replace("'","-") + "\' { t}\n")
        arff_out.write("@data\n")

#print act_list
#print len(act_list)
num_actors = len(act_list)

#mov_list = list()
with open(FILE_PATH + IN_MOV, "r") as movie_in:
    with open(FILE_PATH + OUT, "a") as arff_out:
        next(movie_in)
        # for every movie found in file of movies
        for line in csv.reader(movie_in):
            movie = line[0]
            #print movie
            #mov_list.append(movie)
            movie_row = ['?'] * num_actors
            #print movie_row
            with open(FILE_PATH + IN_REL, "r") as rel_in:
                next(rel_in)
                for liner in csv.reader(rel_in):
                    movie_in_relation = liner[0]
                    actor_in_relation = liner[1]
                    if movie_in_relation == movie:
                        actor_index = act_list.index(actor_in_relation)
                        movie_row[actor_index] = 't'
            arff_out.write(','.join(movie_row) + "\n")



        # print separated y commas (join)
