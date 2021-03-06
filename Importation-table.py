### Ce fichier importe les données depuis un format csv dans la base de donnees.
### Importation des donnes en csv
DL_dt = pd.read_csv('C:/Users/Zoé/Documents/ACO/M2/S9/BigData/archive/MostCommonLanguageByDirector.csv')
print(DL_dt.head(3))
print(DL_dt.columns)

Movies_dt = pd.read_csv('C:/Users/Zoé/Documents/ACO/M2/S9/BigData/archive/AllMoviesDetailsCleaned.csv',
                        delimiter=';', skiprows=0, low_memory=False)
Awds_dt = pd.read_csv('C:/Users/Zoé/Documents/ACO/M2/S9/BigData/archive/900_acclaimed_directors_awards.csv',
                      delimiter=';', skiprows=0, low_memory=False)
Aws_by_ctg_dt = pd.read_csv('C:/Users/Zoé/Documents/ACO/M2/S9/BigData/archive/220k_awards_by_directors.csv',
                            delimiter=',', skiprows=0, low_memory=False)
Mv_cast_dt = pd.read_csv('C:/Users/Zoé/Documents/ACO/M2/S9/BigData/archive/AllMoviesCastingRaw.csv',
                         delimiter=';', skiprows=0, low_memory=False)


#### Table DirectorLanguage
for k in range(1000):
    table = dynamodb.Table('DirectorLanguage')
    trans = {}
    trans['Director'] = DL_dt['director_name'][k]
    trans['Language'] = DL_dt['original_language'][k]
    table.put_item(Item=trans)


### Table Awards
for k in range(len(Awds_dt) - 1):
    table = dynamodb.Table('Awards')

    trans = {}

    trans['Director'] = str(Awds_dt['name'][k])
    trans['Total_Awards'] = int(Awds_dt['Total awards'][k])

    table.put_item(Item=trans)


### Table FilmID
for k in range(1000):
    table = dynamodb.Table('FilmID')
    trans = {}
    trans['ID'] = int(Movies_dt['id'][k])
    trans['Title'] = Movies_dt['original_title'][k]
    table.put_item(Item=trans)


### Table Casting
for k in range(1000):
    table = dynamodb.Table('Casting')
    trans = {}
    trans['FilmID'] = int(Mv_cast_dt['id'][k])
    trans['Main_Actor1'] = str(Mv_cast_dt['actor1_name'][k])
    trans['Actor1_Gender'] = int(Mv_cast_dt['actor1_gender'][k])
    trans['Main_Actor2'] = str(Mv_cast_dt['actor2_name'][k])
    trans['Actor2_Gender'] = int(Mv_cast_dt['actor2_gender'][k])
    trans['Main_Actor3'] = str(Mv_cast_dt['actor3_name'][k])
    trans['Actor3_Gender'] = int(Mv_cast_dt['actor3_gender'][k])
    trans['Main_Actor4'] = str(Mv_cast_dt['actor4_name'][k])
    trans['Actor4_Gender'] = int(Mv_cast_dt['actor4_gender'][k])
    trans['Main_Actor5'] = str(Mv_cast_dt['actor5_name'][k])
    trans['Actor5_Gender'] = int(Mv_cast_dt['actor5_gender'][k])
    trans['Nb_actors'] = int(Mv_cast_dt['actor_number'][k])
    trans['Director'] = str(Mv_cast_dt['director_name'][k])
    trans['Director_Gender'] = int(Mv_cast_dt['director_gender'][k])
    trans['Producer'] = str(Mv_cast_dt['producer_name'][k])
    trans['Editor'] = str(Mv_cast_dt['editor_name'][k])
    table.put_item(Item=trans)

### Table FilmDescription
for k in range(1000):
    table = dynamodb.Table('FilmDescription')
    trans = {}
    trans['ID'] = int(Movies_dt['id'][k])
    trans['Budget'] = int(Movies_dt['budget'][k])
    trans['Genre'] = str(Movies_dt['genres'][k])
    trans['Language'] = str(Movies_dt['original_language'][k])
    trans['nb_Language'] = int(Movies_dt['spoken_languages_number'][k])
    trans['Resume'] = str(Movies_dt['overview'][k])
    trans['Company_Production'] = str(Movies_dt['production_companies'][k])
    trans['Country_Production'] = str(Movies_dt['production_countries'][k])
    trans['Release'] = str(Movies_dt['release_date'][k])
    trans['Revenue'] = int(Movies_dt['revenue'][k])
    trans['Tagline'] = str(Movies_dt['tagline'][k])
    trans['Note'] = int( Movies_dt['vote_average'][k])
    table.put_item(Item=trans)


### Table AwardsByCategory
for k in range(2000):
    table = dynamodb.Table('AwardsByCategory')
    trans = {}

    trans['Director'] = str(Aws_by_ctg_dt['director_name'][k])
    trans['Year'] = int(Aws_by_ctg_dt['year'][k])
    trans['Ceremony'] = str(Aws_by_ctg_dt['ceremony'][k])
    trans['Category'] = str(Aws_by_ctg_dt['category'][k])
    trans['Result'] = str(Aws_by_ctg_dt['outcome'][k])

    table.put_item(Item=trans)
