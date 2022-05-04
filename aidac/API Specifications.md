## Module APIS ##
```pycon
import aidac

# set config 
aidac.set_lazy_eval(true)

# add remote data source 
# connect at this point 
aidac.load_data_source('aida', host, port, user, passwd, db, job_name)
aidac.load_data_source('monetdb', host, port, user, passwd, db, job_name)

aidac.remote_tables(job=None)

>> {job_name.table_name, job_name.table_name ...} 

# read local csv file. On failure, throw IO exception

reviews = aidac.read_csv(path, headers=None)

# connect to remote movies table. On failure, throw remote exception
movies = aidac.read_remote_data(job_name, tables)

joind = aidac.join(reviews, movies, on='title', condition=None)

print(joind)
```

##use case 1
```pycon
aidac.load_data_source('aida', host, port, user, passwd, db, 'bixi')

# concat trips tables from the 2 years
trips2020 = aidac.read_csv('trips2020.csv', headers)
trips2017 = aidac.read_remote_data('bixi', 'tripdata2017')
trips = trips2017.concat(trips2020) # explicitly state lazy execution? Union

trips = trips.filter('duration>60') #np.where?
# or
trips = trips[trips['duration']>60] # this is the eager approach

# concat stations table
gmdata2020 = aidac.read_csv('stations2020.csv', headers)
gmdata2017 = aidac.read_remote_data('bixi', 'stations2020')
gmdata = gmdata2020.concat(gmdata2017)

# remove duplicates
gmdata = gmdata.drop_duplicates()

# groupby the start and end station and count number of trips
joind = trips.join(gmdata, on=['stscode', 'endcode'])
count = joind.groupby(['stscode', 'endcode']).sum()

# visualize
count.hist()
count.to_csv()
```


##use case 2

```pycon
movies['title'] # get the title column

movies.iloc(1, 4) # get the 1, 4 entry

joind = movies.join(reviews, movies, on='title', condition=None)

stats = joind[['rate', 'box_office']] # has to be local

# to use remote model, this is a feature nice to have, ignore it for now
def customized_func(stats)
    model = sklearn.LinearRegression()
    model.fit(stats['rate'], stats['box_office'])
aidac.persist(model)

```






