import random

from test.RandomLoad import RandomLoad
from aida.aida import *;

config = __import__('bixi-config')
seed = 101


def update_seed(sd):
    global seed
    seed = sd
    random.seed(seed)


def q01(dw):
    rl = RandomLoad(2, seed)
    trip = dw.tripdata2017
    rl.load_data_randomly(trip)
    freqStations = trip.filter(Q('stscode', 'endscode', CMP.NE)).aggregate(
        ('stscode', 'endscode', {COUNT('*'): 'numtrips'}), ('stscode', 'endscode')).filter(
        Q('numtrips', C(50), CMP.GTE));
    rl.load_data_randomly(freqStations)
    freqStationsCord = freqStations.join(dw.stations2017, ('stscode',), ('scode',), COL.ALL,
                                         ({'slatitude': 'stlat'}, {'slongitude': 'stlong'})).join(dw.stations2017,
                                                                                                  ('endscode',),
                                                                                                  ('scode',),
                                                                                                  COL.ALL, (
                                                                                                      {
                                                                                                          'slatitude': 'enlat'},
                                                                                                      {
                                                                                                          'slongitude': 'enlong'}));
    return freqStationsCord


def q02(dw):
    rd = RandomLoad(2, seed)
    freqStations = dw.tripdata2017.filter(Q('stscode', 'endscode', CMP.NE)).aggregate(
    ('stscode', 'endscode', {COUNT('*'): 'numtrips'}), ('stscode', 'endscode')).filter(
    Q('numtrips', C(50), CMP.GTE));

    rd.load_data_randomly(freqStations)[0]
    # Next we will enrich the trip data set by using the distance information provided by the Google maps' API.
    # This can be accomplished by the relational join operators provided by AIDA.

    # Google also provides its estimated duration for the trip. We will have to see in the end if our trained model
    # is able to predict the trip duration better than google's estimate. So we will also save Google's estimate for
    # the trip duration for that comparison.

    # In[5]:
    # rd.load_data_randomly(freqStations)
    gtripData = dw.gmdata2017.join(dw.tripdata2017, ('stscode', 'endscode'), ('stscode', 'endscode'), COL.ALL,
                                   COL.ALL).join(freqStations, ('stscode', 'endscode'), ('stscode', 'endscode'),
                                                 ('id', 'duration', 'gdistm', 'gduration'));
    gtripData, freq = rd.load_data_randomly(gtripData, freqStations)
    guniqueTripDist = gtripData.project(('gdistm')).distinct().order('gdistm');

    # We will keep roughly 30% of these distances apart for testing and the rest, we will use for training.

    # In[7]:
    return guniqueTripDist
   

"""
SELECT avg(duration) AS avgd, stsname, endsname FROM  
tripdata2017 as t, 
(SELECT DISTINCT start.sname as stsname, ends.sname as endsname, gm.stscode, gm.endscode, gduration 
FROM gmdata2017 as gm, stations2017 as start, stations2017 as ends
where gm.stscode = start.scode AND gm.endscode = ends.scode ) as s 

WHERE s.stscode = t.stscode AND s.endscode = t.endscode
GROUP BY stsname, endsname
HAVING avg(duration) > 1.5*max(gduration); 
"""
def q03(dw):
    rd = RandomLoad(3, seed)
    gm = dw.gmdata2017
    rd.load_data_randomly(gm)

    stat = gm.join(dw.stations2017, ('stscode', ), ('scode', ),COL.ALL, ({'sname': 'stsname'},))\
        .join(dw.stations2017, ('endscode',), ('scode',), COL.ALL, ( {'sname': 'endsname'}, ))\
        .project(({F('gduration')*1.5:'gduration'}, 'endsname', 'stsname', 'stscode', 'endscode'))

    rd.load_data_randomly(stat)

    j = stat.join(dw.tripdata2017,  ('stscode', 'endscode'), ('stscode', 'endscode'), COL.ALL, ('duration',))\
        .filter(Q('duration', 'gduration', CMP.GT))

    rd.load_data_randomly(j)

    j = j.aggregate(({AVG('duration'): 'avg'}, 'stsname', 'endsname'), ('stsname', 'endsname'))

    return j


def q04(dw):
    pass





