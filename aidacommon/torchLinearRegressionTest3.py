from aida.aida import *;
host = 'tfServer2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw):
    freqStations = dw.tripdata2017.filter(Q('stscode', 'endscode', CMP.NE))     .aggregate(('stscode','endscode',{COUNT('*'):'numtrips'}), ('stscode','endscode'))     .filter(Q('numtrips',C(50), CMP.GTE));

    freqStationsCord = freqStations     .join(dw.stations2017, ('stscode',), ('scode',), COL.ALL, ({'slatitude':'stlat'}, {'slongitude':'stlong'}))     .join(dw.stations2017, ('endscode',), ('scode',), COL.ALL, ({'slatitude':'enlat'}, {'slongitude':'enlong'}));

    def computeDist(tblrData):
        # We are going to keep all the columns of the source tabularData object.
        data = copy.copy(tblrData.rows);  # This only makes a copy of the metadata, but retains original column data
        vdistm = data['vdistm'] = np.empty(tblrData.numRows, dtype=int);  # add a new empty column to hold distance.
        # These are the inputs to Vincenty's formula.
        stlat = data['stlat'];
        stlong = data['stlong'];
        enlat = data['enlat'];
        enlong = data['enlong'];
        for i in range(0, tblrData.numRows):  # populate the distance metric using longitude/latitude of coordinates.
            vdistm[i] = int(geopyd.distance((stlat[i], stlong[i]), (enlat[i], enlong[i])).meters);
        return data;

    freqStationsDist = freqStationsCord._U(computeDist);  # Execute the user transform
    tripData = dw.tripdata2017.join(freqStationsDist, ('stscode', 'endscode'), ('stscode', 'endscode')
                                    , ('id', 'duration'), ('vdistm',));

    duration = tripData[:,1].cdata['duration']
    distance = tripData[:,2].cdata['vdistm']

    model = nn.Linear(1, 1);
    model.cuda()
    X = torch.from_numpy(distance.astype(np.float32))
    y = torch.from_numpy(duration.astype(np.float32))
    epoch_size = 10000
    learningrate = 0.0000001
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learningrate)
    X = X.cuda()
    y = y.cuda()
    y = y.view(y.shape[0], 1)
    X = X.view(X.shape[0], 1)
    start_time = time.time()
    for epoch in range(epoch_size):
        y_predicted = model(X)
        loss = criterion(y_predicted, y)
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()
        print(model.weight)
    dw.linearModel = model
    end_time = time.time()
    execution_time = end_time - start_time
    logging.info("The execution time for 10000 iterations using 2256278 samples is "+str(execution_time))
    return(model.weight)

weight = dw._X(trainingLoop)
print(weight)
