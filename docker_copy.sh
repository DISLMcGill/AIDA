#!/usr/bin/env bash

sudo docker cp aidas whe_client:/home/build/AIDA
sudo docker cp aidacommon whe_client:/home/build/AIDA
sudo docker cp aidaMiddleware whe_client:/home/build/AIDA

sudo docker cp aidas whe_middleware:/home/build/AIDA
sudo docker cp aidacommon whe_middleware:/home/build/AIDA
sudo docker cp aidaMiddleware whe_middleware:/home/build/AIDA

sudo docker cp aidas whe_server_1:/home/build/AIDA
sudo docker cp aidacommon whe_server_1:/home/build/AIDA
sudo docker cp aidaMiddleware whe_server_1:/home/build/AIDA

sudo docker cp aidas whe_server_2:/home/build/AIDA
sudo docker cp aidacommon whe_server_2:/home/build/AIDA
sudo docker cp aidaMiddleware whe_server_2:/home/build/AIDA

