# download cassandra-driver
#python -m pip install cassandra-driver

from cassandra.cluster import Cluster
# use local Cluster
cluster = Cluster(['localhost'])
# connect a new session
session = cluster.connect()

# create keyspace
session.execute("CREATE KEYSPACE FIT5137_MASL WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

# create table and udt
session.execute("USE FIT5137_MASL")

session.execute("CREATE TYPE sightingObs_type(duration text,text text,summary text)")

session.execute("""
CREATE TABLE ufos(
    id text,
    state text,
    day int,
    month int,
    year int,
    hour int,
    shape text,
    city text,
    weatherObs_windchill double,
    weatherObs_wdire text,
    weatherObs_wspd double,
    weatherObs_pressure double,
    weatherObs_temp double,
    weatherObs_hail int,
    weatherObs_rain int,
    weatherObs_vis double,
    weatherObs_dewpt double,
    weatherObs_thunder int,
    weatherObs_fog int,
    weatherObs_tornado int,
    weatherObs_hum double,
    weatherObs_snow int,
    weatherObs_conds text,
    countyName text,
    sightingObs list<frozen<sightingObs_type>>,
    PRIMARY KEY((month,state),day,city,countyName,hour,year))
    """)

prepared = session.prepared("""
INSERT INTO ufos
(id,state,day,month,year,hour,shape,city,weatherObs_windchill,weatherObs_wdire,weatherObs_wspd,weatherObs_pressure,weatherObs_temp,weatherObs_hail,weatherObs_rain,weatherObs_vis,weatherObs_dewpt,weatherObs_thunder,weatherObs_fog,weatherObs_tornado,weatherObs_hum,weatherObs_snow,weatherObs_conds,countyName,sightingObs)
VALUES()
""")

        