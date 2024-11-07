from sollfahrplanreader.vvscurrentdayanalyser import VVSCurrentDayAnalysis
from sollfahrplanreader.nvbwcurrentdayanalyser import NVBWCurrentDayAnalysis
from sollfahrplanreader.efaTrip import EfaTrip
import pandas as pd
from getrootdirectory import getRootDirectory
import os
from datetime import datetime



def mapRowToEfaTrip(row):
    TData=row
    trip=EfaTrip(TData["begin_time"],TData["end_time"],TData["origin"],TData["destination"],TData["trip_id"])  
    return trip  






nvbwAnalysis=NVBWCurrentDayAnalysis("run2")

todayNVBWTrips=nvbwAnalysis.getDeparturesAndArrivalsToday()

vvsAnanlysis=VVSCurrentDayAnalysis("run1")

todayVVSTrips=vvsAnanlysis.getDeparturesAndArrivalsToday()



VVStrips = todayVVSTrips.apply(lambda row: mapRowToEfaTrip(row), axis=1).tolist()
NVBWTrips=todayNVBWTrips.apply(lambda row: mapRowToEfaTrip(row), axis=1).tolist()

VVSTripSet=set(VVStrips)
NVBWTripSet=set(NVBWTrips)
exclusiveVVS=VVSTripSet.difference(NVBWTripSet)

exclusiveTripsDict=[]

for el in exclusiveVVS:
    exclusiveTripsDict.append(el.to_dict())
    print(el.to_dict())
    
exclusiveVVSDf=pd.DataFrame.from_dict(exclusiveTripsDict)



exclusiveVVSDf.to_csv(os.path.join(getRootDirectory(),"data",datetime.now().strftime("%Y-%m-%d")+"_exclusiveVVS"+".csv"),index=False)  
    
    
    