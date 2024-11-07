from datetime import datetime,timedelta
import os
import pandas as pd
import psycopg2
import pickle
from exceptions.databasealredyfilled import DatabaseAlreadyFilledException
from getrootdirectory import getRootDirectory
from configuration.parameters import ConfigurationParameters


        
class GTFSCurrentDayAnalysis:
    def __init__(self,filePath:str) -> None:
        self.parentDirectory=filePath
        self.configurationParameters=ConfigurationParameters()
    def getTodaysServices(self):
        date_parser = lambda x: pd.to_datetime(x, format='%Y%m%d')
        dayOfWeek=datetime.now().strftime('%A')
        dayOfWeek=dayOfWeek.lower()
        calendar_formatted = pd.read_csv(os.path.join(os.getcwd(),"data","calendar.txt"), parse_dates=['start_date','end_date'], date_parser=date_parser)
        todaysServices=calendar_formatted.loc[((calendar_formatted["start_date"]<date_parser(datetime.today().strftime('%Y%m%d'))) & (calendar_formatted["end_date"]>date_parser(datetime.today().strftime('%Y%m%d'))) & (calendar_formatted[dayOfWeek]==1))]
        calendar_dates=pd.read_csv(os.path.join(os.getcwd(),"data","calendar_dates.txt"),parse_dates=['date'], date_parser=date_parser)
        
        includedsServicesServices=calendar_dates.loc[(calendar_dates["date"]==date_parser(datetime.today().strftime('%Y%m%d'))) & (calendar_dates["exception_type"]==1)]
    

        excludedServvices=calendar_dates.loc[(calendar_dates["date"]==date_parser(datetime.today().strftime('%Y%m%d'))) & (calendar_dates["exception_type"]==2)]
        originalServices=set(list(todaysServices["service_id"]))
        removedServices=set(list(excludedServvices["service_id"]))
        addedServices=set(list(includedsServicesServices["service_id"]))
        activeServices=originalServices.difference(removedServices)
        activeServices=activeServices.union(addedServices)
        todaysServices=pd.DataFrame(columns=["service_id","date"])
        todaysServices["service_id"]=list(activeServices)
        todaysServices["date"]=[datetime.today().strftime("%Y%m%d")]*len(activeServices)
        
        return todaysServices
    
    def getTodaysTrips(self):
        todaysServices=self.getTodaysServices()
        trips=pd.read_csv(os.path.join(self.parentDirectory,"data","trips.txt"))
        todayTrips=pd.merge(trips, todaysServices, on='service_id', how='inner')
        todayUniqueTripds=todayTrips.drop_duplicates(subset=["trip_id"])
        
        #fetch the agency id's from routs.txt
        
        routes=pd.read_csv(os.path.join(getRootDirectory(),"data","routes.txt"))
        todaysTripsWithRoutes=pd.merge(todayUniqueTripds,routes,on="route_id",how="inner")
        todaysTripsWithRoutesSubset=todaysTripsWithRoutes[['route_id', 'trip_id', 'service_id','agency_id','route_long_name', 'trip_headsign', 'direction_id',
            'shape_id', 'wheelchair_accessible', 'date']]
        return todaysTripsWithRoutesSubset
    
    def generateTimeStamp(self,day,time:str):
        try:
            timestampStr=day+" "+time
            correct_timestamp=datetime.strptime(timestampStr,"%Y%m%d %H:%M:%S")
        except:
            
            timeparts=time.split(":")
            hours=(timeparts[0])
            minutes=int(timeparts[1])
            correctedHours=correctedHours=int(hours)-24
            current_datetime = datetime.today()

                # Set the time to midnight (00:00:00)
            beginning_of_tomorrow = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

            correct_timestamp=beginning_of_tomorrow+timedelta(days=1,hours=correctedHours,minutes=minutes)
                
            
        return correct_timestamp
    
    def getDeparturesAndArrivalsToday(self):
        todaysTrips=self.getTodaysTrips()
        dtype = {"date": str}
        stopTimes=pd.read_csv(os.path.join(self.parentDirectory,"data","stop_times.txt"),dtype=dtype)
        first_stop = stopTimes['stop_sequence'] == 1
        last_stop = stopTimes.groupby('trip_id')['stop_sequence'].transform('max') == stopTimes['stop_sequence']
        subset = stopTimes[first_stop | last_stop]
        subset
        
        todaysDepartureArrivals=pd.merge(subset,todaysTrips,on="trip_id",how="inner")
        todaysDepartureArrivals.sort_values(by=['trip_id', 'stop_sequence'])
        todaysDepartureArrivals.to_csv(os.path.join(self.parentDirectory,"data",datetime.now().strftime("%Y-%m-%d")+".txt"))
        
        timestamps=[]
        agencies=[]
        agenciesFlat=[]
        trip_ids=[]
        beginTimes=[]
        endTimes=[]
        
        longNames=[]
        
        for i in range(len(todaysDepartureArrivals)):
            row=todaysDepartureArrivals.iloc[i]
            day=row["date"]
            currentTripId=row["trip_id"]
            
            
            if(row["stop_sequence"]==1):
                time=row["departure_time"]
                timestamp=self.generateTimeStamp(day,time)
                beginTimes.append(timestamp)
                timestamps.append(timestamp)
                trip_ids.append(currentTripId)
                agency=row["agency_id"]
                
                agencies.append(agency)
                agenciesFlat.append(agency)
                longname=row["route_long_name"]
                longNames.append(longname)
                
                
                
                
                        
                
            else:
                    
                
                time=row["arrival_time"]
                agency=row["agency_id"]
                agencies.append(agency)
                timestamp=self.generateTimeStamp(day,time)
                endTimes.append(timestamp)
                timestamps.append(timestamp)
                
                
                
              
                
            
            
            
           
                    
            
        todaysDepartureArrivals["timestamp"]=timestamps
        todaysDepartureArrivals["agency"]=agencies
       
        
        flatDataset=pd.DataFrame(columns=["trip_id","begin_time","end_time","agency"])
        flatDataset["trip_id"]=trip_ids
        flatDataset["begin_time"]=beginTimes
        flatDataset["end_time"]=endTimes
        flatDataset["agency"]=agenciesFlat
        flatDataset["route_long_name"]=longNames
        flatDataset["number of updates"]=[0]*len(trip_ids)
        flatDataset.to_pickle(os.path.join(self.parentDirectory,"data","flat"+datetime.now().strftime("%Y-%m-%d")+".pkl"))
        #flatDataset.to_pickle("today_flat"+".pkl")
        flatDataset.to_csv(os.path.join(self.parentDirectory,"data","flat"+datetime.now().strftime("%Y-%m-%d")+".txt"))
        
        todaySubset= todaysDepartureArrivals[["trip_id","arrival_time", "departure_time", "stop_id","stop_sequence",
                                        "stop_headsign","route_id", "service_id","trip_headsign","direction_id",
                                        "date", "timestamp", "agency"]]
        todaySubset["minute of day"]=todaySubset['timestamp'].dt.hour * 60 + todaySubset['timestamp'].dt.minute
        todaySubset.to_pickle(os.path.join(self.parentDirectory,"data",datetime.now().strftime("%Y-%m-%d")+".pkl"))
        return flatDataset
    
    def getVolumeSumaryByMinute(self):
        df=self.getDeparturesAndArrivalsToday()[1]
        df['begin_time'] = pd.to_datetime(df['begin_time'])
        df['end_time'] = pd.to_datetime(df['end_time'])

        # Generate a list of unique timestamps within the range of 'begin_time' and 'end_time'
        all_timestamps = pd.date_range(start=df['begin_time'].min().floor('T'), end=df['end_time'].max().ceil('T'), freq='T')

        # Initialize a dictionary to store the count of active trips for each timestamp
        active_trips_count = {}

        # Iterate over each timestamp and count the number of active trips
        for timestamp in all_timestamps:
            active_trips_count[timestamp] = ((df['begin_time'] <= timestamp) & (timestamp <= df['end_time'])).sum()

        # Create a new DataFrame from the dictionary of active trips counts
        active_trips_df = pd.DataFrame.from_dict(active_trips_count, orient='index', columns=['active_trips_count'])

        # Reset the index to have timestamp as a column
        active_trips_df = active_trips_df.reset_index().rename(columns={'index': 'timestamp'})

        # Display the resulting DataFrame
        active_trips_df.to_pickle("minuteSummary"+datetime.now().strftime("%Y-%m-%d")+".pkl")
        return active_trips_df
    
    def prepareSQLStatmentForTrip(self,tripDetailsRow):
        sqlStatement='INSERT INTO schedule_updates(trip_id, begin_time, end_time, agency,number_of_updates) VALUES ({trip_id}, {begin_time},{end_time},{agency},{number_of_updates});'.format(trip_id=tripDetailsRow["trip_id"],begin_time=tripDetailsRow["begin_time"],end_time=tripDetailsRow["end_time"],agency=tripDetailsRow["agency"],number_of_updates=0)
        return sqlStatement
    
    def checkIfDatabaseAlreadyFilled(self):
        rootPath=getRootDirectory()
        databasename=self.configurationParameters.databasename
        print(databasename=='gtfsanalysis')
        print(f"Database name: {databasename} (type: {type(databasename)})")
        conn = psycopg2.connect(
    database=databasename, user='postgres', password='postgres', host='127.0.0.1', port= '5432'
                           )
        cursor = conn.cursor()
        
           
        sqlStatement="select  * from mi_schedule_updates where service_date="+"'"+datetime.today().strftime("%Y-%m-%d")+"'"
        print(sqlStatement)
        cursor.execute(sqlStatement)
        records=cursor.fetchall()
        print("some records")
        print(len(records))
        cursor.close()
        conn.close()   
        if (len(records)>0):
            
            return True
        else:
            return False
        
                    
        
        
       
    
    def writeToDatabase(self):
        
        alreadyFilled=self.checkIfDatabaseAlreadyFilled()
        print(alreadyFilled)
        if(alreadyFilled):
            raise DatabaseAlreadyFilledException
        
        todaysTrips=self.getDeparturesAndArrivalsToday()
        databasename=self.configurationParameters.databasename
        print(databasename)
        conn = psycopg2.connect(
    database=databasename, user='postgres', password='postgres', host='127.0.0.1', port= '5432'
                           )
        cursor = conn.cursor()
        for i in range(len(todaysTrips)):
            currentRow=todaysTrips.iloc[i]
            sqlStatement="INSERT INTO mi_schedule_updates(trip_id, begin_time, end_time, agency,number_of_updates,service_date) VALUES (%s, %s, %s, %s, %s,%s)"
            values=(currentRow["trip_id"],currentRow["begin_time"],currentRow["end_time"],currentRow["agency"],0,datetime.today().strftime("%Y-%m-%d"))


            cursor.execute(sqlStatement,values)
            
        conn.commit()
        cursor.close()
        conn.close()   