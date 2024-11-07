import pandas as pd
import json
import requests
from datetime import datetime,time
import psycopg2
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from exceptions.noflatfile import NoFlatFileForTodayException
from exceptions.connectiontimeout import ConnectionTimeoutException
import os
import pickle
import smtplib,ssl
import traceback
import io
from configuration.parameters import ConfigurationParameters
from emailnotifier.emailnotifier import EmailNotifier
# Load the DataFrame from the pickle file




class MentzRealtimeVersusScheduleComparator:
    def __init__(self,rootPath) -> None:
        print(rootPath)
        
        try:
            with open(os.path.join(rootPath,"data","flat"+datetime.now().strftime("%Y-%m-%d")+".pkl"), 'rb') as file:
                flat_df = pickle.load(file)
            self.scheduled=flat_df
            self.rootPath=rootPath
            self.configurationParameters=ConfigurationParameters()
        except:
            """ print("no flat file")
            messagebody=traceback.print_exc()
            print(messagebody)
            notifier=EmailNotifier()
            notifier.simpleNotification("Flat file not found",messagebody)  """
            traceback.print_exc()
            raise NoFlatFileForTodayException
               
    def parse_gtfs_rt_feed(self,url):
        
        try : 
            response = requests.get(url,verify=False)
        
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pb_filename = f"gtfs_feed_{timestamp}.pb"
            #with open(os.path.join(self.rootPath,"data","streams",pb_filename), 'wb') as pb_file:
                #pb_file.write(response.content)
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            
            trip_updates = []
            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    trip_updates.append(entity.trip_update)
            
            return trip_updates
        except:
            print("feed not found")
            messagebody=traceback.print_exc()
            print(messagebody)
            notifier=EmailNotifier()
            notifier.simpleNotification("connection timeout",messagebody)
            raise ConnectionTimeoutException
        
        
    def getUpdatesFromPbf(self):
        gtfs_rt_feed_url = self.configurationParameters.realtimeUrl

# Parse GTFS RT feed and extract trip updates
        trip_updates = self.parse_gtfs_rt_feed(gtfs_rt_feed_url)
        return trip_updates
    def getAddedTrips(self,updatesAsDict):
        addedTrips=[]
        for tripDict in updatesAsDict:
            if tripDict["trip"]["scheduleRelationship"]=="ADDED":
                
                
                addedTrips.append(tripDict["trip"]["tripId"])
        
        return addedTrips
    def parsePbfUpdates(self):
        trip_updates=self.getUpdatesFromPbf()
        #trip_updates=self.getUpdatesFromLocalfile()
        
        uniqueTripsId=[]
        updatesAsDict=[]
        uniqueRouteIds=[]

        for update in trip_updates:
            trip_dict=MessageToDict(update)
            updatesAsDict.append(trip_dict)
            
            uniqueTripsId.append(trip_dict["trip"]["tripId"])
            uniqueRouteIds.append(trip_dict["trip"]["routeId"])
            del(trip_dict)
        
        if self.configurationParameters.adddedTrips:
                
            addedTrips=self.getAddedTrips(updatesAsDict)
            
            return {"unique_trips_ids": set(uniqueTripsId),"total_updates":len(updatesAsDict),"unique_route_ids":set(uniqueRouteIds),"AddedTrips":set(addedTrips)}
        else:
            return {"unique_trips_ids": set(uniqueTripsId),"total_updates":len(updatesAsDict),"unique_route_ids":set(uniqueRouteIds)}
       
        
        
        
    
    def updateFlatDataset(self):
        
        latestUpdate=self.parsePbfUpdates()
        uniqueTripIds=latestUpdate["unique_trips_ids"]
        flat_df=self.scheduled.set_index("trip_id").copy()
        newCounts=[]
        databasename=self.configurationParameters.databasename
        conn = psycopg2.connect(
    database=databasename, user='postgres', password='postgres', host='127.0.0.1', port= '5432'
                           )
        cursor = conn.cursor()
        
        updates=[]
        print(datetime.now())
        
        for trip in uniqueTripIds:
            
            newCount=0
            if(trip in flat_df.index):
                newCount = flat_df.at[trip, "number of updates"] + 1
                
                #if(newCount>1):
                    #print(trip +"new count "+str(newCount))
                updates.append((int(newCount), trip))
                
                
                
               
                
                
                flat_df.at[trip, "number of updates"] = newCount
        
        try:
            matchRate=len(updates) /len(uniqueTripIds)
        except:
            print("division by zero error")
        
        try:
            addedTrips=latestUpdate["AddedTrips"]     
            acceptableTrips=uniqueTripIds.union(addedTrips)
            print("accetpabletrips")
            print(len(acceptableTrips))
            adjustedMatchRate=len(acceptableTrips)/len(uniqueTripIds)
            print("adjusted matchrate"+str(adjustedMatchRate))
            fractionAddedTrips=len(addedTrips)/len(uniqueTripIds)
            currentTime=datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
                
            #newsqlStatement='INSERT INTO mi_realtime_analysis(timepoint, matchrate, total_updates,,adjusted_match_rate,fraction_added_trips) VALUES ({currentTime}, {match_rate},{total_updates},{mr_adjusted},{fr_addedTrips});'.format(currentTime="'"+currentTime+"'",match_rate=matchRate,total_updates=len(uniqueTripIds),mr_adjusted=adjustedMatchRate,fr_addedTrips=fractionAddedTrips)
            #print(newsqlStatement)
            #cursor.execute(newsqlStatement)
        except:
            traceback.print_exc()              
        
        if updates:
            
            print(print("applying updates numbering "+str(len(updates))+" at "+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))  )
            sqlStatement = "UPDATE mi_schedule_updates SET number_of_updates=%s WHERE trip_id=%s"
            cursor.executemany(sqlStatement, updates)
            try:
                
                currentTime=datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
                
                newsqlStatement='INSERT INTO mi_realtime_analysis(timepoint, matchrate, total_updates,adjusted_match_rate,fraction_added_trips) VALUES ({currentTime}, {match_rate},{total_updates},{mr_adjusted},{fr_addedTrips});'.format(currentTime="'"+currentTime+"'",match_rate=matchRate,total_updates=len(uniqueTripIds),mr_adjusted=adjustedMatchRate,fr_addedTrips=fractionAddedTrips)
                cursor.execute(newsqlStatement)
            except:
            
                traceback.print_exc()
        conn.commit()
        cursor.close()
        conn.close()          
                
                
                 
            
        flat_df.reset_index(inplace=True)
        return flat_df
    
    
    
    def notifyViaEmail(self):
        port = 587  # For starttls
        smtp_server = "smtp.gmail.com"
        sender_email = "kangkan.dc1@gmail.com"
        receiver_email = self.configurationParameters.recieverEmail
        password = "lizlieooojggyqxp"
        message = """\
        Subject: Exception at GTFS RT Script

        Exception at GTFS RT Script."""

        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, port) as server:
            server.ehlo()  # Can be omitted
            server.starttls(context=context)
            server.ehlo()  # Can be omitted
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message)
                          
#end of class declaration   

