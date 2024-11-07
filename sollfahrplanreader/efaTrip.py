class EfaTrip:
    def __init__(self,beginTime,endTime,origin,destination,parentTripId) -> None:
        self.beginTime=beginTime
        self.endTime=endTime
        self.origin=origin
        self.destination=destination
        self.parentTripId=parentTripId
        
    def __eq__(self, other: object) -> bool:
        if isinstance(other,EfaTrip):
            #print("instance test ")
            if((self.beginTime==other.beginTime) and (self.endTime==other.endTime) and (self.origin==other.origin) and (self.destination==other.destination)):
                return True
            else:
                return False
        else:
            return False
    def __hash__(self):
        return hash((self.origin, self.destination, self.beginTime))
    
    def __str__(self) -> str:
        return (self.parentTripId+" origin "+self.origin +" to "+self.destination +" at "+str(self.beginTime))
    
    
    def to_dict(self):
        
        return {"trip_id":self.parentTripId,"origin":self.origin,"destination":self.destination,"begin_time":self.beginTime}