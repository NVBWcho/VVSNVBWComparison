class ConfigurationParameters:
    def __init__(self) -> None:
        self.databasename='gtfsanalysis'
        self.recieverName="kangkan.dc1@gmail.com"
        self.realtimeUrl="https://gtfs.efa-bw.de/migebiet/"
        self.adddedTrips=True #some feeds do not have schedule relationship field
        self.refreshRate=60  #number of seconds before restart