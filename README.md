# Data Requirements
The data folder should have two subfolders: nvbw and vvs
The gtfs files in each of them should be in a subfolder called gtfs (not zipped). Therefore the file structure of the data folder should be:

```plaintext
nvbw
|   |   
|   |   gtfs.zip
|   |
|   \---gtfs
|           agency.txt
|           calendar.txt
|           calendar_dates.txt
|           feed_info.txt
|           routes.txt
|           shapes.txt
|           stops.txt
|           stop_times.txt
|           transfers.txt
|           trips.txt
|
\---vvs
    |   
    |   gtfs.zip
    |
    \---gtfs
            agency.txt
            calendar.txt
            calendar_dates.txt
            feed_info.txt
            routes.txt
            shapes.txt
            stops.txt
            stop_times.txt
            ticketing_deep_links.txt
            ticketing_identifiers.txt
            transfers.txt
            trips.txt
´´´plaintext            

# Running the main script: 
The main script is the scripts folder and is called mainScript.py. The easiest way to run it is from the project root with python -m scripts.mainScript
