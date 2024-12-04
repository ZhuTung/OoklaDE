import json
import os
import pandas as pd
import numpy as np
from google.cloud import storage

directory = '../OoklaSpeedtest_OpenData'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "fake_secret_key.json"

# list for 'global' to prevent same ID when looping through files
globalSpeedIDs = [num for num in range(1,1000000)]
globalCoordinateIDs = [num for num in range(1,1000000)]
globalDateIDs = [num for num in range(1,1000000)]

# def generate_unique_ID(IDList, GlobalIDList):
#     num = random.choice(GlobalIDList)
#     GlobalIDList.remove(num)
#     IDList.append(num)

# Extract data from geojson files and load it to Google Storage Bucket
def EL_FromGeoJson():
    storage_client = storage.Client()
    bucket_name = "ookla_bucket"
    bucket = storage_client.get_bucket(bucket_name)

    for filename in os.listdir(directory):
        # list for speed
        quadkey_list = []
        avg_d_kbps_list = []
        avg_u_kbps_list = []
        avg_d_mbps_list = []
        avg_u_mbps_list = []
        avg_lat_ms_list = []
        tests_list = []
        devices_list = []

        # list for coordinates
        long1 = []
        lat1 = []
        long2 = []
        lat2 = []
        long3 = []
        lat3 = []
        long4 = []
        lat4 = []
        long5 = []
        lat5 = []

        variable = [long1, lat1], [long2, lat2], [long3, lat3], [long4, lat4], [long5, lat5]

        # list for date
        quarter_list = []
        year_list = []

        # Grab the data for the Date
        txt_idx = filename.find('Q')
        quarter, year = filename[txt_idx: txt_idx + 7].split('_')

        file = os.path.join(directory, filename)
        print(file)

        with open(file, 'r') as output:
            data = json.load(output)

            for idx in range(0, len(data['features'])):
                # Grab the data for the Speed
                #generate_unique_ID(speedIDs, globalSpeedIDs)
                quadkey_list.append(data['features'][idx]['properties']['quadkey'])
                avg_d_kbps_list.append(data['features'][idx]['properties']['avg_d_kbps'])
                avg_u_kbps_list.append(data['features'][idx]['properties']['avg_u_kbps'])
                avg_d_mbps_list.append(data['features'][idx]['properties']['avg_d_mbps'])
                avg_u_mbps_list.append(data['features'][idx]['properties']['avg_u_mbps'])
                avg_lat_ms_list.append(data['features'][idx]['properties']['avg_lat_ms'])
                tests_list.append(data['features'][idx]['properties']['tests'])
                devices_list.append(data['features'][idx]['properties']['devices'])

                # Grab the data for the Coordinates
                #generate_unique_ID(coordinateIDs, globalCoordinateIDs)
                coordinate = data['features'][idx]['geometry']['coordinates'][0]

                for points, vars in zip(coordinate, variable):
                    for dot, var in zip(points, vars):
                        var.append(dot)
                
                # Add the quarter and year for each speed and coordinate
                #generate_unique_ID(dateIDs, globalDateIDs)
                quarter_list.append(quarter)
                year_list.append(year)

            data_df = {
                "quadkey": quadkey_list,
                "avg_d_kbps": avg_d_kbps_list,
                "avg_u_kbps": avg_u_kbps_list,
                "avg_d_mbps": avg_d_mbps_list,
                "avg_u_mbps": avg_u_mbps_list,
                "avg_lat_ms": avg_lat_ms_list,
                "tests": tests_list,
                "devices": devices_list,
                "long1": long1,
                "lat1": lat1,
                "long2": long2,
                "lat2": lat2,
                "long3": long3,
                "lat3": lat3,
                "long4": long4,
                "lat4": lat4,
                "long5": long5,
                "lat5": lat5,
                "quarter": quarter_list,
                "year": year_list
            }

            # Create dataframe
            df = pd.DataFrame(data_df)

            global globalSpeedIDs, globalCoordinateIDs, globalDateIDs

            df["SpeedID"] = np.random.choice(globalSpeedIDs, size = len(df), replace = False)
            df["CoordinateID"] = np.random.choice(globalCoordinateIDs, size = len(df), replace = False)
            df["DateID"] = np.random.choice(globalDateIDs, size = len(df), replace = False)

            globalSpeedIDs = list(set(globalSpeedIDs) - set(df["SpeedID"].values))
            globalCoordinateIDs = list(set(globalCoordinateIDs) - set(df["CoordinateID"].values))
            globalDateIDs = list(set(globalDateIDs) - set(df["DateID"].values))

            df_filename = quarter + "_" + year + ".csv"

            # Upload the dataframe to the bucket as csv file
            bucket.blob(df_filename).upload_from_string(df.to_csv(index = False), 'text/csv')