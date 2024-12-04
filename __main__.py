import geojson_script as gjs
import bigquery as bq

if __name__ == "__main__":

    # For Extraction and Loading
    bq.create_google_bucket()
    gjs.EL_FromGeoJson()
    
    # For Staging
    bq.create_dataset("Staging")
    bq.create_table()
    bq.bucketToBigQuery()

    # For "Production"
    bq.create_dataset("Production")