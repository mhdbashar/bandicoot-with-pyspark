**Create SparkSession**

%pyspark
from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

===========================================================

**Prepare data**

%pyspark
from pyspark.sql.functions import col

# load original records
df_load = sparkSession.read.parquet('/endSyr527')

# load antennas
antennas = sparkSession.read.parquet("/mstr/antenna_.parquet") 

# prepare records with antennas
end = df_load.join(antennas, col("antennas.antenna_id")  == col("df_load.antenna_id")).select(col("interaction"), col("direction"), col("correspondent_id"), col("datetime"), col("call_duration"), col("GSM"), col("antenna_id"), col("LATITUDE").alias("latitude"),col("LONGITUDE").alias("longitude")).distinct()

end.show()

=============================================================

**Collect records and antennas for every GSM**

%pyspark
from pyspark.sql import functions as F

data_frame = end.groupBy('GSM').agg(F.collect_list(F.struct("interaction","direction","correspondent_id","datetime","call_duration","antenna_id")).alias("BC_records"), F.collect_list(F.struct("antenna_id","latitude","longitude")).alias("BC_antennas")) 
data_frame.show()

==============================================================

**fun: Extract features from Bandicoot user**

%pyspark
import bandicoot as bc
from datetime import datetime


def loaduser(user, recordslist, antennaslist):
    # prepare user's records
    band_rec= []
    for x in recordslist:
        band_rec.append(bc.Record(x['interaction'], x['direction'], x['correspondent_id'], datetime.strptime(x['datetime'],"%Y-%m-%d %H:%M:%S"), float(x['call_duration']), bc.Position(x['antenna_id'])))
    
    # prepare user's antennas    
    antennas = {str(x['antenna_id']) :(0 if str(x['latitude']) is None else float(str(x['latitude'])), 0 if str(x['longitude']) is None else float(str(x['longitude']))) for x in antennaslist}
    
    attrs = {}
    
    # load bandicoot user
    user_rec = bc.io.load(user, band_rec, antennas, attrs)
    
    # edit user weekend
    bc_user = user_rec[0]
    bc_user.weekend=[5,6]
    
    # return user's features
    return bc.utils.all(bc_user, summary='extended', split_week=True, split_day=True)
    
 ==============================================================
 
 **Main processing for all GSM**
 
 %pyspark

 df_q = data_frame.rdd.map(lambda x: loaduser(x['GSM'],x['BC_records'],x['BC_antennas'])).collect()

==============================================================

**fun: Write users' features**

%pyspark

import bandicoot as bc
from pyspark.sql import SparkSession

sc.addPyFile("/usr/lib/python2.7/site-packages/bandicoot/utils.py")
from utils import flatten

def to_csv(objects, filename, digits=5, warnings=True):
    rows = []
    if not isinstance(objects, list):
        objects = [objects]

    data = [flatten(obj) for obj in objects]
    all_keys = [d for datum in data for d in datum.keys()]
    field_names = sorted(set(all_keys), key=lambda x: all_keys.index(x))
    # print field_names
    rows.append(field_names)
    
    def make_repr(item):
        if item is None:
            return None
        elif isinstance(item, float):
            return repr(round(item, digits))
        else:
            return str(item)
    for row in data:
        row = dict((k, make_repr(v)) for k, v in row.items())
        rows.append([make_repr(row.get(k, None)) for k in field_names])
    
    df = sparkSession.createDataFrame(rows)
    df.write.mode('overwrite').csv(filename)
    
    if warnings:
        print("Successfully exported {} object(s) to {}".format(len(objects),
              filename))
              
==============================================================

**Write final result**

%pyspark
to_csv(df_q,"/Mhdbashar/bandicootv527x")
