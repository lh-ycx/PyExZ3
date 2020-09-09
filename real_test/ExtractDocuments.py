import sys
import os
import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, explode, lit, when, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType

#Test in local dev box
#from pyspark import SparkContext
#sc = SparkContext('local','test')
#sc.addPyFile(r'C:\CxCache\Microsoft.Substrate.AI.OdinmlHDI.16.2.2189\Content\odinml.py')
#sc.addPyFile(r'D:\matrixcompliant\MatrixCompliant\src\ODIN-ML\Partner\Common\Spark\Lib\scrubber.py')
# End test setting
import odinml
from scrubber import compliant_handle

def logPrint(msg):
    print("SystemLog: " + str(msg))

@compliant_handle()
def join():
    # aether arguments
    args_map = odinml.parse_args(sys.argv)
    appName = args_map["--AppName".lower()]
    appName_guid = args_map["--AppGuid".lower()]
    category = args_map["--Category".lower()]
    data_set = args_map["--DataSet".lower()]
    EncryptedOutput = args_map["--output_EncryptedOutput".lower()]
    end_date = args_map["--EndDate".lower()]
    file_format = args_map["--FileFormat".lower()]
    file_path = args_map["--FilePath".lower()]
    ground_truth_filename = args_map["--GroundTruthFileName".lower()]
    output_format = args_map["--OutputFormat".lower()]
    start_date = args_map["--StartDate".lower()]
    inputGroundTruthDataPath = args_map["--input_InputGroundTruthDataPath".lower()]

    #output file
    outputFile = EncryptedOutput + "/documents.csv"

    groundTruthDataFile = inputGroundTruthDataPath + "/" + ground_truth_filename

    appName = appName + appName_guid
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .getOrCreate()

    logPrint("Load ground truth data from " + groundTruthDataFile)
    groundTruth = spark.read.json(groundTruthDataFile)

    #Read "/{*}" under this dir
    logPrint("Load documents data from " + file_path)
    if ((not start_date) or start_date.isspace()) or ((not end_date) or end_date.isspace()):
        documents = odinml.load_curated_dataset(spark, data_set, category, location=file_path)
    else:
        documents = odinml.load_curated_dataset(spark, data_set, category, location=file_path, startdate=start_date, enddate=end_date)
    
    documents = documents\
        .select('puser', 'FileName', 'DocumentTitle', 'Author')\
        .withColumn( "UserId", regexp_replace(regexp_replace(col("puser"), "\"", ""), "\'", ""))

    documents = documents\
        .join(groundTruth, groundTruth.UserId == documents.UserId)\
        .select(documents.UserId, 'Filename', 'DocumentTitle', 'Author')
        
    
    documents.write.csv(path=outputFile, header=True, sep='\t', mode='overwrite')
    

if __name__ == "__main__":
    join()