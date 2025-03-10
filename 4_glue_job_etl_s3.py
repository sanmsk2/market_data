import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1741620486427 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": True}, connection_type="s3", format="csv", connection_options={"paths": ["s3://san-prd-stock-market-data/market_data/"], "recurse": True}, transformation_ctx="AmazonS3_node1741620486427")

# Script generated for node Change Schema
ChangeSchema_node1741620839210 = ApplyMapping.apply(frame=AmazonS3_node1741620486427, mappings=[("date", "string", "date", "date"), ("open", "string", "open", "decimal"), ("high", "string", "high", "decimal"), ("low", "string", "low", "decimal"), ("close", "string", "close", "decimal"), ("volume", "string", "volume", "string"), ("dividends", "string", "dividends", "string"), ("stock splits", "string", "stock splits", "string"), ("ticker", "string", "ticker", "string")], transformation_ctx="ChangeSchema_node1741620839210")

# Script generated for node Reorder Column
ReorderColumn_node1741620945009 = SelectFields.apply(frame=ChangeSchema_node1741620839210, paths=["date", "ticker", "open", "low", "high", "close", "volume", "dividends", "stock splits"], transformation_ctx="ReorderColumn_node1741620945009")

# Define your custom output file name with timestamp
output_file_name = f"stock_market_data_final_CSV"

# Define the S3 output path using the custom file name
output_path = f"s3://san-prd-stock-market-data/market_data/{output_file_name}"

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ReorderColumn_node1741620945009, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741620757304", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1741622024101 = glueContext.getSink(path=output_path, connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1741622024101")
AmazonS3_node1741622024101.setCatalogInfo(catalogDatabase="stock_market_db",catalogTableName="market_data_final")
AmazonS3_node1741622024101.setFormat("csv")
AmazonS3_node1741622024101.writeFrame(ReorderColumn_node1741620945009)
job.commit()
