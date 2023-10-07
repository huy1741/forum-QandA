from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .config("spark.mongodb.input.uri", "mongodb://admin:password@mongo:27017/stackoverflow.questions") \
    .config("spark.mongodb.output.uri", "mongodb://admin:password@mongo:27017/stackoverflow.questions") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.master", "spark://spark:7077") \
    .config('spark.sql.legacy.timeParserPolicy', 'LEGACY') \
    .config("spark.dynamicAllocation.executorIdleTimeout", "30s") \
    .config("spark.default.parallelism", "16") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the schema for your DataFrame
question_schema = StructType([
    StructField("Id", IntegerType(), True),
    # Keep OwnerUserId as StringType
    StructField("OwnerUserId", StringType(), True),
    # Keep CreationDate as StringType
    StructField("CreationDate", StringType(), True),
    # Keep ClosedDate as StringType
    StructField("ClosedDate", StringType(), True),
    StructField("Score", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Body", StringType(), True)
])

# Read data from stackoverflow.questions
questions_df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .schema(question_schema) \
    .load()

# Convert CreationDate and ClosedDate to DateType for stackoverflow.questions
questions_df = questions_df.withColumn(
    "CreationDate", to_date(col("CreationDate"), "yyyy-MM-dd"))
questions_df = questions_df.withColumn(
    "ClosedDate", to_date(col("ClosedDate"), "yyyy-MM-dd"))

# Check if questions_df is empty
if questions_df.count() == 0:
    print("stackoverflow.questions is empty")
else:
    questions_df.show()

# Read data from stackoverflow.answers
answer_schema = StructType([
    StructField("Id", IntegerType(), True),  # Renamed to "AnswerId"
    StructField("OwnerUserId", StringType(), True),
    # Keep it as StringType for now
    StructField("CreationDate", StringType(), True),
    StructField("ParentId", StringType(), True),  # Renamed to "AnswerScore"
    StructField("Score", IntegerType(), True),  # Renamed to "QuestionId"
    StructField("Body", StringType(), True)  # Renamed to "AnswerBody"
])
answers_df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .schema(answer_schema) \
    .option("uri", "mongodb://admin:password@mongo:27017/stackoverflow.answers") \
    .load()

# Convert AnswerCreationDate to DateType for stackoverflow.answers
answers_df = answers_df.withColumn(
    "CreationDate", (to_date(col("CreationDate"), "yyyy-MM-dd")))

# Check if answers_df is empty
if answers_df.count() == 0:
    print("stackoverflow.answers is empty")
else:
    answers_df.show()
# Create a window specification for ranking answers by CreationDate within each question
windowSpec = Window.partitionBy("ParentId").orderBy(col("CreationDate"))

# Add a column to the answers_df to rank answers within each question
answers_df = answers_df.withColumn("AnswerRank", row_number().over(windowSpec))

# Calculate the number of answers for each question
question_answer_count = answers_df \
    .filter(answers_df["ParentId"].isNotNull()) \
    .groupBy("ParentId") \
    .agg(max("AnswerRank").alias("Number of answers"))

# Show the result
question_answer_count.show()

# Write the DataFrame to a CSV file with a specified path and header
question_answer_count.write.csv("/usr/local/share/data/output_file.csv", header=True)

