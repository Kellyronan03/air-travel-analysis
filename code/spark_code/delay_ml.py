from pyspark.sql import SparkSession
from pyspark.ml import classification
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when
from pyspark.ml.classification import NaiveBayes, LogisticRegression, RandomForestClassifier
import os

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDelayPrediction").getOrCreate()

# Define output file path
output_file_path = "output/model_eval.txt"

# Load the data (replace with your actual file location)
columns = ["Date", "Month", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier",
           "FlightNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin",
           "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay",
           "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "OriginAirport", "OriginCity",
           "OriginState", "DestAirport", "DestCity", "DestState", "Description", "ArrDelayFlag"]
df = spark.read.csv("input/final_flights.csv", header=False, inferSchema=True).toDF(*columns)

# Preprocess data
df = df.filter(col("ArrDelayFlag").isNotNull())  # Remove rows with missing labels

# Replace negative values with 0
def replace_negatives(column):
    return when(col(column) < 0, 0).otherwise(col(column))

# List of columns to check for negative values
neg_check_cols = ["CRSDepTime", "CRSArrTime", "TaxiIn", "TaxiOut", "CRSElapsedTime"]
for col_name in neg_check_cols:
    df = df.withColumn(col_name, replace_negatives(col_name))

indexers = [
    StringIndexer(inputCol="Origin", outputCol="OriginIndex").setHandleInvalid("skip"),
    StringIndexer(inputCol="Dest", outputCol="DestIndex").setHandleInvalid("skip"),
    StringIndexer(inputCol="UniqueCarrier", outputCol="CarrierIndex").setHandleInvalid("skip"),
    StringIndexer(inputCol="FlightNum", outputCol="FlightNumIndex").setHandleInvalid("skip")
]

# Apply indexers
for indexer in indexers:
    df = indexer.fit(df).transform(df)

# Create label column
df = StringIndexer(inputCol="ArrDelayFlag", outputCol="FlightStatusIndex").fit(df).transform(df)

# Assemble features
feature_cols = ["Month", "DayOfWeek", "CRSDepTime", "CRSArrTime", "Distance", "TaxiIn", "TaxiOut", "CRSElapsedTime",
                "OriginIndex", "DestIndex", "CarrierIndex", "FlightNumIndex"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_unscaled")
df = assembler.transform(df)

# Scale features
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=False, withStd=True)
df = scaler.fit(df).transform(df)

# Split data
train_data, test_data = df.randomSplit([0.8, 0.2], seed=1234)

# Define models
models = [
    ("Logistic Regression", LogisticRegression(labelCol="FlightStatusIndex", featuresCol="features")),
    ("Random Forest", RandomForestClassifier(labelCol="FlightStatusIndex", featuresCol="features", maxBins=500)),
    ("Naive Bayes", NaiveBayes(labelCol="FlightStatusIndex", featuresCol="features"))
]

# Model evaluation function
def evaluate_model(model, model_name):
    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="FlightStatusIndex", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    # Generate confusion matrix
    confusion_matrix = predictions.groupBy("FlightStatusIndex", "prediction").count().collect()
    matrix = {i: {j: 0 for j in range(4)} for i in range(4)}  
    for row in confusion_matrix:
        matrix[row['FlightStatusIndex']][row['prediction']] = row['count']

    # Print results
    print(f"{model_name} Accuracy: {accuracy}")
    print(f"{model_name} Confusion Matrix:")
    print("Predicted\\Actual  0    1    2    3")
    for actual in range(4):
        print(f"       {actual}       ", end="")
        for predicted in range(4):
            print(f"{matrix[actual][predicted]}   ", end="")
        print()

    with open(output_file_path, "a") as file:
        file.write(f"{model_name} Accuracy: {accuracy}\n")
        file.write(f"{model_name} Confusion Matrix:\n")
        
    #Save to file 
        file.write("Predicted\\Actual  0    1    2    3\n")
        for actual in range(4):
            file.write(f"       {actual}       ")
            for predicted in range(4):
                file.write(f"{matrix[actual][predicted]}   ")
            file.write("\n")
        
        file.write("\n")

# Evaluate models
for model_name, model in models:
    evaluate_model(model.fit(train_data), model_name)

# Stop Spark session
spark.stop()

print("Model evaluation completed.")
