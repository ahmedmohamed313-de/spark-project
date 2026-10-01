from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_data(df: DataFrame) -> DataFrame:

    filtered_df = df.filter(
        (F.col("amount") > 0) & 
        (F.col("name").isNotNull())
    )
    

    cleaned_df = filtered_df.withColumn("amount_with_tax", F.col("amount") * 1.20)
    
    return cleaned_df
