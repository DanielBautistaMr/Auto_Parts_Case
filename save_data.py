from awsglue.dynamicframe import DynamicFrame

# Convertir DynamicFrame a DataFrame
df = dynamic_frame.toDF()

# Combinar todas las particiones en una sola
df = df.coalesce(1)

# Convertir de vuelta a DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Escribir en formato CSV
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://mi-etl-bucket/output-folder/"},
    format="csv"
)
