Author: @reneebetina

# READING PARQUET FILES on AWS S3 using PYSPARK SHELL

Installation
----
Pre-requisites:
* Java Installed and
* Python Installed and
* Pyspark Installed `pip install pyspark`

For more info: https://sparkbyexamples.com/pyspark-tutorial/#pyspark-installation

### Running Pyspark Shell
----
1. If you are connecting to an existing cluster, you need to connect to it first.
2. Once connected, start Pyspark shell using the `pyspark` command. This will work with or without arguments. 
3. Once you are inside pyspark shell you can use any of the commands below: 
> #### READING FILES and storing it to a variable

> `df = spark.read.parquet(“<S3 Path>“).persist()`

> *`df` is a variable name. You may use any name.

> For Parquet files: spark.read.parquet(“_____“).persist()

> For CSV files: spark.read.csv(“_____“).persist()

4. After reading all the parquet files in your desired S3 path
`df.filter(“““ __<your conditions>__ “““).show(__<number of rows>__, truncate=False)`

> i.e. `df.filter(“““ last_name like 'A%' and department='Pediatrics' and age < 18 “““).show(5, truncate=False)`

you can use the usual SQL where conditions:
* columnName = ' '
* `like` ‘%___%’
* columnName `is null`
* `df.count()` - count all rows including the headers 
* `df.distinct().count()` - get unique rows then count 
* `show(__)` - show specific number of rows
* `truncate=False` - means full value is displayed

----
Other notes: 

* If you are working on a very large dataframe (billions of records), ensure you have enough cluster resources so that you will not encounter timeouts.

* If you encounter timeouts - monitor your cluster health and kill unnecessary jobs to make space for your spark query.