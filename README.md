# AprioriAssignment3

Apriori Algo for finding frequent itemsets and associatin rules using Spark

Example Command to submit jar application on spark-submit:

spark-submit --class com.minal.spark.Main --master spark://minal-bilal:7077 --executor-memory 100G aprioriminal_2.11-1.0.jar apriori-minal-assignment/src/main/resources/input.dat 0.01 0.6 apriori-minal-assignment/output.txt
