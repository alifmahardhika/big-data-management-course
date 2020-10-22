
#import the library
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Pipeline


#menambahkan SparkContext
#menambahkan SQLContext untuk membuat dataframe pada Spark
conf = SparkConf().setAppName('Classify-Text')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)



#membuat data buatan dengan tipe DataFrame
data = sqlContext.createDataFrame([
(0, "film jelek banget", 0.0),
(1, "akting buruk", 0.0),
(2, "karakter tidak bagus", 0.0),
(3, "aktor kurang menjiwai", 0.0),
(4, "film membosankan", 0.0),
(5, "karakter sangat menjiwai", 1.0),
(6, "cukup bagus", 1.0),
(7, "film tidak membosankan", 1.0),
(8, "aktor ganteng", 1.0),
(9, "film seru", 1.0)],
["id", "text", "label"])

#membagi data training dan testing sebanyak 60% dan 40%
training, testing = data.randomSplit([0.6, 0.4], seed=0)

#memecah kalimat menjadi kata dengan transformer Tokenizer()
tokenizer = Tokenizer(inputCol="text", outputCol="words")

#mengubah kata-kata tersebut menjadi Term Frequency 
#transformer HashingTF()
hashingTF = HashingTF (inputCol=tokenizer.getOutputCol() , outputCol="features")

#mendefiniskan estimator untuk machine learning model
lr = LogisticRegression(maxIter=10)

#membuat pipeline (stages)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

#training
lrModel = pipeline.fit(training)

#testing
predictions = lrModel.transform(testing)


#menampilkan hasil prediksi
predictions.select("text","label", "prediction").show()

