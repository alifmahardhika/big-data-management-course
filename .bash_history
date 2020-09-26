ls
hdfs dfs ls
hdfs dfs -ls
hdfs dfs -mkdir /user/alif.mahardhika
hdfs dfs -ls
nano test.txt
hdfs dfs -ls
ls
vi test.txt
nano test.txt
ls
cat test.txt 
hdfs dfs -copyFromLocal test.txt
ls
ls -al
clear
hdfs dfs -cp test.txt test2.txt
ls
hdfs dfs -ls
hdfs dfs -copyToLocal test2.txt test2.txt
ls
hdfs dfs -rm test2.txt
hdfs dfs -ls
hdfs dfs -mkdir test
hdfs dfs -mv test.txt /test/test.txt
hdfs dfs -ls
hdfs dfs -mv test.txt test/test.txt
hdfs dfs -ls
hdfs dfs -cat test/test.txt
clear
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduceexamples-*.jar
clear
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount test.txt output-word-count
clear
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount test/test.txt output-word-count
hdfs dfs -ls
hdfs dfs -ls output-word-count
hdfs dfs -get output-word-count
ls
cd output-word-count/
ls
ls -al
cat part-r-00000 
ls
cat _SUCCESS 
clear
cd ..
hdfs dfs -ls -h /data
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount data/shakespeare.txt shakespeare-word-count

hdfs dfs -ls /data
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /data/shakespeare.txt shakespeare-word-count
ls
hdfs dfs -ls
hdfs dfs -get shakespeare-word-count
ls
cd shakespeare-word-count/
ls
cat part-r-00000 
clear
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar
hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 2 10
whoami
exit
hdfs dfs -cat /test/shakespeare.txt
hdfs dfs -cat test/shakespeare.txt
hdfs dfs -cat data/shakespeare.txt
hdfs dfs -cat /data/shakespeare.txt
clear
hdfs dfs -cp /data/shakespeare.txt /test/shakespeare.txt
hdfs dfs -cp /data/shakespeare.txt test/shakespeare.txt
hdfs dfs -ls
hdfs dfs -ls test
git
exit
