### ch1  sbt  构建spark应用体验
````
sbt package
spark-submit  --class "SimpleApp" --master "local[*]" target/scala-2.11/ch1_2.11-0.1.jar
````
