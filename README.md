
# 카프카 연동 스파크 스트림 (뉴스 분석)

* 카프카 연동
    * https://sparkbyexamples.com/spark-streaming-kafka-consumer-example-in-json-format/

* 한글 형태소 분석
    * https://github.com/uosdmlab/spark-nkp
    * https://itlab.tistory.com/124
    * https://bitbucket.org/eunjeon/seunjeon/src/master/

## Build

```bash
gradle clean build -x test
```

## Run
* 주의
    * --packages 옵션에 여러 package를 지정할 경우 ','를 구분자로 하며 공백이 존재하여서는 안 된다.
    
```bash

spark-submit \
--master local \
--deploy-mode client \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0 \
--class com.example.stream.BypassDStream build/libs/newsteam-1.0-SNAPSHOT.jar 


spark-submit \
--master local \
--deploy-mode client \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 \
--class com.example.stream.BypassDFStream build/libs/newsteam-1.0-SNAPSHOT.jar




spark-submit \
--master local \
--deploy-mode client \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0,\
com.github.uosdmlab:spark-nkp_2.11:0.3.3,\
org.mongodb.spark:mongo-spark-connector_2.11:2.4.0,\
redis.clients:jedis:3.0.1 \
--class com.example.stream.TokenizeDStream build/libs/newsteam-1.0-SNAPSHOT.jar



spark-submit \
--master local \
--deploy-mode client \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,\
com.github.uosdmlab:spark-nkp_2.11:0.3.3,\
org.mongodb.spark:mongo-spark-connector_2.11:2.4.0,\
redis.clients:jedis:3.0.1 \
--class com.example.stream.TokenizeDFStream build/libs/newsteam-1.0-SNAPSHOT.jar

```


## Run (yarn)

```bash
spark-submit \
--master yarn \
--deploy-mode cluster \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0,\
com.github.uosdmlab:spark-nkp_2.11:0.3.3,\
org.mongodb.spark:mongo-spark-connector_2.11:2.4.0,\
redis.clients:jedis:3.0.1 \
--class com.example.stream.TokenizeDStream build/libs/newsteam-1.0-SNAPSHOT.jar


spark-submit \
--master yarn \
--deploy-mode cluster \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,\
com.github.uosdmlab:spark-nkp_2.11:0.3.3,\
org.mongodb.spark:mongo-spark-connector_2.11:2.4.0,\
redis.clients:jedis:3.0.1 \
--class com.example.stream.TokenizeDFStream build/libs/newsteam-1.0-SNAPSHOT.jar
```


## Docker 

```bash
docker run \
--name redis \
-d \
-v ${HOME}/data/crawl/redis:/data \
-p 6379:6379 \
redis redis-server --appendonly yes


docker run \
--name=crawldb \
-d \
-p 27017:27017 \
-v ${HOME}/data/crawl/mongo:/data/db \
mongo:4.1.13

```