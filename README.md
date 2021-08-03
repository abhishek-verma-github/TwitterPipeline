# Twitter-Kafka-Beam-Pipeline

Apache beam pipeline to filter incoming tweets by `follower_counts`.
A twitter client pushes tweets as message to Kafka-topic. The Producer code is in `com.github.abhishek.twitter` package.

The apache beam via KafkaIO subscribe to kafka_topic and Streams the filtered tweets to Data WareHouse **BIG Query**.
Here window based streaming is been used.

**Requirements:**
- JDK 11
- Maven
- Google Cloud Platform account.

**Dependencies:**

 
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>2.8.0</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.slf4j/slf4j-simple -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.30</version>
        </dependency>

        <!--twitter dependency-->
        <dependency>
            <groupId>com.twitter</groupId>
            <artifactId>hbc-core</artifactId> <!-- or hbc-twitter4j -->
            <version>2.2.0</version> <!-- or whatever the latest version is -->
        </dependency>
        
        <!-- Apache Beam 2.31.0 -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-core</artifactId>
            <version>2.31.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-runners-direct-java</artifactId>
            <version>2.31.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-io-google-cloud-platform</artifactId>
            <version>2.31.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-extensions-google-cloud-platform-core</artifactId>
            <version>2.31.0</version>
        </dependency>
        <!-- https://mvnrepository.com/artifact/org.apache.beam/beam-sdks-java-io-kafka -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-sdks-java-io-kafka</artifactId>
            <version>2.31.0</version>
        </dependency>


        <!-- dataflow dependencies  -->
        <dependency>
            <groupId>org.apache.beam</groupId>
            <artifactId>beam-runners-google-cloud-dataflow-java</artifactId>
            <version>2.31.0</version>
            <scope>runtime</scope>
        </dependency>


_To compile and Run_:

```# Set up environment variables
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export PIPELINE_FOLDER=gs://${PROJECT_ID}
export MAIN_CLASS_NAME=com.mypackage.pipeline.MyPipeline
export RUNNER=DataflowRunner

mvn compile exec:java \
-Dexec.mainClass=${MAIN_CLASS_NAME} \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--region=${REGION} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER}"
--table=${PROJECT_ID:DATASET.TABLE}```
