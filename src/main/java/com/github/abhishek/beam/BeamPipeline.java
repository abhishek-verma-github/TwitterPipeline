package com.github.abhishek.beam;

/*
# Set up environment variables
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export PIPELINE_FOLDER=gs://${PROJECT_ID}
export MAIN_CLASS_NAME=com.mypackage.pipeline.MyPipeline
export RUNNER=DataflowRunner
export GOOGLE_APPLICATION_CREDENTIALS={PATH_TO_JSON}
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=${MAIN_CLASS_NAME} \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--region=${REGION} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER}"[DataflowRunner]
--table=${PROJECT_ID:DATASET.TABLE}
 */

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.JsonParser;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Locale;

public class BeamPipeline {

    private final static Logger logger = LoggerFactory.getLogger(BeamPipeline.class.getName());

    private final static String projectId = "<PROJECT_ID>";
    private final static String bootstrapServer = "<bootstrap_server>:9093";
    private final static String kafkaTopic = "TOPIC";
    private final static String bqTable = String.format("%s:%s.%s",projectId,"<DATASET","<TABLE>");
    private final static String region = "<REGION>";


    public static void main(String[] args) {

//        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
//        Options options = PipelineOptionsFactory.fromArgs(args)
//                                                .withValidation()
//                                                .as(Options.class);

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        // For cloud execution, set the Google Cloud project, staging location,
        // and set DataflowRunner.
        options.setJobName("streaming-twitter-kafka-pipeline-" + System.currentTimeMillis());
        options.setProject(projectId);
        options.setRegion(region);
        options.setStagingLocation(String.format("gs://%s/staging",projectId));
        options.setGcpTempLocation(String.format("gs://%s/tmp",projectId));
        options.setRunner(DataflowRunner.class);

        BeamPipeline.runPipeline(options, bootstrapServer, kafkaTopic, bqTable);

    }


    static class ParseTweet extends DoFn<String,Tweet> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<Tweet> outputReceiver) throws Exception{
            try {     // create date formatter
                DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.US);
                dateFormat.setLenient(true);
                DateFormat newDateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");

                // user fields
                String user_id = JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("id_str").toString();
                String user_name = JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("name").toString();
                String screen_name = JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("screen_name").toString();
                String location = JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("location").toString();
                String url = JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("url").toString();
                String description = JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("description").toString();
                int followers_count = Integer.parseInt(JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").toString());
                int friends_count = Integer.parseInt(JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("friends_count").toString());
                int favourites_count = Integer.parseInt(JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("favourites_count").toString());
                int statuses_count = Integer.parseInt(JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("statuses_count").toString());
                String user_created_at = newDateFormat.format(dateFormat.parse(JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("created_at").toString()));
                String time_zone = JsonParser.parseString(json).getAsJsonObject().get("user").getAsJsonObject().get("time_zone").toString();

                // Tweet fields
                String tweet_created_at = newDateFormat.format(dateFormat.parse(JsonParser.parseString(json).getAsJsonObject().get("created_at").toString()));
                String tweet_id = JsonParser.parseString(json).getAsJsonObject().get("id_str").toString();
                String text = JsonParser.parseString(json).getAsJsonObject().get("text").toString();
                String source = JsonParser.parseString(json).getAsJsonObject().get("source").toString();
                int retweet_counts = Integer.parseInt(JsonParser.parseString(json).getAsJsonObject().get("id_str").toString());

                Tweet tweet = new Tweet(tweet_created_at, tweet_id, text, source, retweet_counts,
                        user_id, user_name, screen_name, location, url, description,
                        followers_count,friends_count, favourites_count, statuses_count,
                        user_created_at, time_zone);

                outputReceiver.output(tweet);
            }catch(Exception e){
                // logging for the time being, but should be entered into deadletter queue.
                logger.error("[BAD DATA]: " + JsonParser.parseString(json).toString());
            }
        }
    }

    // BigQuery Schema:

    public static TableSchema tableSchema = new TableSchema().setFields(
            Arrays.asList(
                    new TableFieldSchema().setName("user_id").setType("STRING").setMode("REQUIRED"),
                    new TableFieldSchema().setName("user_name").setType("STRING").setMode("REQUIRED"),
                    new TableFieldSchema().setName("screen_name").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("location").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("url").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("description").setType("STRING").setMode("NULLABLE"),

                    new TableFieldSchema().setName("followers_count").setType("INT64").setMode("REQUIRED"),
                    new TableFieldSchema().setName("friends_count").setType("INT64").setMode("REQUIRED"),
                    new TableFieldSchema().setName("favourite_count").setType("INT64").setMode("NULLABLE"),
                    new TableFieldSchema().setName("statuses_count").setType("INT64").setMode("NULLABLE"),

                    new TableFieldSchema().setName("user_created_at").setType("DATETIME").setMode("REQUIRED"),

                    new TableFieldSchema().setName("time_zone").setType("STRING").setMode("NULLABLE"),

                    new TableFieldSchema().setName("tweet_created_at").setType("DATETIME").setMode("REQUIRED"),

                    new TableFieldSchema().setName("tweet_id").setType("STRING").setMode("REQUIRED"),
                    new TableFieldSchema().setName("text").setType("STRING").setMode("REQUIRED"),
                    new TableFieldSchema().setName("source").setType("STRING").setMode("NULLABLE"),

                    new TableFieldSchema().setName("retweet_count").setType("INT64").setMode("NULLABLE")
            )
    );



    public static void runPipeline(DataflowPipelineOptions options, String bootstrapServer, String kafkaTopic, String bqTable){

        // create beam pipeline
        Pipeline pipeline = Pipeline.create(options);


        pipeline.getSchemaRegistry().registerPOJO(Tweet.class);

        PCollection<String> rawTweets = pipeline.apply("read from kafka source",KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServer)
                .withTopic(kafkaTopic)  // use withTopics(List<String>) to read from multiple topics.
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                // .withConsumerConfigUpdates(ImmutableMap.of("group.id", "my_beam_app_1")) // settings for ConsumerConfig. e.g :
                .withoutMetadata())
                // get value only skip keys [remove following if using key if more than one partition]
                .apply(Values.create())
                // Apply Window
                .apply("WindowByMinute", Window.<String>into(
                        FixedWindows.of(Duration.standardSeconds(60))).withAllowedLateness(
                        Duration.standardMinutes(5))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withLateFirings(AfterPane.elementCountAtLeast(200)))
                        .accumulatingFiredPanes());

        // (Apply Schema for complex transforms).
        PCollection<Tweet> tweets = rawTweets.apply("Parse JSON to Tweet",ParDo.of(new ParseTweet()));

        // Filter by followers...
        tweets.apply("Filter by followers_count",Filter.by(new SerializableFunction<Tweet, Boolean>() {
            public Boolean apply(Tweet input) {
                return input.followers_count > 40_000;
            }
        }));

        // map to TableRows to write to BigQuery.
        PCollection<TableRow> tableRows = tweets.apply("Map to TableRow for saving into Big Query Table",MapElements.via(
                new SimpleFunction<Tweet, TableRow>() {
                    @Override
                    public TableRow apply(Tweet tweet) {

                        return new TableRow().set("user_id",tweet.user_id)
                                .set("user_id",tweet.user_id)
                                .set("user_name",tweet.user_name)
                                .set("screen_name",tweet.screen_name)
                                .set("location",tweet.location)
                                .set("url",tweet.url)
                                .set("description",tweet.description)
                                .set("followers_count",tweet.followers_count)
                                .set("friends_count",tweet.friends_count)
                                .set("favourite_count",tweet.favourites_count)
                                .set("statuses_count",tweet.statuses_count)
                                .set("user_created_at",tweet.user_created_at)
                                .set("time_zone",tweet.time_zone)
                                .set("tweet_created_at",tweet.created_at)
                                .set("tweet_id",tweet.id)
                                .set("text",tweet.text)
                                .set("source",tweet.source)
                                .set("retweet_count",tweet.retweet_counts);
                    }
                }
        ));

        // write to big query table where schema already defined.
        tableRows.apply("Write to BigQuery", BigQueryIO.writeTableRows().to(bqTable) // [Noted] - we are writing TableRows directly and therefore writeTableRows() transform.]
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) // Table with Schema must already be created in Big Query.
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        /*
            NOTE: WRITE_TRUNCATE will delete and recreate your table each and every time.
            This is helpful in early pipeline iteration, especially as you are iterating on your schema,
            but can easily cause unintended issues in production. WRITE_APPEND or WRITE_EMPTY are safer.
         */

        // test write to file
        // rawTweets.apply(TextIO.write().to("/tmp/tweets").withWindowedWrites().withNumShards(1).withSuffix(".txt"));

        pipeline.run();
    }

}
}

