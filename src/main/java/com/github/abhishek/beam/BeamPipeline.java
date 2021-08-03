package com.github.abhishek.beam;

/*
# Set up environment variables
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
--table=${PROJECT_ID:DATASET.TABLE}
 */

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.abhishek.beam.Tweet;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Locale;

public class BeamPipeline {

    private final static Logger logger = LoggerFactory.getLogger(BeamPipeline.class.getName());


    public interface Options extends PipelineOptions {
        // source options
        @Description("Input Kafka Topic")
        @Default.String("twitter_tweets")
        String getTopic();
        void setTopic(String topic);

        @Description("Consumer Group")
        @Default.String("beam-driver-application")
        String getConsumerGroup();
        void setConsumerGroup(String group);


        // Sink Options
        @Description("Output for the pipeline")
        @Default.String("TwitterStreams:tweets")
        String getTable();
        void setTable(String table);

        @Description("Window Duration")
        @Default.Long(60)
        long getWindowDuration();
        void setWindowDuration(long duration);

        @Description("Window lateness [minutes]")
        @Default.Long(10)
        long getAllowedLateness();
        void setAllowedLateness(long lateness);
    }


    public static void main(String[] args) {

        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args)
                                                .withValidation()
                                                .as(Options.class);


        BeamPipeline.runPipeline(options);

    }


    static class ParseTweet extends DoFn<String,Tweet> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<Tweet> outputReceiver) throws Exception{
           try {     // create date formatter
               DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
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
               logger.error("[BAD DATA]: " + JsonParser.parseString(json).toString());
           }
        }
    }

    // BigQuery Schema:
    public static final Schema BQSchema = Schema.builder()
            .addStringField("user_id")
            .addStringField("user_name")
            .addStringField("screen_name")
            .addStringField("location")
            .addStringField("url")
            .addStringField("description")
            .addInt64Field("followers_count")
            .addInt64Field("friends_count")
            .addInt64Field("favourite_count")
            .addInt64Field("statuses_count")
            .addDateTimeField("user_created_at")
            .addStringField("time_zone")
            .addDateTimeField("tweet_created_at")
            .addStringField("tweet_id")
            .addStringField("text")
            .addStringField("source")
            .addInt64Field("retweet_count")
            .build();



    public static void runPipeline(Options options){

        // create beam pipeline
        Pipeline pipeline = Pipeline.create(options);
        options.setJobName("streaming-twitter-kafka-pipeline-" + System.currentTimeMillis());
        options.setWindowDuration(30);
        options.setAllowedLateness(0);
        options.setTopic("twitter_tweets");


        pipeline.getSchemaRegistry().registerPOJO(Tweet.class);

        PCollection<String> rawTweets = pipeline.apply("read from kafka source",KafkaIO.<String, String>read()
                                                                                .withBootstrapServers("127.0.0.1:9092")
                                                                                .withTopic("twitter_tweets")  // use withTopics(List<String>) to read from multiple topics.
                                                                                .withKeyDeserializer(StringDeserializer.class)
                                                                                .withValueDeserializer(StringDeserializer.class)
                                                                            // .withConsumerConfigUpdates(ImmutableMap.of("group.id", "my_beam_app_1")) // settings for ConsumerConfig. e.g :
                                                                                .withoutMetadata())
                                                // get value only skip keys [remove following if using key if more than one partition]
                                                .apply(Values.create())

        .apply("WindowByMinute", Window.<String>into(
                FixedWindows.of(Duration.standardSeconds(60))).withAllowedLateness(
                Duration.standardMinutes(5))
                .triggering(AfterWatermark.pastEndOfWindow()
                        .withLateFirings(AfterPane.elementCountAtLeast(200)))
                .accumulatingFiredPanes());

        PCollection<Tweet> tweets = rawTweets.apply(ParDo.of(new ParseTweet()));

        tweets.apply(Filter.by(new SerializableFunction<Tweet, Boolean>() {
            @Override
            public Boolean apply(Tweet input) {
                return input.followers_count > 40_000;
            }
        }));

        PCollection<Row> rows = tweets.apply("ConvertToRow", ParDo.of(new DoFn<Tweet, Row>() {
            @ProcessElement
            public void processElement(@Element Tweet tweet, OutputReceiver<Row> r) {

                Row row = Row.withSchema(BQSchema)
                        .addValues(tweet)
                        .build();
                r.output(row);
            }
        })).setRowSchema(BQSchema);

        // to big query
        rows.apply("Write to BigQuery", BigQueryIO.<Row>write().to(options.getTable()) // [optioins.getTable()]
                .useBeamSchema()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        /*
            NOTE: WRITE_TRUNCATE will delete and recreate your table each and every time.
            This is helpful in early pipeline iteration, especially as you are iterating on your schema,
            but can easily cause unintended issues in production. WRITE_APPEND or WRITE_EMPTY are safer.
         */

        // test write to file
        // rawTweets.apply(TextIO.write().to("/tmp/tweets").withWindowedWrites().withNumShards(1).withSuffix(".txt"));


        pipeline.run().waitUntilFinish();
    }


}
