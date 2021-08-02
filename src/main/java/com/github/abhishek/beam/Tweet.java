package com.github.abhishek.beam;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaFieldSchema.class)
public class Tweet {
    String created_at;
    String id;
    String text;
    String source;
    int retweet_counts;

    String user_id;
    String user_name;
    String screen_name;
    String location;
    String url;
    String description;
    int followers_count;
    int friends_count;
    int favourites_count;
    int statuses_count;
    String user_created_at;
    String time_zone;

    @SchemaCreate
    public Tweet(String created_at, String id, String text, String source, int retweet_counts, String user_id, String user_name, String screen_name, String location, String url, String description, int followers_count, int friends_count, int favourites_count, int statuses_count, String user_created_at, String time_zone) {
        this.created_at = created_at;
        this.id = id;
        this.text = text;
        this.source = source;
        this.retweet_counts = retweet_counts;
        this.user_id = user_id;
        this.user_name = user_name;
        this.screen_name = screen_name;
        this.location = location;
        this.url = url;
        this.description = description;
        this.followers_count = followers_count;
        this.friends_count = friends_count;
        this.favourites_count = favourites_count;
        this.statuses_count = statuses_count;
        this.user_created_at = user_created_at;
        this.time_zone = time_zone;
    }
}

