/*
 * Copyright 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.training.appdev.services.gcp.spanner;

import com.google.cloud.spanner.*;
import com.google.training.appdev.services.gcp.domain.Feedback;

import java.util.ArrayList;
import java.util.List;

public class SpannerService {
    private static final SpannerService spannerService= new SpannerService(){};

    public static SpannerService create(){
        return spannerService;
    }

    public void insertFeedback(Feedback feedback){
        // TODO: Get a reference to the Spanner API
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();

        try {
            // Get a reference to the quiz-instance and its quiz-database
            DatabaseId db = DatabaseId.of(
                                options.getProjectId(),
                                "quiz-instance",
                                "quiz-database");

            // Get a client for the quiz-database
            DatabaseClient dbClient = spanner.getDatabaseClient(db);

            // Create a list to hold mutations against the database
            List<Mutation> mutations = new ArrayList<>();


            // Add an insert mutation
            mutations.add(
                    // Build a new insert mutation
                    Mutation.newInsertBuilder("Feedback")
                            .set("feedbackId")
                            .to(feedback.getEmail() + '_' +
                                feedback.getQuiz() + "_" +
                                feedback.getTimestamp())
                            .set("email")
                            .to(feedback.getEmail())
                            .set("quiz")
                            .to(feedback.getQuiz())
                            .set("feedback")
                            .to(feedback.getFeedback())
                            .set("rating")
                            .to(feedback.getRating())
                            .set("score")
                            .to(
                            feedback.getSentimentScore())
                            .set("timestamp")
                            .to(feedback.getTimestamp())
                            .build());
                    
            // Write the change to Spanner

            dbClient.write(mutations);

        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
