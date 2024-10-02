package com.silzila.payload.request;

import com.databricks.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Flow {

    @JsonProperty("flow")
    private String flow ;
    @JsonProperty("source")
    private List<String> source;
    @JsonProperty("condition")
    private String condition;
    @JsonProperty("sourceType")
    private List<String> sourceType;
    @JsonProperty("isAggregation")
    private Boolean isAggregation ;
    @JsonProperty("aggregation")
    private List<String> aggregation;
}
