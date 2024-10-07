package com.silzila.payload.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CalculatedFieldRequest {
    @JsonProperty("fields")
    private Fields fields;
    @JsonProperty("conditionFilters")
    private Condition condionFilters;
    @JsonProperty("flows")
    private Flows flows;


}
