package com.silzila.payload.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CalculatedFieldRequest {
    @JsonProperty("fields")
    private Fields fields;
    @JsonProperty("conditionFilters")
    private ConditionFilters condionFilters;
    @JsonProperty("flows")
    private Flows flows;


}
