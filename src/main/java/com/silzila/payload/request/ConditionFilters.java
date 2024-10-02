package com.silzila.payload.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class ConditionFilters {
    @JsonProperty("conditionFilterMap")
    private Map<String, ConditionFilter> conditionFilterMap = new HashMap<>();
}
