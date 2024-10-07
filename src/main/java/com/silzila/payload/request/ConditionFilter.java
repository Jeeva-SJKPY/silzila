package com.silzila.payload.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConditionFilter {
    @JsonProperty("shouldAllConditionsMatch")
    private Boolean shouldAllConditionsMatch;
    @JsonProperty("conditions")
    private List<Condition> conditions;

}
