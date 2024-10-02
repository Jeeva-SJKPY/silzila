package com.silzila.payload.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Flows {
    @JsonProperty("flowMap")
    private Map<String, List<Flow>> flowMap = new HashMap<>();
}
