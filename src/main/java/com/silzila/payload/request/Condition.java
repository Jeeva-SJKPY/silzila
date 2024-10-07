package com.silzila.payload.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Condition {
    @JsonProperty("leftOperand")
    private List<String> leftOperand;

    @JsonProperty("leftOperandType")
    private List<String> leftOperandType;

    @JsonProperty("operator")
    private String operator;

    @JsonProperty("rightOperand")
    private List<String> rightOperand;

    @JsonProperty("rightOperandType")
    private List<String> rightOperandType;



}
