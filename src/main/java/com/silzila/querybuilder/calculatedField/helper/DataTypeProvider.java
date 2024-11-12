package com.silzila.querybuilder.calculatedField.helper;

import java.util.List;
import java.util.Map;

import com.silzila.payload.request.Field;
import com.silzila.payload.request.Flow;

public class DataTypeProvider {

    private final static List<String> basicMathOperations = List.of(
    "addition", "subtraction", "multiplication", "division", "ceiling", 
    "floor", "absolute", "power", "min", "max"
    );

    private final static List<String> basicTextOperations = List.of(
    "concat", "propercase", "lowercase", "uppercase", "trim", "ltrim", 
    "rtrim", "length", "substringright", "substringleft", "replace", "split"
    );

    private final static List<String> basicDateOperations = List.of(
        "stringToDate","addDateInterval","dateInterval","datePartName","datePartNumber",
        "truncateDate","currentDate", "currentTimestamp","minDate", "maxDate"
    );

     // to get a datatype of a flow 
    public static String getDataType(Map<String, List<Flow>> flows, Map<String, Field> fields, Flow firstFlow) {
        String flowType = firstFlow.getFlow();
    
        if ("if".equals(firstFlow.getCondition())) {
            String result = conditionFlowDateType(flows, fields, firstFlow);
            return  result;
        }
        else if (basicMathOperations.contains(flowType)
            || List.of("dateInterval", "datePartNumber").contains(flowType)
            || firstFlow.getIsAggregation()) {
            return "integer";
        } else if (basicTextOperations.contains(flowType)
            || List.of("datePartName").contains(flowType)) {
            return "text";
        } else if ("currentTimeStamp".equals(flowType)) {
            return "timestamp";
        } else if (basicDateOperations.contains(flowType)){
            return "date";
        }
        else {
            return "unknown"; 
        }
    }
    
    private static String conditionFlowDateType(Map<String, List<Flow>> flows, Map<String, Field> fields, Flow firstFlow) {
        String sourceType = firstFlow.getSourceType().get(0);
    
        if ("field".equals(sourceType)) {
            Field field = fields.get(firstFlow.getSource().get(0));
            return (field != null) ? field.getDataType().toString() : "unknown";
        } else if ("flow".equals(sourceType)) {
            String flowSourceId = firstFlow.getSource().get(0);
            List<Flow> sourceFlows = flows.get(flowSourceId);
    
            if (sourceFlows != null && !sourceFlows.isEmpty()) {
                return getDataType(flows, fields, sourceFlows.get(0));
            }
            return "unknown";
        }

        else{
            return sourceType;
        }
    }

}
