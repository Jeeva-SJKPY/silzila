package com.silzila.querybuilder.calculatedField.selectClause;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.silzila.exception.BadRequestException;
import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.payload.request.ConditionFilter;
import com.silzila.payload.request.Field;
import com.silzila.payload.request.Filter;
import com.silzila.payload.request.Flow;
import com.silzila.querybuilder.WhereClause;
import com.silzila.querybuilder.calculatedField.ConditionFilterToFilter;

public class PostgresSelectClass {

    public static String calculatedFieldsComposed(List<CalculatedFieldRequest> calculatedFieldRequests) {
        StringBuilder calculatedFieldString = new StringBuilder();
    
        for (int i = 0; i < calculatedFieldRequests.size(); i++) {
            if (i > 0) {
                calculatedFieldString.append(" ,\n");
            } else {
                calculatedFieldString.append("\n");
            }
            calculatedFieldString.append(calculatedFieldComposed(calculatedFieldRequests.get(i)));
        }
    
        return calculatedFieldString.toString();
    }
    
    
    public static String calculatedFieldComposed(CalculatedFieldRequest calculatedFieldRequest) {

        Map<String, String> basicMathOperations = Map.of(
                "addition", "+",
                "subtraction", "-",
                "multiplication", "*",
                "division", "/",
                "ceiling","CEIL",
                "floor","FLOOR",
                "absolute","ABS",
                "power","POWER",
                "min", "LEAST",
                "max","GREATEST"
        );

        StringBuilder calculatedField =  new StringBuilder();
    
        Map<String, Field> fields = calculatedFieldRequest.getFields();
        Map<String, List<ConditionFilter>> conditionFilterMap = calculatedFieldRequest.getConditionFilters()!= null ?
                calculatedFieldRequest.getConditionFilters() : new HashMap<>();
        Map<String, List<Flow>> flowMap = calculatedFieldRequest.getFlows();
    
        Map<String, String> conditionFilterStringMap = new HashMap<>();
        Map<String, String> flowStringMap = new HashMap<>();
        List<String> flowForConditionFilter = extractFlowForConditionFilters(conditionFilterMap);

        resolveFlowDependencies(flowMap, flowForConditionFilter);
        processConditionFilters(conditionFilterMap, fields, flowStringMap, conditionFilterStringMap);
        processFlows(flowForConditionFilter, flowMap, fields, flowStringMap, conditionFilterStringMap, basicMathOperations);
    
        return calculatedField.append(" (").append(flowStringMap.get("f1"))
                            .append( ") AS ").append(calculatedFieldRequest.getCalculatedFieldName()).toString();
    }
    
    // extract flow from condition filters
    private static List<String> extractFlowForConditionFilters(Map<String, List<ConditionFilter>> conditionFilterMap) {
        List<String> flowForConditionFilter = new ArrayList<>();
        if (conditionFilterMap != null && !conditionFilterMap.isEmpty()) {
            conditionFilterMap.forEach((key, conditionFilters) -> {
                conditionFilters.forEach(conditionFilter -> {
                    conditionFilter.getConditions().forEach(condition -> {
                        addFlowFromOperand(condition.getLeftOperandType(), condition.getLeftOperand(), flowForConditionFilter);
                        addFlowFromOperand(condition.getRightOperandType(), condition.getRightOperand(), flowForConditionFilter);
                    });
                });
            });
        }
        return flowForConditionFilter;
    }
    
    //if the condition filters has flow - add to flowForConditionFilter
    private static void addFlowFromOperand(List<String> operandType, List<String> operand, List<String> flowForConditionFilter) {
        if (operandType != null && operandType.get(0).equals("flow")) {
            flowForConditionFilter.add(operand.get(0));
        }
    }
    
    // if the flow required for a condition filter , has sourcetype flow 
    private static void resolveFlowDependencies(Map<String, List<Flow>> flowMap, List<String> flowForConditionFilter) {
        if (!flowForConditionFilter.isEmpty()) {
            for (int i = 0; i < flowForConditionFilter.size(); i++) {
                String key = flowForConditionFilter.get(i);
                List<Flow> flows = flowMap.get(key);
                flows.forEach(flow -> {
                    for (int j = 0; j < flow.getSource().size(); j++) {
                        if (flow.getSourceType().get(j).equals("flow")) {
                            flowForConditionFilter.add(0, flow.getSource().get(j));
                        }
                    }
                });
            }
        }
    }

    private static void processConditionFilters(Map<String, List<ConditionFilter>> conditionFilterMap,
                                                Map<String, Field> fields,
                                                Map<String, String> flowStringMap,
                                                Map<String, String> conditionFilterStringMap) {
        if (conditionFilterMap != null && !conditionFilterMap.isEmpty()) {
            conditionFilterMap.forEach((key, conditionFilters) -> {
                conditionFilters.forEach(conditionFilter -> {
                    List<Filter> filters = ConditionFilterToFilter.mapConditionFilterToFilter(
                            conditionFilter.getConditions(), fields, flowStringMap);
                    try {
                        String conditionString = WhereClause.filterPanelWhereString(
                                filters, conditionFilter.getShouldAllConditionsMatch(), "postgres");
                        conditionFilterStringMap.put(key, conditionString);
                    } catch (BadRequestException e) {
                        e.printStackTrace();
                    }
                });
            });
        }
    }
    
    
    
    private static void procesFlowReqruiredForCondition(List<String> flowForConditionFilter,
                                     Map<String, List<Flow>> flowMap,
                                     Map<String, Field> fields,
                                     Map<String, String> flowStringMap,
                                     Map<String, String> conditionFilterStringMap,
                                     Map<String, String> basicMathOperations) {
    
        flowForConditionFilter.forEach(flowKey -> {
            List<Flow> flows = flowMap.get(flowKey);
            Flow firstFlow = flows.get(0);
    
            if (firstFlow.getCondition() != null) {
                processConditionalFlow(flows, flowStringMap, conditionFilterStringMap, fields, flowKey);
            } else if (basicMathOperations.containsKey(firstFlow.getFlow())) {
                processNonConditionalMathFlow(firstFlow, fields, flowStringMap, flowKey, basicMathOperations);
            }
            else{

            }
        });
    }

    private static void processFlows(List<String> flowForConditionFilter,
                                     Map<String, List<Flow>> flowMap,
                                     Map<String, Field> fields,
                                     Map<String, String> flowStringMap,
                                     Map<String, String> conditionFilterStringMap,
                                     Map<String, String> basicMathOperations) {
    
        flowMap.forEach((flowKey,flows) -> {
            
            Flow firstFlow = flows.get(0);
    
            if (firstFlow.getCondition() != null) {
                processConditionalFlow(flows, flowStringMap, conditionFilterStringMap, fields, flowKey);
            } else if (basicMathOperations.containsKey(firstFlow.getFlow())){
                processNonConditionalMathFlow(firstFlow, fields, flowStringMap, flowKey, basicMathOperations);
            }
            else{
                processNonConditionalTextFlow(firstFlow, fields, flowStringMap, flowKey);
            }
        });
    }
    
    private static void processConditionalFlow(List<Flow> flows,
                                               Map<String, String> flowStringMap,
                                               Map<String, String> conditionFilterStringMap,
                                               Map<String, Field> fields,
                                               String flowKey) {
    
        StringBuilder caseQuery = new StringBuilder("CASE ");
    
        flows.forEach(flow -> {
            if ("if".equals(flow.getCondition()) || "elseif".equals(flow.getCondition())) {
                caseQuery.append("WHEN ").append(conditionFilterStringMap.get(flow.getFilter())).append(" THEN ");
                appendSourceToQuery(fields, flowStringMap, flow, caseQuery);
            } else if ("else".equals(flow.getCondition())) {
                caseQuery.append(" ELSE ");
                appendSourceToQuery(fields, flowStringMap, flow, caseQuery);
            }
        });
    
        caseQuery.append(" END ");
        flowStringMap.put(flowKey, caseQuery.toString());
    }
    
    private static void appendSourceToQuery(Map<String, Field> fields, Map<String, String> flowStringMap, Flow flow, StringBuilder query) {
        String sourceType = flow.getSourceType().get(0);
        if ("field".equals(sourceType)) {
            Field field = fields.get(flow.getSource().get(0));
            query.append(field.getTableId()).append(".").append(field.getFieldName()).append(" ");
        } else if ("flow".equals(sourceType)) {
            query.append(flowStringMap.get(flow.getSource().get(0))).append(" ");
        } else {
            query.append(flow.getSource().get(0)).append(" ");
        }
    }
    
    //to process math flow
    private static void processNonConditionalMathFlow(Flow flow,
                                                  Map<String, Field> fields,
                                                  Map<String, String> flowStringMap,
                                                  String flowKey,
                                                  Map<String, String> basicMathOperations) {
    List<String> result = new ArrayList<>();
    List<String> source = flow.getSource();
    List<String> sourceType = flow.getSourceType();
    String flowType = flow.getFlow();
    
    if (basicMathOperations.containsKey(flowType)) {
        if (List.of("addition", "subtraction", "multiplication", "division").contains(flowType)) {
            processMathBasicOperations(flow, fields, flowStringMap, basicMathOperations, result, flowKey, source, sourceType);
        } else if (List.of("ceiling", "floor", "absolute").contains(flowType)) {
            processMathSingleArgumentOperations(flow, fields, flowStringMap, basicMathOperations, flowKey, source, sourceType);
        } else if (List.of("min", "max").contains(flowType)) {
            processMultipleArgumentOperations(flow, fields, flowStringMap, basicMathOperations, flowKey, result, source, sourceType);
        } else if ("power".equals(flowType)) {
            processPowerOperation(flow, fields, flowStringMap, basicMathOperations, flowKey, source, sourceType);
        }
    }
}

    // to process math basic operations - addition, subtraction, multiplicattion, division
    private static void processMathBasicOperations(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap,
                                            Map<String, String> basicMathOperations, List<String> result,
                                            String flowKey,List<String> source,List<String> sourceType) {
        for (int i = 0; i < source.size(); i++) {
            String processedSource = getProcessedSource(source.get(i), sourceType.get(i), fields, flowStringMap, flow, i);
            result.add(processedSource);
            if (i < source.size() - 1) {
                result.add(basicMathOperations.get(flow.getFlow()));
            }
        }
        flowStringMap.put(flowKey, String.join(" ", result));
    }

    // to procees math single argument operations - absolute,ceiling,floor
    private static void processMathSingleArgumentOperations(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap,
                                                        Map<String, String> basicMathOperations, String flowKey,
                                                        List<String> source, List<String> sourceType) {
        String processedSource = getProcessedSource(source.get(0), sourceType.get(0), fields, flowStringMap, flow, 0);
        flowStringMap.put(flowKey, basicMathOperations.get(flow.getFlow()) + "(" + processedSource + ")");
    }

    // to procees math multiple argument operations - minimum and maximum
    private static void processMultipleArgumentOperations(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap,
                                                        Map<String, String> basicMathOperations, String flowKey,
                                                        List<String> result, List<String> source, List<String> sourceType) {
        for (int i = 0; i < source.size(); i++) {
            String processedSource = getProcessedSource(source.get(i), sourceType.get(i), fields, flowStringMap, flow, i);
            result.add(processedSource);
            if (i < source.size() - 1) {
                result.add(",");
            }
        }
        flowStringMap.put(flowKey, basicMathOperations.get(flow.getFlow()) + "(" + String.join(" ", result) + ")");
    }

    // To process the power operation
    // 1st source - base value, 2nd source - exponent value
    private static void processPowerOperation(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap,
                                            Map<String, String> basicMathOperations, String flowKey,
                                            List<String> source, List<String> sourceType) {
        String processedSource = getProcessedSource(source.get(0), sourceType.get(0), fields, flowStringMap, flow, 0);
        flowStringMap.put(flowKey, basicMathOperations.get(flow.getFlow()) + "(" + processedSource + "," + source.get(1) + ")");
    }

    // to get a list of source with and without aggregation
    private static String getProcessedSource(String source, String sourceType,
                                             Map<String, Field> fields, Map<String, String> flowStringMap, Flow flow, int index) {
        String processedSource = "";
        if ("field".equals(sourceType)) {
            Field field = fields.get(source);
            processedSource = field.getTableId() + "." + field.getFieldName();
        } else if ("flow".equals(sourceType)) {
            processedSource = flowStringMap.get(source);
        } else {
            processedSource = source;
        }

        if (flow.getIsAggregation()) {
            processedSource = flow.getAggregation().get(index) + "(" + processedSource + ")";
        }
        return processedSource;
    }

    private static String processNonConditionalTextFlow(Flow firstFlow, Map<String, Field> fields, Map<String, String> flowStringMap, String flowKey) {
        String flowType = firstFlow.getFlow();
        StringBuilder result = new StringBuilder();
    
        List<String> processedSources = processSources(firstFlow, fields, flowStringMap);

        List<String> aggregation = firstFlow.getAggregation();
        String length = aggregation != null && !aggregation.isEmpty() ? aggregation.get(0) : "0";
    
        switch (flowType) {
            case "concat":
                result.append("CONCAT (\n")
                      .append(String.join(" ,\n", processedSources))
                      .append(")");
                break;
    
            case "uppercase":
                result.append("UPPER (")
                      .append(String.join("", processedSources))
                      .append(")");
                break;
    
            case "lowercase":
                result.append("LOWER (")
                      .append(String.join("", processedSources))
                      .append(")");
                break;
            case "substringright":
                result.append("RIGHT(")
                    .append(processedSources.get(0))
                    .append(", ")
                    .append(length)
                    .append(")");
                break;
            case "substringleft":
                result.append("LEFT(")
                    .append(processedSources.get(0))
                    .append(", ")
                    .append(length)
                    .append(")");
                break;
            
        }
    
        // Store the result in flowStringMap using the flowKey
        flowStringMap.put(flowKey, result.toString());
        return result.toString();
    }
    
    private static List<String> processSources(Flow firstFlow, Map<String, Field> fields, Map<String, String> flowStringMap) {
        List<String> source = firstFlow.getSource();
        List<String> sourceType = firstFlow.getSourceType();
        List<String> resultString = new ArrayList<>();
    
        for (int i = 0; i < source.size(); i++) {
            String sourceElement = source.get(i);
            String type = sourceType.get(i);
    
            if ("field".equals(type)) {
                Field field = fields.get(sourceElement);
                resultString.add(field.getTableId() + "." + field.getFieldName());
            } else if ("flow".equals(type)) {
                resultString.add(flowStringMap.get(sourceElement));
            } else {
                resultString.add(sourceElement);
            }
        }
    
        return resultString;
    }
}
    