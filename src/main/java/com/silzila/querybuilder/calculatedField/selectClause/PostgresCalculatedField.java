package com.silzila.querybuilder.calculatedField.selectClause;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.helper.ColumnListFromClause;
import com.silzila.helper.FieldNameProcessor;
import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.payload.request.ConditionFilter;
import com.silzila.payload.request.Field;
import com.silzila.payload.request.Filter;
import com.silzila.payload.request.Flow;
import com.silzila.querybuilder.RelationshipClauseGeneric;
import com.silzila.querybuilder.WhereClause;
import com.silzila.querybuilder.calculatedField.ConditionFilterToFilter;
import com.silzila.querybuilder.calculatedField.DateFlow.PostgresDateFlow;

public class PostgresCalculatedField {

    private static ThreadLocal<DatasetDTO> threadLocalDatasetDTO = new ThreadLocal<>();

    private final static Map<String, String> basicMathOperations = Map.of(
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

        private final static Map<String, String> basicTextOperations = Map.ofEntries(
            Map.entry("concat", "CONCAT"),
            Map.entry("propercase", "INITCAP"),
            Map.entry("lowercase", "LOWER"),
            Map.entry("uppercase", "UPPER"),
            Map.entry("trim", "TRIM"),
            Map.entry("ltrim", "LTRIM"),
            Map.entry("rtrim", "RTRIM"),
            Map.entry("length", "LENGTH"),
            Map.entry("substringright", "RIGHT"),
            Map.entry("substringleft", "LEFT"),
            Map.entry("replace", "REPLACE"),
            Map.entry("split", "SPLIT_PART")
        );
    
    // to compose a multiple calculated fields
    public static String calculatedFieldsComposed(List<CalculatedFieldRequest> calculatedFieldRequests) throws BadRequestException {
        StringBuilder calculatedFieldString = new StringBuilder();
    
        for (int i = 0; i < calculatedFieldRequests.size(); i++) {
            if (i > 0) {
                calculatedFieldString.append(" ,\n");
            } else {
                calculatedFieldString.append("\n");
            }
            calculatedFieldString.append(calculatedFieldComposedWithAlias(calculatedFieldRequests.get(i)));
        }
    
        return calculatedFieldString.toString();
    }
    
    // to compose a calculated field without alias
    public static String calculatedFieldComposed(CalculatedFieldRequest calculatedFieldRequest) throws BadRequestException {

        StringBuilder calculatedField =  new StringBuilder();
    
        Map<String, Field> fields = calculatedFieldRequest.getFields();
        Map<String, List<ConditionFilter>> conditionFilterMap = calculatedFieldRequest.getConditionFilters()!= null ?
                calculatedFieldRequest.getConditionFilters() : new HashMap<>();
        Map<String, List<Flow>> flowMap = calculatedFieldRequest.getFlows();

        List<String> aggregatedFlow = new ArrayList<>();
        Map<String, String> conditionFilterStringMap = new HashMap<>();
        Map<String, String> flowStringMap = new HashMap<>();
        List<String> flowForConditionFilter = extractFlowForConditionFilters(conditionFilterMap);

        String flowKey = "";
        for (String key : flowMap.keySet()) {
            flowKey = key;  
        }

        // resolveFlowDependencies(flowMap, flowForConditionFilter);
        // procesFlowReqruiredForCondition(flowForConditionFilter, flowMap, fields, flowStringMap, conditionFilterStringMap,flowKey);
        //processConditionFilters(conditionFilterMap, fields,flowMap, flowStringMap, conditionFilterStringMap);
        processFlows(flowForConditionFilter, flowMap, fields, flowStringMap,conditionFilterMap,conditionFilterStringMap,flowKey,aggregatedFlow);
    
        return calculatedField.append(flowStringMap.get(flowKey)).toString();
    }

    // to compose a field with alias (calculatedFieldName)
    public static String calculatedFieldComposedWithAlias(CalculatedFieldRequest calculatedFieldRequest)throws BadRequestException{

        StringBuilder calculatedField = new StringBuilder();

        String formattedAliasName = FieldNameProcessor.formatFieldName(calculatedFieldRequest.getCalculatedFieldName());

        return calculatedField.append(" (").append(calculatedFieldComposed(calculatedFieldRequest))
        .append( ") AS ").append(formattedAliasName).toString();
    }

    //composing a query to get sample records of calculated field
    public static String composeSampleRecordQuery(CalculatedFieldRequest calculatedFieldRequest,DatasetDTO datasetDTO,Integer recordCount) throws BadRequestException{
        StringBuilder query = new StringBuilder("SELECT \n\t");

        // fixing a record count, if it is null or exceed 100
        if (recordCount == null || recordCount == 0|| recordCount > 100) {
            recordCount = 100;
        }

        query.append(calculatedFieldComposedWithAlias(calculatedFieldRequest));

        List<String> allColumnList = (calculatedFieldRequest!=null) 
                                         ? ColumnListFromClause.getColumnListFromFields(calculatedFieldRequest.getFields()) 
                                         : new ArrayList<>();
        String fromClause = RelationshipClauseGeneric.buildRelationship(allColumnList,datasetDTO.getDataSchema(),"postgresql");

        query.append("\nFROM ").append(fromClause) .append( "\nLIMIT ").append(recordCount);

        return query.toString();
        
    }

    public static String composeFilterOptionsQuery(CalculatedFieldRequest calculatedFieldRequest,DatasetDTO datasetDTO) throws BadRequestException{
        StringBuilder query = new StringBuilder("SELECT DISTINCT \n\t");


        query.append(calculatedFieldComposedWithAlias(calculatedFieldRequest));

        List<String> allColumnList = (calculatedFieldRequest!=null) 
                                         ? ColumnListFromClause.getColumnListFromFields(calculatedFieldRequest.getFields()) 
                                         : new ArrayList<>();
        String fromClause = RelationshipClauseGeneric.buildRelationship(allColumnList,datasetDTO.getDataSchema(),"postgresql");

        query.append("\nFROM ").append(fromClause);

        return query.toString();
        
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
        if (operandType != null && operandType.get(0).equals("flow") && !flowForConditionFilter.contains(operand.get(0))) {
            flowForConditionFilter.add(operand.get(0));
        }
    }
    
    // if the flow required for a condition filter , has sourcetype flow 
    // private static void resolveFlowDependencies(Map<String, List<Flow>> flowMap, List<String> flowForConditionFilter) {
    //     Set<String> processedKeys = new HashSet<>(flowForConditionFilter); 
    //     int i = 0;
    
    //     while (i < flowForConditionFilter.size()) {
    //         String key = flowForConditionFilter.get(i);
    //         List<Flow> flows = flowMap.get(key);
            
    //         if (flows != null) { // Check for null to avoid potential NPE
    //             flows.forEach(flow -> {
    //                 for (int j = 0; j < flow.getSource().size(); j++) {
    //                     if (flow.getSourceType().get(j).equals("flow")) {
    //                         String newKey = flow.getSource().get(j);
    //                         if (processedKeys.add(newKey)) { // Only add if not already processed
    //                             flowForConditionFilter.add(newKey);
    //                         }
    //                     }
    //                 }
    //             });
    //         }
    //         i++;
    //     }
    // }
    

    // private static void processConditionFilters(Map<String, List<ConditionFilter>> conditionFilterMap,
    //                                             Map<String, Field> fields,
    //                                             Map<String, List<Flow>> flows,
    //                                             Map<String, String> flowStringMap,
    //                                             Map<String, String> conditionFilterStringMap) {
    //     if (conditionFilterMap != null && !conditionFilterMap.isEmpty()) {
    //         conditionFilterMap.forEach((key, conditionFilters) -> {
    //             conditionFilters.forEach(conditionFilter -> {
    //                 List<Filter> filters = ConditionFilterToFilter.mapConditionFilterToFilter(
    //                         conditionFilter.getConditions(), fields,flows, flowStringMap);
    //                 try {
    //                     String conditionString = WhereClause.filterPanelWhereString(
    //                             filters, conditionFilter.getShouldAllConditionsMatch(), "postgresql");
    //                     conditionFilterStringMap.put(key, conditionString);
    //                 } catch (BadRequestException e) {
    //                     e.printStackTrace();
    //                 }
    //             });
    //         });
    //     }
    // }

    private static String processConditionFilter(Map<String, List<ConditionFilter>> conditionFilterMap,
                                             Map<String, Field> fields,
                                             Map<String, List<Flow>> flows,
                                             Map<String, String> flowStringMap,
                                             Map<String, String> conditionFilterStringMap,
                                             String key) throws BadRequestException {
    if (conditionFilterMap == null || conditionFilterMap.isEmpty() || !conditionFilterMap.containsKey(key)) {
        return "";
    }

    StringBuilder conditionStringBuilder = new StringBuilder();
    List<ConditionFilter> conditionFilters = conditionFilterMap.get(key);

    for (ConditionFilter conditionFilter : conditionFilters) {
        List<Filter> filters = ConditionFilterToFilter.mapConditionFilterToFilter(
                conditionFilter.getConditions(), fields, flows, flowStringMap);

        String whereClause = WhereClause.filterPanelWhereString(
                filters, conditionFilter.getShouldAllConditionsMatch(), "postgresql");

        // Append this whereClause to the builder and add a separator if needed
        if (conditionStringBuilder.length() > 0) {
            conditionStringBuilder.append(" AND "); // or " OR " depending on logic
        }
        conditionStringBuilder.append(whereClause);

        // Store the computed string in the conditionFilterStringMap
        conditionFilterStringMap.put(key, conditionStringBuilder.toString());
    }

    return conditionStringBuilder.toString();
}

    
    
    // // to process flow required for a condition filters
    // private static void procesFlowReqruiredForCondition(List<String> flowForConditionFilter,
    //                                  Map<String, List<Flow>> flowMap,
    //                                  Map<String, Field> fields,
    //                                  Map<String, String> flowStringMap,
    //                                  Map<String, String> conditionFilterStringMap,
    //                                  String lastKey) throws BadRequestException {
    
    //     for (String flowKey : flowForConditionFilter) {
    //     List<Flow> flows = flowMap.get(flowKey);
    //     Flow firstFlow = flows.get(0);

    //     if (firstFlow.getCondition() != null) {
    //         processConditionalFlow(flows, flowStringMap, conditionFilterStringMap, fields, flowKey);
    //     } else if (basicMathOperations.containsKey(firstFlow.getFlow()) || firstFlow.getIsAggregation()) {
    //         processNonConditionalMathFlow(firstFlow, fields, flowStringMap, flowKey, lastKey);
    //     } else if (basicTextOperations.containsKey(firstFlow.getFlow())) {
    //         processNonConditionalTextFlow(firstFlow, fields, flowStringMap, flowKey);
    //     } else {
    //         processNonConditionalDateFlow(firstFlow, fields, flowStringMap, flowKey);
    //     }
    // }
    // }

    // to process flows
    private static void processFlows(List<String> flowForConditionFilter,
                                 Map<String, List<Flow>> flowMap,
                                 Map<String, Field> fields,
                                 Map<String, String> flowStringMap,
                                 Map<String, List<ConditionFilter>> conditionFilterMap,
                                 Map<String, String> conditionFilterStringMap,
                                 String lastKey, List<String> aggregatedFlow) throws BadRequestException {

    for (Map.Entry<String, List<Flow>> entry : flowMap.entrySet()) {
        String flowKey = entry.getKey();
        List<Flow> flows = entry.getValue();
        Flow firstFlow = flows.get(0);

        if (firstFlow.getCondition() != null) { 
            processConditionalFlow(flows,flowMap,flowStringMap,conditionFilterMap,conditionFilterStringMap, fields, flowKey);
        } else if (basicMathOperations.containsKey(firstFlow.getFlow()) || firstFlow.getIsAggregation()) {
            processNonConditionalMathFlow(firstFlow, fields, flowStringMap, flowKey, lastKey,aggregatedFlow);
        } else if (basicTextOperations.containsKey(firstFlow.getFlow())) {
            processNonConditionalTextFlow(firstFlow, fields, flowStringMap, flowKey);
        } else {
            processNonConditionalDateFlow(firstFlow, fields, flowStringMap, flowKey);
        }
    }
}

    
    private static void processConditionalFlow(List<Flow> flows,
                                        Map<String, List<Flow>> flowMap,
                                        Map<String, String> flowStringMap,
                                        Map<String, List<ConditionFilter>> conditionFilterMap,
                                        Map<String, String> conditionFilterStringMap,
                                        Map<String, Field> fields,
                                        String flowKey) throws BadRequestException {

            StringBuilder caseQuery = new StringBuilder("CASE ");

            for (Flow flow : flows) {
            if ("if".equals(flow.getCondition()) || "elseif".equals(flow.getCondition())) {
            caseQuery.append("WHEN ").append(processConditionFilter(conditionFilterMap, fields, flowMap, flowStringMap, conditionFilterStringMap, flow.getFilter())).append(" THEN ");
            appendSourceToQuery(fields, flowStringMap, flow, caseQuery);
            } else if ("else".equals(flow.getCondition())) {
            caseQuery.append(" ELSE ");
            appendSourceToQuery(fields, flowStringMap, flow, caseQuery);
            }
            }

            caseQuery.append(" END ");
            flowStringMap.put(flowKey, caseQuery.toString());
    }

    
    private static void appendSourceToQuery(Map<String, Field> fields, Map<String, String> flowStringMap, Flow flow, StringBuilder query) throws BadRequestException {
        String sourceType = flow.getSourceType().get(0);
        if ("field".equals(sourceType)) {
            Field field = fields.get(flow.getSource().get(0));
            if(field == null){
                throw new BadRequestException("No such a field with an id:" + flow.getSource().get(0));
            }
            query.append(field.getTableId()).append(".").append(field.getFieldName()).append(" ");
        } else if ("flow".equals(sourceType)) {
            String sourceFlowValue = flowStringMap.get(flow.getSource().get(0));
            if(sourceFlowValue == null || sourceFlowValue.isEmpty()){
                throw new BadRequestException("No such a flow with an id:" + flow.getSource().get(0));
            }
            query.append(sourceFlowValue).append(" ");
        }
        else if(sourceType.equals("text")){
            query.append("'").append(flow.getSource().get(0)).append("'").append(" ");
        } 
        else {
            query.append(flow.getSource().get(0)).append(" ");
        }
    }
    
    //to process math flow
    private static void processNonConditionalMathFlow(Flow flow,
                                                  Map<String, Field> fields,
                                                  Map<String, String> flowStringMap,
                                                  String flowKey,
                                                  String lastKey, List<String> aggregatedFlow) throws BadRequestException {
    List<String> result = new ArrayList<>();
    List<String> source = flow.getSource();
    List<String> sourceType = flow.getSourceType();
    String flowType = flow.getFlow();
    
    
    if (basicMathOperations.containsKey(flowType)) {
        if (List.of("addition", "subtraction", "multiplication", "division").contains(flowType)) {
            
            processMathBasicOperations(flow, fields, flowStringMap, result, flowKey, source, sourceType,lastKey,aggregatedFlow);
        } else if (List.of("ceiling", "floor", "absolute").contains(flowType)) {
            
            processMathSingleArgumentOperations(flow, fields, flowStringMap, flowKey, source, sourceType,lastKey,aggregatedFlow);
        } else if (List.of("min", "max").contains(flowType)) {
            
            processMultipleArgumentOperations(flow, fields, flowStringMap, flowKey, result, source, sourceType,lastKey,aggregatedFlow);
        } else if ("power".equals(flowType)) {
            
            processPowerOperation(flow, fields, flowStringMap, flowKey, source, sourceType,lastKey,aggregatedFlow);
        }      
    }
    else if(!basicMathOperations.containsKey(flowType) && flow.getIsAggregation()){
        processAggregation(flow,"", sourceType.get(0), source.get(0), fields, flowStringMap, flowKey, lastKey, false,aggregatedFlow);
    }
}

    // to process math basic operations - addition, subtraction, multiplicattion, division
    private static void processMathBasicOperations(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap,
                                             List<String> result,String flowKey,List<String> source,List<String> sourceType, String lastKey, List<String> aggregatedFlow) throws BadRequestException {
        
        if (source.size() < 2) {
        throw new BadRequestException("Insufficient parameters: at least two values are required for basic math operations.");
        }

        for (int i = 0; i < source.size(); i++) {
            String processedSource = getMathProcessedSource(source.get(i), sourceType.get(i), fields, flowStringMap, flow, i,flowKey, lastKey,aggregatedFlow);
            result.add(processedSource);
            if (i < source.size() - 1) {
                result.add(basicMathOperations.get(flow.getFlow()));
            }
        }
        String mathematicalExpression = String.join(" ", result);
        flowStringMap.put(flowKey, mathematicalExpression);
        if(flow.getIsAggregation() && !flowKey.equals(lastKey)){
            mathematicalExpression = processAggregation(flow, mathematicalExpression, "agg", null, fields, flowStringMap, flowKey, lastKey, true,aggregatedFlow);
        }
    }

    // to procees math single argument operations - absolute,ceiling,floor
    private static void processMathSingleArgumentOperations(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap,
                                                         String flowKey,List<String> source, List<String> sourceType, String lastKey, List<String> aggregatedFlow) throws BadRequestException {
        String processedSource = getMathProcessedSource(source.get(0), sourceType.get(0), fields, flowStringMap, flow, 0,flowKey,lastKey,aggregatedFlow);
        flowStringMap.put(flowKey, basicMathOperations.get(flow.getFlow()) + "(" + processedSource + ")");
    }

    // to procees math multiple argument operations - minimum and maximum
    private static void processMultipleArgumentOperations(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap,
                                                         String flowKey,List<String> result, List<String> source, List<String> sourceType, String lastKey, List<String> aggregatedFlow) throws BadRequestException {
        if (source.size() < 2) {
        throw new BadRequestException("Insufficient parameters: at least two values are required for basic math operations.");
        }

        for (int i = 0; i < source.size(); i++) {
            String processedSource = getMathProcessedSource(source.get(i), sourceType.get(i), fields, flowStringMap, flow, i,flowKey,lastKey,aggregatedFlow);
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
                                             String flowKey,List<String> source, List<String> sourceType, String lastKey, List<String> aggregatedFlow) throws BadRequestException {
        if (source.size() != 2) {
        throw new BadRequestException("Invalid parameters: Power operation requires exactly two parameters.");
        }

        String processedSource = getMathProcessedSource(source.get(0), sourceType.get(0), fields, flowStringMap, flow, 0,flowKey,lastKey,aggregatedFlow);
        flowStringMap.put(flowKey, basicMathOperations.get(flow.getFlow()) + "(" + processedSource + "," + source.get(1) + ")");
    }

    // to get a list of source with and without aggregation
    private static String getMathProcessedSource(String source, String sourceType,
                                             Map<String, Field> fields, Map<String, String> flowStringMap, Flow flow, int index,String flowKey,String lastKey,List<String> aggregatedFlow) throws BadRequestException {
        String processedSource = "";
        if ("field".equals(sourceType)) {
            Field field = fields.get(source);
            if(field== null){
                throw new BadRequestException("No such a field with an id:" + flow.getSource().get(0));
            }
            processedSource = field.getTableId() + "." + field.getFieldName(); 
        } else if ("flow".equals(sourceType)) {
            
            String sourceKey = (flow.getSourceType().contains("field") || flow.getSourceType().contains("flow")) && aggregatedFlow.contains(source)? source + "@" : source;
            String sourceFlowValue = Optional.ofNullable(flowStringMap.get(sourceKey))
                                    .orElse(flowStringMap.get(source));
            if(sourceFlowValue == null || sourceFlowValue.isEmpty()){
                throw new BadRequestException("No such a flow with an id:" + source);
            }
            System.out.println(source);
            processedSource = sourceFlowValue;
        } else {
            processedSource = source;
        }

        if (flow.getIsAggregation()) {
            processedSource = flow.getAggregation().get(index) + "(" + processedSource + ")";
            System.out.println("AggregatedFlow " + aggregatedFlow);
            System.out.println("FlowKey + " + flowKey);
            aggregatedFlow.add(flowKey);
        }
        return processedSource;
    }

    private static String processAggregation(Flow flow, String processedSource, String sourceType, 
                                         String source, Map<String, Field> fields,Map<String,String> flowStringMap,String flowKey, String lastKey, Boolean isAggregatedWithBasicMath, List<String> aggregatedFlow) throws BadRequestException {

    if(!isAggregatedWithBasicMath){
        processedSource = getMathProcessedSource(source, sourceType, fields, flowStringMap, flow, 0, flowKey, lastKey,aggregatedFlow);
        flowStringMap.put(flowKey,processedSource);
    }

    if (!flowKey.equals(lastKey)) {
        try {
            String fromClause = "";

            if ("field".equals(sourceType)) {
                Field field = fields.get(source);
                fromClause = RelationshipClauseGeneric.buildRelationship(
                        Collections.singletonList(field.getTableId()), 
                        threadLocalDatasetDTO.get().getDataSchema(), 
                        "postgresql"
                );
            } else if ("flow".equals(sourceType) || isAggregatedWithBasicMath) {
                fromClause = RelationshipClauseGeneric.buildRelationship(
                        ColumnListFromClause.getColumnListFromFields(fields), 
                        threadLocalDatasetDTO.get().getDataSchema(), 
                        "postgresql"
                );
            }

            processedSource = "(SELECT " + processedSource + " FROM " + fromClause + ")";
        } catch (BadRequestException e) {
            System.out.println(e.getMessage());
        }
    }
  
        flowStringMap.put(flowKey+"@", processedSource);

    return processedSource;
}
    private static String processNonConditionalTextFlow(Flow firstFlow, Map<String, Field> fields, Map<String, String> flowStringMap, String flowKey) throws BadRequestException {
        String flowType = firstFlow.getFlow();
        StringBuilder result = new StringBuilder();
    
        List<String> processedSources = processTextSources(firstFlow, fields, flowStringMap);
    
        if(List.of("concat","propercase","uppercase","lowercase","trim","ltrim","rtrim","length").contains(flowType)){
            result.append(processTextSingleArgumentOperations(flowType,processedSources));
        }
        else if(List.of("substringright","substringleft").contains(flowType)){
            result.append(processSubStringOperations(firstFlow,flowType,processedSources));
        }
        else if ("replace".equals(flowType)){
            result.append(processTextReplaceOperation(firstFlow,flowType,processedSources));
        }
        else if ("split".equals(flowType)){
            result.append(processTextSplitOperation(firstFlow,flowType,processedSources));
        }
    
        // Store the result in flowStringMap using the flowKey
        flowStringMap.put(flowKey, result.toString());
        return result.toString();
    }

    // to process text operation - propercase,lowercase....
    private static String processTextSingleArgumentOperations(String flowType,List<String> processedSources){

            StringBuilder result = new StringBuilder();
            result.append(basicTextOperations.get(flowType)).append(" (")
                      .append(String.join(", ", processedSources))
                      .append(")");
            return result.toString();
    }

    // to process substring operation
    //1st source - String to extract from, 2nd source - number of chars to extract 
    private static String processSubStringOperations(Flow flow,String flowType,List<String> processedSources) throws BadRequestException{
        if (flow.getSource().size() != 2) {
            throw new BadRequestException("Invalid parameters: Substring operation requires exactly two parameters (field and position).");
        }
        
        StringBuilder result = new StringBuilder();
        result.append(basicTextOperations.get(flowType)).append(" (")
                    .append(processedSources.get(0))
                    .append(", ")
                    .append(flow.getSource().get(1))
                    .append(")");
        return result.toString();
    }

    // to process text replace operation
    //1st source - string, 2nd source - substring to be replaced, 3rd source - replacement to the replaced substring
    private static String processTextReplaceOperation(Flow flow,String flowType,List<String> processedSources) throws BadRequestException{
        if (flow.getSource().size() != 3) {
            throw new BadRequestException("Invalid parameters: Text replace operation requires exactly three parameters (field,string,string).");
        }
        StringBuilder result = new StringBuilder();
        result.append(basicTextOperations.get(flowType)).append(" (")
                    .append(processedSources.get(0))
                    .append(", '")
                    .append(flow.getSource().get(1))
                    .append("', '")
                    .append(flow.getSource().get(2))
                    .append("')");
        return result.toString();
    }

    // to process text split operation
    //1st source - string, 2nd source - delimiter, 3rd source - position(substring to be returned)
    private static String processTextSplitOperation(Flow flow,String flowType,List<String> processedSources) throws BadRequestException{
        if (flow.getSource().size() != 3) {
            throw new BadRequestException("Invalid parameters: Text split operation requires exactly three parameters (field,delimiter,position).");
        }
        StringBuilder result = new StringBuilder();
        result.append(basicTextOperations.get(flowType)).append(" (")
                    .append(processedSources.get(0))
                    .append(", '")
                    .append(flow.getSource().get(1))
                    .append("',")
                    .append(flow.getSource().get(2))
                    .append(")");
        return result.toString();
    }
    
    private static List<String> processTextSources(Flow firstFlow, Map<String, Field> fields, Map<String, String> flowStringMap) throws BadRequestException {
        List<String> source = firstFlow.getSource();
        List<String> sourceType = firstFlow.getSourceType();
        List<String> resultString = new ArrayList<>();
    
        for (int i = 0; i < source.size(); i++) {
            String sourceElement = source.get(i);
            String type = sourceType.get(i);
    
            if ("field".equals(type)) {
                Field field = fields.get(sourceElement);
            if(field== null){
                throw new BadRequestException("No such a field with an id:" + sourceElement);
            }
                resultString.add(field.getTableId() + "." + field.getFieldName());
            } else if ("flow".equals(type)) {
                String sourceFlowValue = flowStringMap.get(sourceElement);
            if(sourceFlowValue == null || sourceFlowValue.isEmpty()){
                throw new BadRequestException("No such a flow with an id:" + sourceElement);
            }
                resultString.add(sourceFlowValue);
            } else {

                resultString.add("'"+sourceElement+"'");
            }
        }
    
        return resultString;
    }

    private static void processNonConditionalDateFlow(Flow firstFlow, Map<String, Field> fields, Map<String, String> flowStringMap, String flowKey) throws BadRequestException{
            String dateFlow = PostgresDateFlow.postgresDateFlow(firstFlow,fields,flowStringMap,flowKey);
            flowStringMap.put(flowKey, dateFlow);
    }
 

    public static void setDatasetForAggregation(DatasetDTO datasetDTO){
        threadLocalDatasetDTO.set(datasetDTO);
    }

}
    