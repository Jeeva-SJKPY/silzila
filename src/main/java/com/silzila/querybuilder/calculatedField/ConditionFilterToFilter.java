package com.silzila.querybuilder.calculatedField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.silzila.payload.request.Condition;
import com.silzila.payload.request.Field;
import com.silzila.payload.request.Filter;
import com.silzila.payload.request.Filter.DataType;
import com.silzila.querybuilder.calculatedField.helper.DataTypeProvider;
import com.silzila.payload.request.Flow;

public class ConditionFilterToFilter {
    
    public static List<Filter> mapConditionFilterToFilter(List<Condition> conditions, Map<String, Field> fields,Map<String, List<Flow>> flows,Map<String, String> flowMap) {

    List<Filter> filters = new ArrayList<>();

    conditions.forEach((condition) -> {
        Filter filter = new Filter();
        List<String> leftOperand = condition.getLeftOperand();
        List<String> leftOperandType = condition.getLeftOperandType();
        List<String> rightOperand = condition.getRightOperand();
        List<String> rightOperandType = condition.getRightOperandType();

        if (leftOperandType.get(0).equals("field")) {
            setFieldFilter(leftOperand.get(0), filter, fields);
        } else if (leftOperandType.get(0).equals("flow")) {
            setFlowFilter(leftOperand.get(0), filter, flowMap,fields,flows);
        } else {
            filter.setFieldName(leftOperand.get(0));
            filter.setIsField(false);
        }

        mapOperatorAndType(condition, filter);
        filter.setShouldExclude(condition.getShouldExclude());
        filter.setIsTillDate(condition.getIsTillDate());
        filter.setUserSelection(buildUserSelection(rightOperand, rightOperandType, fields, flowMap));

        filters.add(filter);
    });

    System.out.println("Filters " + filters);

    return filters;
}

private static void setFieldFilter(String leftOperand, Filter filter, Map<String, Field> fields) {
    Field field = fields.get(leftOperand);
    filter.setTableId(field.getTableId());
    filter.setFieldName(field.getFieldName());
    filter.setIsField(true);
    filter.setDataType(Filter.DataType.fromValue(field.getDataType().value()));
    System.out.println("Filter timegrain  " + field.getTimeGrain());
    filter.setTimeGrain(Filter.TimeGrain.fromValue(field.getTimeGrain()));
}

private static void setFlowFilter(String leftOperand, Filter filter, Map<String, String> flowMap, Map<String, Field> fields,Map<String, List<Flow>> flows) {
    String flow = flowMap.get(leftOperand);
    Flow flowType = flows.get(leftOperand).get(0);
    filter.setFieldName(flow);
    filter.setDataType(Filter.DataType.fromValue(DataTypeProvider.getDataType(flows, fields, flowType)));
    filter.setIsField(false);
}

private static void mapOperatorAndType(Condition condition, Filter filter) {
    String operator = condition.getOperator();
    if (operator.equals("relativeFilter")) {
        filter.setFilterType("relativeFilter");
        filter.setDataType(DataType.DATE);
        filter.setRelativeCondition(condition.getRelativeCondition());
        filter.setOperator(Filter.Operator.fromValue("between"));
    } else if (operator.equals("tillDate")) {
        filter.setFilterType("tillDate");
        filter.setDataType(DataType.DATE);
        filter.setIsTillDate(true);
        filter.setOperator(Filter.Operator.fromValue("in"));
    } else {
        filter.setFilterType("search");
        filter.setOperator(Filter.Operator.fromValue(operator));
    }
}

private static List<String> buildUserSelection(List<String> rightOperand, List<String> rightOperandType, Map<String, Field> fields, Map<String, String> flowMap) {
    List<String> userSelection = new ArrayList<>();
    if(rightOperand != null){
        for (int i = 0; i < rightOperand.size(); i++) {
            String rightOp = rightOperand.get(i);
            String rightOpType = rightOperandType.get(i);
            if (rightOpType.equals("field")) {
                Field field = fields.get(rightOp);
                userSelection.add(field.getFieldName());
            } else if (rightOpType.equals("flow")) {
                String flow = flowMap.get(rightOp);
                userSelection.add(flow);
            } else {
                userSelection.add(rightOp);
            }
        }
    }
    return userSelection;
}

// to get a flow id's
// private static Set<String> getTableIdsFromFlows(Map<String, List<Flow>> flowMap, Map<String, Field> fields) {
//     Set<String> tableIds = new HashSet<>();
//     Set<String> visitedFlows = new HashSet<>();

//     for (String flowKey : flowMap.keySet()) {
//         collectTableIdsFromFlow(flowKey, flowMap, tableIds, visitedFlows,fields);
//     }

//     return tableIds;
// }

// private static void collectTableIdsFromFlow(
//         String key,Map<String, List<Flow>> flowMap, 
//         Set<String> tableIds,Set<String> visitedFlows, Map<String, Field> fields) {

//     List<Flow> flows = flowMap.get(key);

//     if (flows == null || visitedFlows.contains(key)) {
//         return;
//     }

//     visitedFlows.add(key);

//     for (Flow flow : flows) {
//         for (int i = 0; i < flow.getSource().size(); i++) {
//             String sourceType = flow.getSourceType().get(i);
//             String source = flow.getSource().get(i);

//             if ("field".equals(sourceType)) {
//                 Field field = fields.get(source);
//                 tableIds.add(field.getTableId());
//             } else if ("flow".equals(sourceType)) {
//                 collectTableIdsFromFlow(source, flowMap, tableIds, visitedFlows,fields);
//             }
//         }
//     }
// }


}