package com.silzila.querybuilder.calculatedField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.silzila.payload.request.Condition;
import com.silzila.payload.request.Field;
import com.silzila.payload.request.Filter;
import com.silzila.payload.request.Filter.DataType;

public class ConditionFilterToFilter {
    
    public static List<Filter> mapConditionFilterToFilter(List<Condition> conditions, Map<String, Field> fields, Map<String, String> flowMap) {

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
            setFlowFilter(leftOperand.get(0), filter, flowMap);
        } else {
            filter.setFieldName(leftOperand.get(0));
            filter.setIsField(false);
        }

        mapOperatorAndType(condition, filter);
        filter.setShouldExclude(condition.getShouldExclude());
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

private static void setFlowFilter(String leftOperand, Filter filter, Map<String, String> flowMap) {
    String flow = flowMap.get(leftOperand);
    filter.setFieldName(flow);
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
        filter.setOperator(Filter.Operator.fromValue("between"));
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

}