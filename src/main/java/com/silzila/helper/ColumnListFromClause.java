package com.silzila.helper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.payload.request.Field;
import com.silzila.payload.request.Query;

public class ColumnListFromClause {
    
    public static List<String> getColumnListFromQuery(Query req){

        Set<String> allColumnList = new HashSet<>();
        // take list of unique dim tables & another list on all unique tables
        req.getDimensions().forEach((dim) -> allColumnList.add(dim.getTableId()));
        // take list of unique measure tables & another list on all unique tables
        req.getMeasures().forEach((measure) -> allColumnList.add(measure.getTableId()));
        // take list of unique field tables & another list on all unique tables
        req.getFields().forEach((field) -> allColumnList.add(field.getTableId()));
        // take list of unique filter tables & another list on all unique tables
        req.getFilterPanels().forEach((panel) -> {
            panel.getFilters().forEach((filter) ->allColumnList.add(filter.getTableId()));
        });

        return new ArrayList<>(allColumnList);

    }

    public static List<String> getColumnListFromFields(Map<String, Field> fields) {
        return fields.values().stream()
            .map(Field::getTableId)
            .distinct()
            .collect(Collectors.toList()); 
    }
    
    public static List<String> getColumnListFromFieldsRequest(List<CalculatedFieldRequest> calculatedFieldRequests) {
        return calculatedFieldRequests.stream()
            .flatMap(request -> getColumnListFromFields(request.getFields()).stream())
            .distinct()
            .collect(Collectors.toList());
    }
    
    

}
