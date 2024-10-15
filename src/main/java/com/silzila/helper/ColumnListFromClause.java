package com.silzila.helper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.payload.request.Field;
import com.silzila.payload.request.Query;

public class ColumnListFromClause {
    
    public static List<String> getColumnListFromQuery(Query req) {

        Set<String> uniqueTables = new HashSet<>();
    
        req.getDimensions().forEach(dim -> collectTableIds(dim.getTableId(), dim.getIsCalculatedField() ? dim.getCalculatedField().getFields() : null, uniqueTables));
    
        req.getMeasures().forEach(measure -> collectTableIds(measure.getTableId(), measure.getIsCalculatedField() ? measure.getCalculatedField().getFields() : null, uniqueTables));
        req.getFields().forEach(field -> uniqueTables.add(field.getTableId()));
        
        req.getFilterPanels().forEach(panel -> 
            panel.getFilters().forEach(filter -> collectTableIds(filter.getTableId(), filter.getIsCalculatedField() ? filter.getCalculatedField().getFields() : null, uniqueTables)));
    
        return new ArrayList<>(uniqueTables);
    }
    
    private static void collectTableIds(String tableId, Map<String, Field> fields, Set<String> uniqueTables) {
        if (tableId != null) {
            uniqueTables.add(tableId);
        }
        if (fields != null) {
            uniqueTables.addAll(getColumnListFromFields(fields));
        }
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
