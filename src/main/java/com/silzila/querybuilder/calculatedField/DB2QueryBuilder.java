package com.silzila.querybuilder.calculatedField;

import java.util.List;

import com.silzila.dto.DatasetDTO;
import com.silzila.payload.request.CalculatedFieldRequest;

public class DB2QueryBuilder implements QueryBuilder{
    private String queryString = "";
    
    
    @Override
    public String build(List<CalculatedFieldRequest> requests) {
        return null;
    }
}