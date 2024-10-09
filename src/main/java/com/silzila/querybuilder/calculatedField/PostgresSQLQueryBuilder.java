package com.silzila.querybuilder.calculatedField;

import com.silzila.dto.DatasetDTO;
import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.querybuilder.calculatedField.selectClause.PostgresSelectClass;

public class PostgresSQLQueryBuilder implements QueryBuilder{
    
    private String queryString = "";
    
    //call postgres select class
    @Override
    public String build(CalculatedFieldRequest request) {
        return PostgresSelectClass.calculatedFieldComposed(request);
    }
}
