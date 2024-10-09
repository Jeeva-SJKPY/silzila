package com.silzila.querybuilder.calculatedField;

import java.util.List;

import com.silzila.dto.DatasetDTO;
import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.querybuilder.calculatedField.selectClause.PostgresSelectClass;

public class PostgresSQLQueryBuilder implements QueryBuilder{
    
    private String queryString = "";
    
    //call postgres select class
    @Override
    public String build(List<CalculatedFieldRequest> requests) {
        return PostgresSelectClass.calculatedFieldsComposed(requests);
    }
}
