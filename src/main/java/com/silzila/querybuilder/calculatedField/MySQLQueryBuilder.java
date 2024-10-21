package com.silzila.querybuilder.calculatedField;

import java.util.List;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.querybuilder.calculatedField.selectClause.MySQLCalculatedField;


public class MySQLQueryBuilder implements QueryBuilder{
    
    
    @Override
    public String build(List<CalculatedFieldRequest> requests) {
        return MySQLCalculatedField.calculatedFieldsComposed(requests);
    }

    @Override
    public String buildSampleRecordQuery(CalculatedFieldRequest request,DatasetDTO datasetDTO,Integer recordCount) throws BadRequestException {
        return MySQLCalculatedField.composeSampleRecordQuery(request, datasetDTO, recordCount);

    }

    public String build(CalculatedFieldRequest request) {
        return MySQLCalculatedField.calculatedFieldComposed(request);
    }
}
