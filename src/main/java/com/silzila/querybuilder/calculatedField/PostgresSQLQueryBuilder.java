package com.silzila.querybuilder.calculatedField;

import java.util.List;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.payload.request.CalculatedFieldRequest;
import com.silzila.querybuilder.calculatedField.selectClause.PostgresCalculatedField;

public class PostgresSQLQueryBuilder implements QueryBuilder{
    
    //call postgres select class
    @Override
    public String build(List<CalculatedFieldRequest> requests) throws BadRequestException {
        return PostgresCalculatedField.calculatedFieldsComposed(requests);
    }

    @Override
    public String build(CalculatedFieldRequest request) throws BadRequestException {
        return PostgresCalculatedField.calculatedFieldComposed(request);
    }

    @Override
    public String buildSampleRecordQuery(CalculatedFieldRequest request,DatasetDTO datasetDTO,Integer recordCount) throws BadRequestException{
        return PostgresCalculatedField.composeSampleRecordQuery(request, datasetDTO,recordCount);
    }

    @Override 
    public void setDatasetDTOForAggregation(DatasetDTO datasetDTO){
        PostgresCalculatedField.setDatasetForAggregation(datasetDTO);
    }
}
