package com.silzila.querybuilder.calculatedField;

import java.util.List;

import org.springframework.stereotype.Component;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.payload.request.CalculatedFieldRequest;


@Component
public class CalculatedFieldQueryComposer {

    public String composeQuery(List<CalculatedFieldRequest> requests, String vendor){

        QueryBuilder queryBuilder = QueryBuilderFactory.getQueryBuilder(vendor);

        return queryBuilder.build(requests);
    }

    public String composeSampleRecordQuery(CalculatedFieldRequest request ,String vendor, DatasetDTO datasetDTO,Integer recordCount) throws BadRequestException{

        QueryBuilder queryBuilder = QueryBuilderFactory.getQueryBuilder(vendor);

        return queryBuilder.buildSampleRecordQuery(request,datasetDTO,recordCount);
    }

    public String composeQuery(CalculatedFieldRequest calculatedFieldRequest, String vendor){

        QueryBuilder queryBuilder = QueryBuilderFactory.getQueryBuilder(vendor);

        return queryBuilder.build(calculatedFieldRequest);
    }

    public void setDatasetDTOForAggregation(DatasetDTO datasetDTO,String vendor){
        QueryBuilder queryBuilder = QueryBuilderFactory.getQueryBuilder(vendor);
        queryBuilder.setDatasetDTOForAggregation(datasetDTO);

    }
}
