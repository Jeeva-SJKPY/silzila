package com.silzila.querybuilder.calculatedField;

import java.util.List;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.payload.request.CalculatedFieldRequest;

public interface QueryBuilder {

    //handling multiple request
    public String build(List<CalculatedFieldRequest> requests) throws BadRequestException;

    //handling single request
    public String build(CalculatedFieldRequest request) throws BadRequestException;

    //handling single request to get sample records
    public String buildSampleRecordQuery(CalculatedFieldRequest request,DatasetDTO datasetDTO,Integer recordCount) throws BadRequestException;

    public void setDatasetDTOForAggregation(DatasetDTO datasetDTO);
}
