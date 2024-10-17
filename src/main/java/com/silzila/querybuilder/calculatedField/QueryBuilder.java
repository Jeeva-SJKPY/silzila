package com.silzila.querybuilder.calculatedField;

import java.util.List;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.payload.request.CalculatedFieldRequest;

public interface QueryBuilder {

    //handling multiple request
    public String build(List<CalculatedFieldRequest> request);

    //handling single request
    public String buildSampleRecordQuery(CalculatedFieldRequest request,DatasetDTO datasetDTO,Integer recordCount) throws BadRequestException;
}
