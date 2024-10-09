package com.silzila.querybuilder.calculatedField;

import java.util.List;

import com.silzila.payload.request.CalculatedFieldRequest;

public interface QueryBuilder {

    //
    public String build(List<CalculatedFieldRequest> request);
}
