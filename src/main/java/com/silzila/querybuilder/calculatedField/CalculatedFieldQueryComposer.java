package com.silzila.querybuilder.calculatedField;

import org.springframework.stereotype.Component;
import com.silzila.payload.request.CalculatedFieldRequest;


@Component
public class CalculatedFieldQueryComposer {
    public String composeQuery(CalculatedFieldRequest request, String vendor){

        QueryBuilder queryBuilder = QueryBuilderFactory.getQueryBuilder(vendor);

        return queryBuilder.build(request);
    }
}
