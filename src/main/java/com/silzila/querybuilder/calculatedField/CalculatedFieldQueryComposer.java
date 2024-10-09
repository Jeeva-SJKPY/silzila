package com.silzila.querybuilder.calculatedField;

import java.util.List;

import org.springframework.stereotype.Component;
import com.silzila.payload.request.CalculatedFieldRequest;


@Component
public class CalculatedFieldQueryComposer {
    public String composeQuery(List<CalculatedFieldRequest> requests, String vendor){

        QueryBuilder queryBuilder = QueryBuilderFactory.getQueryBuilder(vendor);

        return queryBuilder.build(requests);
    }
}
