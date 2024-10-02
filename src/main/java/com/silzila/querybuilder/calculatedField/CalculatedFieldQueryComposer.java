package com.silzila.querybuilder.calculatedField;

import com.silzila.dto.DatasetDTO;
import com.silzila.payload.request.CalculatedFieldRequest;

public class CalculatedFieldQueryComposer {
    public String composeQuery(CalculatedFieldRequest request, DatasetDTO ds, String vendor){
        QueryBuilder queryBuilder = QueryBuilderFactory.getQueryBuilder(vendor);
        queryBuilder.setSelectClause(request, ds);
        queryBuilder.setFromClause(request, ds);
        queryBuilder.setWhereClause(request, ds);
        queryBuilder.setGroupByClause(request, ds);
        queryBuilder.setOrderByClause(request, ds);
        return queryBuilder.build();
    }
}
