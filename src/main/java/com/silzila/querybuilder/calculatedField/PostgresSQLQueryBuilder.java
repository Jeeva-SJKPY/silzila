package com.silzila.querybuilder.calculatedField;

import com.silzila.dto.DatasetDTO;
import com.silzila.payload.request.CalculatedFieldRequest;

public class PostgresSQLQueryBuilder implements QueryBuilder{
    private String queryString = "";
    @Override
    public void setSelectClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds) {

    }

    @Override
    public void setFromClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds) {

    }

    @Override
    public void setWhereClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds) {

    }

    @Override
    public void setGroupByClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds) {

    }

    @Override
    public void setOrderByClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds) {

    }

    @Override
    public String build() {
        return null;
    }
}
