package com.silzila.querybuilder.calculatedField;

import com.silzila.dto.DatasetDTO;
import com.silzila.payload.request.CalculatedFieldRequest;

public interface QueryBuilder {

    public void setSelectClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds);
    public void setFromClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds);
    public void setWhereClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds);
    public void setGroupByClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds);
    public void setOrderByClause(CalculatedFieldRequest calculatedFieldRequest, DatasetDTO ds);

    public String build();
}
