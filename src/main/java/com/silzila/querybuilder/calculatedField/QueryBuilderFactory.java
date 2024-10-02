package com.silzila.querybuilder.calculatedField;

import org.springframework.security.web.authentication.www.NonceExpiredException;

public class QueryBuilderFactory {

    public static QueryBuilder getQueryBuilder(String vendor){
        if(vendor.equals("DB2")){
            return new DB2QueryBuilder();
        }
        return null ;
    }
}
