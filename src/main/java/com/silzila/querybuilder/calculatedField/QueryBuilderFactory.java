package com.silzila.querybuilder.calculatedField;

import org.springframework.security.web.authentication.www.NonceExpiredException;

public class QueryBuilderFactory {

    public static QueryBuilder getQueryBuilder(String vendor){
        switch(vendor){
            case "DB2":
                return new DB2QueryBuilder();

            case "postgresssql":
                return new PostgresSQLQueryBuilder();
        }

        return null ;
    }
}
