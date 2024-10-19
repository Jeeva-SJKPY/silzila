package com.silzila.querybuilder.filteroptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.silzila.dto.DatasetDTO;
import com.silzila.exception.BadRequestException;
import com.silzila.helper.ColumnListFromClause;
import com.silzila.payload.request.ColumnFilter;
import com.silzila.payload.request.Table;
import com.silzila.querybuilder.RelationshipClauseGeneric;
import com.silzila.querybuilder.calculatedField.selectClause.PostgresCalculatedField;

public class FilterQueryPostgres {

    private static final Logger logger = LogManager.getLogger(FilterQueryPostgres.class);

    public static String getFilterOptions(ColumnFilter req, Table table,DatasetDTO datasetDTO) throws BadRequestException {
        logger.info("=========== FilterQueryPostgres fn calling...");
        /*
         * ************************************************
         * get distinct values - binary, text & number fields
         * ************************************************
         */
        String query = "";
        String fromClause="";
        //if table is null getting information from column filter request directly
        if (table == null) {
            fromClause = " FROM " + req.getSchemaName() + "." + req.getTableName() + " AS " + req.getTableId()+" ";
        }
        else if(req.getIsCalculatedField()){
            //generating fromclause
            List<String> allColumnList = (req.getCalculatedField()!=null) 
                                         ? ColumnListFromClause.getColumnListFromFields(req.getCalculatedField().getFields()) 
                                         : new ArrayList<>();
            if(!allColumnList.contains(req.getTableId())){
                    allColumnList.add(req.getTableId());
            }
            fromClause =" FROM " + RelationshipClauseGeneric.buildRelationship(allColumnList,datasetDTO.getDataSchema(),"postgresql") + " ";
        }
        else{
        if(!table.isCustomQuery()) {
            fromClause = " FROM " + table.getSchema() + "." + table.getTable() + " AS " +table.getId()+" ";
        }else{
            fromClause= " FROM (" + table.getCustomQuery() + ") AS "+ table.getId()+" ";
        }
        }

        String selectField = req.getIsCalculatedField()? PostgresCalculatedField.calculatedFieldComposed(req.getCalculatedField()) : req.getTableId()+ "."  + req.getFieldName();
        if (req.getIsCalculatedField()) {
                String flowKey = "";
                for (String key : req.getCalculatedField().getFlows().keySet()) {
                    flowKey = key;  
                }
                req.setDataType(ColumnFilter.DataType.fromValue(
                    PostgresCalculatedField.getDataType(
                        req.getCalculatedField().getFlows(), 
                        req.getCalculatedField().getFields(), 
                        req.getCalculatedField().getFlows().get(flowKey).get(0)
                    )
                ));
            }

        if (List.of("TEXT", "BOOLEAN").contains(req.getDataType().name())) {
            query = "SELECT DISTINCT " + selectField + fromClause + "ORDER BY 1";
        }

        /*
         * ************************************************
         * get distinct & Range values - number fields
         * ************************************************
         */
        else if (List.of("INTEGER", "DECIMAL").contains(req.getDataType().name())) {

            if (!Objects.isNull(req.getFilterOption())) {
                // get distinct values
                if (req.getFilterOption().name().equals("ALL_VALUES")) {
                    query = "SELECT DISTINCT " + selectField + fromClause + "ORDER BY 1";
                }
                // get Range values
                else if (req.getFilterOption().name().equals("MIN_MAX")) {
                    query = "SELECT MIN("  + selectField + ") AS min, MAX("
                            + selectField + ") AS max" + fromClause;
                }
                // if filter option is not provided, throw error
            } else {
                throw new BadRequestException("filterOption cannot be empty for number fields!");
            }
        }

        /*
         * ************************************************
         * DATE - dictinct values & Search
         * ************************************************
         */
        else if (List.of("DATE", "TIMESTAMP").contains(req.getDataType().name())) {
            // if Time grain is empty then throw error
            if (Objects.isNull(req.getTimeGrain())) {
                throw new BadRequestException("Error: Date/Timestamp Column should have Time Grain!");
            }
            /*
             * Date - dictinct values
             */
            if (req.getFilterOption().name().equals("ALL_VALUES")) {
                if (req.getTimeGrain().name().equals("YEAR")) {
                    String field = "EXTRACT(YEAR FROM " + selectField
                            + ")::INTEGER AS Year";
                    query = "SELECT DISTINCT " + field + fromClause + "ORDER BY 1";
                } else if (req.getTimeGrain().name().equals("QUARTER")) {
                    String field = "CONCAT('Q', EXTRACT(QUARTER FROM " + selectField + ")::INTEGER) AS Quarter";
                    query = "SELECT DISTINCT " + field + fromClause + "ORDER BY 1";
                } else if (req.getTimeGrain().name().equals("MONTH")) {
                    String sortField = "EXTRACT(MONTH FROM " + selectField
                            + ")::INTEGER";
                    String field = "TRIM(TO_CHAR(" + selectField + ", 'Month'))";
                    query = "SELECT " + field + " AS Month" + fromClause + "GROUP BY " + sortField + ", " + field
                            + " ORDER BY " + sortField;
                } else if (req.getTimeGrain().name().equals("YEARQUARTER")) {
                    String field = "TO_CHAR(" + selectField
                            + ", 'YYYY') || '-Q' || TO_CHAR(" + selectField + ", 'Q')";
                    query = "SELECT DISTINCT " + field + " AS YearQuarter" + fromClause + "ORDER BY 1";
                } else if (req.getTimeGrain().name().equals("YEARMONTH")) {
                    String field = "TO_CHAR(" + selectField + ", 'YYYY-MM')";
                    query = "SELECT DISTINCT " + field + " AS YearMonth" + fromClause + "ORDER BY 1";
                } else if (req.getTimeGrain().name().equals("DATE")) {
                    String field = "DATE(" + selectField + ")";
                    query = "SELECT DISTINCT " + field + " AS Date" + fromClause + "ORDER BY 1";
                }
                // in postgres, dayofweek starts from 0. So we add +1 to be consistent across DB
                else if (req.getTimeGrain().name().equals("DAYOFWEEK")) {
                    String sortField = "EXTRACT(DOW FROM " + selectField
                            + ")::INTEGER +1";
                    String field = "TRIM(TO_CHAR(" + selectField + ", 'Day'))";
                    query = "SELECT " + field + " AS DayOfWeek" + fromClause + "GROUP BY " + sortField + ", " + field
                            + " ORDER BY " + sortField;
                } else if (req.getTimeGrain().name().equals("DAYOFMONTH")) {
                    String field = "EXTRACT(DAY FROM " + selectField
                            + ")::INTEGER AS DayOfMonth";
                    query = "SELECT DISTINCT " + field + fromClause + "ORDER BY 1";
                }

            }
            /*
             * Date - Search (Min & Max only)
             */
            else if (req.getFilterOption().name().equals("MIN_MAX")) {
                if (req.getTimeGrain().name().equals("YEAR")) {
                    String col = "EXTRACT(YEAR FROM " + selectField + ")::INTEGER";
                    query = "SELECT MIN(" + col + ") AS min, MAX(" + col + ") AS max" + fromClause;
                } else if (req.getTimeGrain().name().equals("QUARTER")) {
                    String col = "EXTRACT(QUARTER FROM " + selectField + ")::INTEGER";
                    query = "SELECT MIN(" + col + ") AS min, MAX(" + col + ") AS max" + fromClause;
                } else if (req.getTimeGrain().name().equals("MONTH")) {
                    String col = "EXTRACT(MONTH FROM " + selectField + ")::INTEGER";
                    query = "SELECT MIN(" + col + ") AS min, MAX(" + col + ") AS max" + fromClause;
                } else if (req.getTimeGrain().name().equals("DATE")) {
                    String col = "DATE(" + selectField + ")";
                    query = "SELECT MIN(" + col + ") AS min, MAX(" + col + ") AS max" + fromClause;
                } else if (req.getTimeGrain().name().equals("DAYOFWEEK")) {
                    String col = "EXTRACT(DOW FROM " + selectField + ")::INTEGER +1";
                    query = "SELECT MIN(" + col + ") AS min, MAX(" + col + ") AS max" + fromClause;
                } else if (req.getTimeGrain().name().equals("DAYOFMONTH")) {
                    String col = "EXTRACT(DAY FROM " + selectField + ")::INTEGER";
                    query = "SELECT MIN(" + col + ") AS min, MAX(" + col + ") AS max" + fromClause;
                }
            }
        } else {
            throw new BadRequestException("Error: Wrong combination of Data Type & Filter Option!");
        }
        return query;

    }

}
