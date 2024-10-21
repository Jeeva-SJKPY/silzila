package com.silzila.querybuilder.calculatedField.DateFlow;

import com.silzila.payload.request.Field;
import com.silzila.payload.request.Flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MySQLDateflow {
    private static Map<String,String> dateParts = Map.of("day","DAY","week","WEEK","month","MONTH","year","YEAR");

    private static Map<String,String> dateOperations = Map.of("currentDate", "CURRENT_DATE()","currentTimestamp","CURRENT_TIMESTAMP()","minDate","MIN","maxDate","MAX");

    public static String mySQLDateFlow(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap, String flowKey){
        return switch(flow.getFlow()) {
            case "stringToDate" -> stringToDateConversion(flow, fields, flowStringMap);
            case "addDateInterval" -> addDateInterval(flow, fields, flowStringMap);
            case "dateInterval" -> calculateDateInterval(flow, fields, flowStringMap);
            case "datePartName" -> getDatePartName(flow, fields, flowStringMap);
            case "datePartNumber" -> getDatePartNumber(flow, fields, flowStringMap);
            case "truncateDate" -> getTruncateDateToPart(flow, fields, flowStringMap);
            case "currentDate", "currentTimestamp" -> getCurrentDateOrTimeStamp(flow, fields, flowStringMap);
            default -> getMinOrMaxOfColumn(flow, fields, flowStringMap);
        };
    }

    // to process string to date conversion
    //1st source -> string, 2nd source -> date format
    private static String stringToDateConversion(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append("STR_TO_DATE (").append(processedSource.get(0)).append(",").append(processedSource.get(1)).append(")");

        return result.toString();
    }

    // add a interval to a date
    //1st source -> field or date, 2nd source -> number of date part , 3rd source -> date part(year,month,week,day)
    private static String addDateInterval(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append(" DATE_ADD(").append(processedSource.get(0)).append(" , INTERVAL ").append(flow.getSource().get(1)).append(" ").append(flow.getSource().get(2).toUpperCase()).append(")");

        return result.toString();
    }

    // difference between two dates
    //1st source -> field or date, 2nd source -> field or date , 3rd source -> result count in date part(year,month,week,day)
    private static String calculateDateInterval(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        String result = """
                            CASE 
                                WHEN 'day' = '%?' THEN DATEDIFF(%!,%&)
                                WHEN 'week' = '%?' THEN (DATEDIFF(%!,%&) / 7)
                                WHEN 'month' = '%?' THEN  TIMESTAMPDIFF(MONTH, %! , %&) 
                                WHEN 'year' = '%?' THEN  TIMESTAMPDIFF(YEAR, %! , %&) 
                            END
                """;
        result = result.replace("%!", processedSource.get(0))
                .replace("%&", processedSource.get(1))
                .replace("%?", flow.getSource().get(2));

        return result;
    }

    //to get the name of the date part
    //1st source -> field or date, 2nd source -> date part(month,day)
    private static String getDatePartName(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        String part = flow.getSource().get(1);

        if(part.equals("day")){
            part = "%W";
        }
        else if (part.equals("month")){
            part = "%M";
        }

        result.append("DATE_FORMAT(").append(processedSource.get(0)).append(", '").append(part).append("')");

        return result.toString();
    }

    //to get the number of the date part
    //1st source -> field or date, 2nd source -> date part(year,month,day)
    private static String getDatePartNumber(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append("EXTRACT(").append(dateParts.get(flow.getSource().get(1))).append(" FROM ").append(processedSource.get(0)).append(")");

        return result.toString();
    }

    //to truncate a date to a desired date part
    //1st source -> field or date, 2nd source -> date part(year,month,week)
    private static String getTruncateDateToPart(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        String part = flow.getSource().get(1);

        if(part.equals("year")){
            result.append("DATE_FORMAT(").append(processedSource.get(0)).append(",").append("'%Y-01-01')");
        }
        else if (part.equals("month")){
            result.append("DATE_FORMAT(").append(processedSource.get(0)).append(",").append("'%Y-%m-01')");
        }
        else if (part.equals("week")){
            result.append("DATE_SUB(").append(processedSource.get(0)).append(",INTERVAL (DAYOFWEEK(").append(processedSource.get(0)).append(") - 1) DAY)");
        }

        return result.toString();
    }

    //to get a current date or timestamp
    private static String getCurrentDateOrTimeStamp(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        return dateOperations.get(flow.getFlow());

    }

    //to get a min or max
    private static String getMinOrMaxOfColumn(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        return dateOperations.get(flow.getFlow()) + "(" + processedSource.get(0) + ")";

    }

    // to process a date sources
    private static List<String> processDateSources(Flow firstFlow, Map<String, Field> fields, Map<String, String> flowStringMap) {
        List<String> source = firstFlow.getSource();
        List<String> sourceType = firstFlow.getSourceType();
        List<String> resultString = new ArrayList<>();

        for (int i = 0; i < source.size(); i++) {
            String sourceElement = source.get(i);
            String type = sourceType.get(i);

            if ("field".equals(type)) {
                Field field = fields.get(sourceElement);
                resultString.add(field.getTableId() + "." + field.getFieldName());
            } else if ("flow".equals(type)) {
                resultString.add(flowStringMap.get(sourceElement));
            } else {

                resultString.add("'"+sourceElement+"'");
            }
        }

        return resultString;
    }

}

