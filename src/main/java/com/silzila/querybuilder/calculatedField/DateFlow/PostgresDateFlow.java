package com.silzila.querybuilder.calculatedField.DateFlow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.silzila.exception.BadRequestException;
import com.silzila.payload.request.Field;
import com.silzila.payload.request.Flow;

public class PostgresDateFlow {

    private static Map<String,String> dateParts = Map.of("day","DAY","week","WEEK","month","MONTH","year","YEAR");

    private static Map<String,String> dateOperations = Map.of("currentDate", "CURRENT_DATE","currentTimestamp","NOW()","minDate","MIN","maxDate","MAX");

    public static String postgresDateFlow(Flow flow, Map<String, Field> fields, Map<String, String> flowStringMap, String flowKey) throws BadRequestException{
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
    private static String stringToDateConversion(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap) throws BadRequestException{

        if (flow.getSource().size() != 2) {
            throw new BadRequestException("Invalid parameters: String to date operation requires exactly two parameters (field,date format).");
        }

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append("TO_DATE (").append(processedSource.get(0)).append(",").append(processedSource.get(1)).append(")");

        return result.toString();
    }

    // add a interval to a date
    //1st source -> field or date, 2nd source -> number of date part , 3rd source -> date part(year,month,week,day) 
    private static String addDateInterval(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap) throws BadRequestException{

        if (flow.getSource().size() != 3) {
            throw new BadRequestException("Invalid parameters: add date interval operation requires exactly three parameters.");
        }

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append(processedSource.get(0)).append("::DATE + INTERVAL '").append(flow.getSource().get(1)).append(" ").append(flow.getSource().get(2).toUpperCase()).append("'");

        return result.toString();
    }

    // difference between two dates
    //1st source -> field or date, 2nd source -> field or date , 3rd source -> result count in date part(year,month,week,day)
    private static String calculateDateInterval(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap) throws BadRequestException{

        
        if (flow.getSource().size() != 3) {
            throw new BadRequestException("Invalid parameters: date interval operation requires exactly three parameters (field,string,string).");
        }

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        String result = """
                            CASE 
                                WHEN 'day' = '%?' THEN ( %!::DATE - %&::DATE)::INTEGER
                                WHEN 'week' = '%?' THEN (( %!::DATE - %&::DATE) / 7)::INTEGER
                                WHEN 'month' = '%?' THEN  (EXTRACT(YEAR FROM AGE( %!::DATE, %&::DATE)) * 12) +
                                    EXTRACT(MONTH FROM AGE( %!::DATE, %&::DATE))
                                WHEN 'year' = '%?' THEN EXTRACT(YEAR FROM AGE( %!::DATE, %&::DATE))
                            END
                """;
        result = result.replace("%!", processedSource.get(0))
                .replace("%&", processedSource.get(1))
                .replace("%?", flow.getSource().get(2));

        return result;
    }

    //to get the name of the date part 
    //1st source -> field or date, 2nd source -> date part(month,day) 
    private static String getDatePartName(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap) throws BadRequestException{
        
        if (flow.getSource().size() != 2) {
            throw new BadRequestException("Invalid parameters: date partname operation requires exactly two parameters.");
        }

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append("TO_CHAR(").append(processedSource.get(0)).append("::DATE, '").append(dateParts.get(flow.getSource().get(1))).append("')");

        return result.toString();
    }

    //to get the number of the date part 
    //1st source -> field or date, 2nd source -> date part(year,month,day) 
    private static String getDatePartNumber(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap) throws BadRequestException{

        
        if (flow.getSource().size() != 2) {
            throw new BadRequestException("Invalid parameters: Text replace operation requires exactly two parameters.");
        }

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append("EXTRACT(").append(dateParts.get(flow.getSource().get(1))).append(" FROM ").append(processedSource.get(0)).append("::DATE)");

        return result.toString();
    }

    //to truncate a date to a desired date part
    //1st source -> field or date, 2nd source -> date part(year,month,week) 
    private static String getTruncateDateToPart(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap) throws BadRequestException{
        
        if (flow.getSource().size() != 2) {
            throw new BadRequestException("Invalid parameters: Text replace operation requires exactly two parameters.");
        }

        StringBuilder result = new StringBuilder();

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        result.append("DATE_TRUNC('").append(dateParts.get(flow.getSource().get(1))).append("',").append(processedSource.get(0)).append("::DATE)");

        return result.toString();
    }

    //to get a current date or timestamp
    private static String getCurrentDateOrTimeStamp(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap){

        return dateOperations.get(flow.getFlow());

    }

    //to get a min or max 
    private static String getMinOrMaxOfColumn(Flow flow,Map<String, Field> fields,Map<String, String> flowStringMap) throws BadRequestException{

        List<String> processedSource = processDateSources(flow, fields, flowStringMap);

        return dateOperations.get(flow.getFlow()) + "(" + processedSource.get(0) + ")";  
        
    }

    // to process a date sources
    private static List<String> processDateSources(Flow firstFlow, Map<String, Field> fields, Map<String, String> flowStringMap) throws BadRequestException {
        List<String> source = firstFlow.getSource();
        List<String> sourceType = firstFlow.getSourceType();
        List<String> resultString = new ArrayList<>();
    
        for (int i = 0; i < source.size(); i++) {
            String sourceElement = source.get(i);
            String type = sourceType.get(i);
    
            if ("field".equals(type)) {
                Field field = fields.get(sourceElement);
                if(field==null){
                    throw new BadRequestException("No such a field with an id:" + sourceElement);
                }
                resultString.add(field.getTableId() + "." + field.getFieldName());
            } else if ("flow".equals(type)) {
                String sourceFlowValue = flowStringMap.get(sourceElement);
                if(sourceFlowValue == null || sourceFlowValue.isEmpty()){
                    throw new BadRequestException("No such a flow with an id:" + sourceElement);
                }
                resultString.add(sourceFlowValue);
            } else {
                resultString.add("'"+sourceElement+"'");
            }
        }
    
        return resultString;
    }
    
}
