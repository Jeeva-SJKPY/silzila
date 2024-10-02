package com.silzila.payload.request;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import javax.annotation.Generated;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)

@Generated("jsonschema2pojo")
@AllArgsConstructor
@NoArgsConstructor
public class Fields implements Serializable {

    @JsonProperty("fieldMap")
    private Map<String, Field> fieldMap = new HashMap<>();

    // private final static long serialVersionUID = -7207259868710547121L;

    public Map<String, Field> getFieldMap() {
        return fieldMap;
    }

    public void setFieldMap(Map<String, Field> fieldMap) {
        this.fieldMap = fieldMap;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Fields:");
        sb.append("[");
        for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
            sb.append(  entry.getKey() + ":" + entry.getValue());
            sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

}

