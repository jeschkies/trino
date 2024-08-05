package io.trino.loki.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Data
{

    public String getResultType()
    {
        return resultType;
    }

    public void setResultType(String resultType)
    {
        this.resultType = resultType;
    }

    private String resultType;

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "resultType", include = JsonTypeInfo.As.EXTERNAL_PROPERTY)
    @JsonSubTypes(value = {
            @JsonSubTypes.Type(value = Streams.class, name = "streams"),
            @JsonSubTypes.Type(value = Matrix.class, name = "matrix")
    })
    private QueryResult.Result result;

    public QueryResult.Result getResult()
    {
        return result;
    }

    public void setResult(QueryResult.Result result)
    {
        this.result = result;
    }
}
