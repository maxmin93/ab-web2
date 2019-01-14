package net.bitnine.agensbrowser.web.util;

/*
 * Copyright (c) 2014-2017, Bitnine Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.*;

public class JsonbUtil {

    private JsonbUtil() {}

    public static final JSONObject parseJson(String jsonString) throws SQLException {

        jsonString = jsonString.trim();
        if( !jsonString.startsWith("{") || !jsonString.endsWith("}") || jsonString.equals("{}") )
            return new JSONObject();
        jsonString = jsonString.replace("\\\\", "");
        jsonString = jsonString.replace("\\\"", "'");

        Jsonb props = new Jsonb();
        props.setValue(jsonString);
        if (!(props.getJsonValue() instanceof JSONObject))
            throw new PSQLException("Parsing properties failed", PSQLState.DATA_ERROR);

        return (JSONObject) props.getJsonValue();
    }

    public static final Map<String,Object> jsonToMap(JSONObject props) throws SQLException {

        Map<String,Object> map = new HashMap<String,Object>();
        Set<Map.Entry<String,Object>> set = props.entrySet();
        for(Map.Entry<String,Object> entry : set){
            map.put( entry.getKey().toString(), entry.getValue() );
        }

        return map;
    }

    public static final Map<String,Object> parseJsonToMap(String jsonString) throws SQLException {

        JSONObject props = parseJson(jsonString);
        return jsonToMap(props);
    }

    public static final String typeof(Object value){
        if( value == null ) return null;
        else if( Boolean.class.isInstance(value) ) return Boolean.class.getName();
        else if( Integer.class.isInstance(value)) return Integer.class.getName();
        else if( Long.class.isInstance(value)) return Long.class.getName();
        else if( Float.class.isInstance(value)) return Float.class.getName();
        else if( Double.class.isInstance(value)) return Double.class.getName();
        else if( String.class.isInstance(value)) return String.class.getName();
        else if( JSONArray.class.isInstance(value)) return JSONArray.class.getName();
        else if( JSONObject.class.isInstance(value)) return JSONObject.class.getName();
        else if( Object.class.isInstance(value)) return value.getClass().getName();
        return "unknown";
    }

    public static final String simpleTypeof(Object value){
        String type = JsonbUtil.typeof(value);
        String simpleType = type == null ? "NULL" : type;
        switch (type){
            case "java.lang.Boolean":
                simpleType = "BOOLEAN";
                break;
            case "java.lang.Integer":
            case "java.lang.Long":
            case "java.lang.Float":
            case "java.lang.Double":
                simpleType = "NUMBER";
                break;
            case "java.lang.String":
                simpleType = "STRING";
                break;
        }
        return simpleType;
    }

    /*
    "{
        \"id\": 2,
        \"kind\": \"tv series\",
        \"title\": \"#1 Single\",
        \"md5sum\": \"e424d95c3d5c10fe98eb923a9b05d8da\",
        \"full_info\": [
            {\"certificates\": \"USA:TV-PG\"},
            {\"color info\": \"Color\"},
            {\"countries\": \"USA\"},
            {\"genres\": \"Reality-TV\"},
            {\"languages\": \"English\"},
            {\"locations\": \"New York City, New York, USA\"},
            {\"runtimes\": \"30\"},
            {\"sound mix\": \"Stereo\"},
            {\"release dates\": \"USA:22 January 2006\"}
            ],
        \"series_years\": \"2006-????\",
        \"phonetic_code\": \"S524\",
        \"production_year\": 2006
    }"
     */

}