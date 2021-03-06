package fr.infologic.vei.audit.migration.xml;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;

public class XMLToMongo
{
    public static BasicBSONObject transform(String document)
    {
        Map<String, Object> map = xmlToMaps(document);
        removeClass(map);
        renameEtat(map);
        return mapsToMongo(map);
    }

    private static Map<String, Object> xmlToMaps(String document)
    {
        return XML.toJSONObject(document).getJSONObject("object").map;
    }
    
    private static void removeClass(Map<String, Object> map)
    {
        map.remove("class");
    }
    
    private static void renameEtat(Map<String, Object> map)
    {
        Object etat = map.remove("auditInfo.etat");
        if(etat != null)
        {
            map.put("etat", etat);
        }
    }

    private static BasicBSONObject mapsToMongo(Map<String, Object> map)
    {
        BasicBSONObject result = new BasicBSONObject(map);
        for(Map.Entry<String, Object> entry : result.entrySet())
        {
            if(isObject(entry.getValue()))
            {
                entry.setValue(objectToMongo(entry));
            }
        }
        return result;
    }

    private static BSONObject objectToMongo(Map.Entry<String, Object> object)
    {
        Map<String, Object> content = ((JSONObject) object.getValue()).map;
        if(content.size() == 1)
        {
            Map.Entry<String, Object> singleEntry = content.entrySet().iterator().next();
            if(isMap(singleEntry))//{entry:{or[key:..., value:...}or]}
            {
                return mapToMongo(object.getKey(), singleEntry.getValue());
            }
            if(isArray(singleEntry))
            {
                return arrayToMongo(singleEntry.getValue());
            }
        }
        return mapsToMongo(content);
    }
    
    private static BasicBSONList arrayToMongo(Object content)
    {
        BasicBSONList result = new BasicBSONList();
        for(Object value : asList(content))
        {
            result.add(isObject(value) ? mapsToMongo(((JSONObject) value).map) : value);
        }
        return result;
    }

    private static BSONObject mapToMongo(String key, Object value)
    {
        if(isLangue(key))
        {
            return mapLanguesToMongo(value);
        }
        return mapZVToMongo(value);
    }
    
    private static BSONObject mapLanguesToMongo(Object value)
    {
        BasicDBObjectBuilder map = BasicDBObjectBuilder.start();
        for(JSONObject entry : (List<JSONObject>) asList(value))
        {
            map.push(String.valueOf(entry.getJSONObject("key").get("code")));
            JSONObject object = entry.getJSONObject("value");
            for(String key : object.keySet())
            {
                if(!isLangue(key))
                {    
                    map.add(key, scalarOrEmpty(object.get(key)));
                }
            }
            map.pop();
        }
        return map.get();
    }

    private static final String[] valuesZV = {"valCle", "valDat", "valNum", "valTexte"};
    private static BSONObject mapZVToMongo(Object value)
    {
        BasicDBObjectBuilder map = BasicDBObjectBuilder.start();
        for(JSONObject entry : (List<JSONObject>) asList(value))
        {
            map.push(String.valueOf(entry.getJSONObject("key").get("code")));
            JSONObject object = entry.getJSONObject("value");
            map.add("famZvVal", getCodeFamZvVal(object));
            for(String val : valuesZV)
            {
                map.add(val, scalarOrEmpty(object.get(val)));
            }
            map.pop();
        }
        return map.get();
    }

    private static Object getCodeFamZvVal(JSONObject object)
    {
        JSONObject famZvVal = object.optJSONObject("famZvVal");
        return scalarOrEmpty(famZvVal == null ? null : famZvVal.opt("code"));
    }

    private static Object scalarOrEmpty(Object object)
    {
        return object == null || isObject(object) ? new BasicDBObject() : object;
    }
    
    private static List<?> asList(Object value)
    {
        return isArray(value) ? ((JSONArray) value).list : Collections.singletonList(value);
    }

    private static boolean isArray(Object value)
    {
        return value instanceof JSONArray;
    }

    private static boolean isObject(Object value)
    {
        return value instanceof JSONObject;
    }

    private static boolean isArray(Map.Entry<String, Object> value)
    {
        return value.getKey().equals("value");
    }
    
    private static boolean isMap(Map.Entry<String, Object> value)
    {
        if(!value.getKey().equals("entry"))
        {
            return false;
        }
        Object innerValue = value.getValue();
        JSONObject entry;
        if(isArray(innerValue))
        {
            entry = ((JSONArray) innerValue).getJSONObject(0);
        }
        else
        {
            entry = (JSONObject) innerValue;
        }
        return entry.map.size() == 2 && entry.has("key") && entry.has("value");
    }
    
    private static boolean isLangue(String key)
    {
        return "langue".equals(key);
    }



}
