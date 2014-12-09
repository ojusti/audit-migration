package fr.infologic.vei.audit.migration.xml;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.ObjectAssert;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class DBObjectAssert extends MapAssert<String, Object>
{
    private DBObject object;

    protected DBObjectAssert(DBObject actual)
    {
        super(actual.toMap());
        this.object = actual;
    }

    public static DBObjectAssert assertThat(DBObject result)
    {
        return new DBObjectAssert(result);
    }

    public DBObjectAssert valueOf(String key)
    {
        return new DBObjectAssert((DBObject) object.get(key));
    }
    public ObjectAssert scalar(String key)
    {
        return Assertions.assertThat(object.get(key));
    }

    public ListAssert<Object> asArray()
    {
        return Assertions.assertThat((BasicDBList) object);
    }

}
