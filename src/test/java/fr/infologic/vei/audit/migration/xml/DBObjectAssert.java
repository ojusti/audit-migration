package fr.infologic.vei.audit.migration.xml;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.ObjectAssert;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

public class DBObjectAssert extends MapAssert<String, Object>
{
    private BSONObject object;

    protected DBObjectAssert(BSONObject actual)
    {
        super(actual.toMap());
        this.object = actual;
    }

    public static DBObjectAssert assertThat(BSONObject result)
    {
        return new DBObjectAssert(result);
    }

    public DBObjectAssert valueOf(String key)
    {
        return new DBObjectAssert((BSONObject) object.get(key));
    }
    public ObjectAssert scalar(String key)
    {
        return Assertions.assertThat(object.get(key));
    }

    public ListAssert<Object> asArray()
    {
        return Assertions.assertThat((BasicBSONList) object);
    }

}
