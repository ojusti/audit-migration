package fr.infologic.vei.audit.migration.xml;


import static com.mongodb.BasicDBObjectBuilder.start;
import static fr.infologic.vei.audit.migration.xml.DBObjectAssert.assertThat;

import java.util.Date;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.bson.BasicBSONObject;
import org.junit.Test;

import com.mongodb.DBObject;


public class XMLToMongoTest
{
    @Test
    public void transformString()
    {
        BasicBSONObject result = transform("<string>NEGOCE</string>");
        assertThat(result).containsExactly(e("string", "NEGOCE"));
    }
    
    @Test
    public void transformDecimal()
    {
        BasicBSONObject result = transform("<number>123</number>");
        assertThat(result).containsExactly(e("number", 123));
    }
    
    @Test
    public void transformReal()
    {
        BasicBSONObject result = transform("<number>123.2</number>");
        assertThat(result).containsExactly(e("number", 123.2));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void transformDate()
    {
        BasicBSONObject result = transform("<date>26/09/14</date>");
        assertThat(result).containsExactly(e("date", new Date(114, 8, 26, 0, 0)));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void transformTimestamp()
    {
        BasicBSONObject result = transform("<timestamp>03/12/14 15:32</timestamp>");
        assertThat(result).containsExactly(e("timestamp", new Date(114, 11, 3, 15, 32)));
    }
    
    @Test
    public void transformReference()
    {
        BasicBSONObject result = transform("<reference><code>a code</code></reference>");
        assertThat(result).valueOf("reference").containsExactly(e("code", "a code"));
    }
    
    @Test
    public void transformNull()
    {
        BasicBSONObject result = transform("<null/>");
        assertThat(result).valueOf("null").isEmpty();
    }

    @Test
    public void transformArray()
    {
        BasicBSONObject result = transform("<array><value>X</value><value>A</value></array>");
        assertThat(result).valueOf("array").asArray().containsExactly("X","A");
    }
    @Test
    public void transformSingletonArray()
    {
        BasicBSONObject result = transform("<array><value>X</value></array>");
        assertThat(result).valueOf("array").asArray().containsExactly("X");
    }
    
    @Test
    public void transformArrayOfObjects()
    {
        BasicBSONObject result = transform("<array><value><site><code>05</code></site><lieu><site><code>05</code></site><code>556</code></lieu></value>"
                                                + "<value><site><code>05</code></site><lieu><site><code>05</code></site><code>561</code></lieu></value></array>");
        assertThat(result).valueOf("array").asArray().containsExactly(lieu("05", 556), lieu("05", 561));
    }
    
    private static DBObject lieu(Object site, int lieu)
    {
        return start().push("lieu").add("code", lieu).push("site").add("code", site).pop().pop().push("site").add("code", site).pop().get();
    }
    
    @Test
    public void transformMapOfZV()
    {
        BasicBSONObject result = transform("<map><entry><key><code>001</code></key><value><famZv><code>001</code></famZv><famZvVal><famZv><code>001</code></famZv><code>001</code></famZvVal><valNum>5</valNum><valCle/><valDat/><valTexte/></value></entry>"
                                              + "<entry><key><code>002</code></key><value><famZv><code>002</code></famZv><famZvVal><famZv><code>002</code></famZv><code>001</code></famZvVal><valNum/><valCle/><valDat/><valTexte>ABC</valTexte></value></entry></map>");
        assertThat(result).valueOf("map").containsExactly(e("001", zv("001", 5)), e("002", zv("001", "ABC")));
    }
    
    private static DBObject zv(String codeVal, String valTexte)
    {
        return start("code", codeVal).push("valCle").pop().push("valDat").pop().push("valNum").pop().add("valTexte", valTexte).get();
    }
    private static DBObject zv(String codeVal, int valNum)
    {
        return start("code", codeVal).push("valCle").pop().push("valDat").pop().add("valNum", valNum).push("valTexte").pop().get();
    }
    
    @Test
    public void transformMapOfLangues()
    {
        BasicBSONObject result = transform("<langue><entry><key><code>02</code></key><value><langue><code>02</code></langue><libStd>A</libStd><libCourt>B</libCourt><libAbrege>C</libAbrege></value></entry>"
                                                 + "<entry><key><code>01</code></key><value><langue><code>01</code></langue><libStd>X</libStd><libCourt>Y</libCourt><libAbrege>Z</libAbrege></value></entry></langue>");
        assertThat(result).valueOf("langue").containsExactly(e("02", langue("A", "B", "C")), e("01", langue("X", "Y", "Z")));
    }
    
    private static DBObject langue(String libStd, String libCourt, String libAbrege)
    {
        return start("libAbrege", libAbrege).add("libCourt", libCourt).add("libStd", libStd).get();
    }
    
    @Test
    public void transformAuditInfoEtat()
    {
        BasicBSONObject result = transform("<auditInfo.etat>ACTIF</auditInfo.etat>");
        assertThat(result).containsExactly(e("etat", "ACTIF"));
    }
    
    
    @SuppressWarnings("deprecation")
    @Test
    public void transformAll()
    {
        BasicBSONObject result = transform("<string>NEGOCE</string>"
                                         + "<decimal>123</decimal>"
                                         + "<number>123.2</number>"
                                         + "<date>26/09/14</date>"
                                         + "<timestamp>03/12/14 15:32</timestamp>"
                                         + "<reference><code>a code</code></reference>"
                                         + "<null/>"
                                         + "<array><value>X</value><value>A</value></array>"
                                         + "<singleton><value>X</value></singleton>"
                                         + "<refArray><value><site><code>05</code></site><lieu><site><code>05</code></site><code>556</code></lieu></value>"
                                                   + "<value><site><code>05</code></site><lieu><site><code>05</code></site><code>561</code></lieu></value></refArray>"
                                         + "<map><entry><key><code>001</code></key><value><famZv><code>001</code></famZv><famZvVal><famZv><code>001</code></famZv><code>001</code></famZvVal><valNum>5</valNum><valCle/><valDat/><valTexte/></value></entry>"
                                              + "<entry><key><code>002</code></key><value><famZv><code>002</code></famZv><famZvVal><famZv><code>002</code></famZv><code>001</code></famZvVal><valNum/><valCle/><valDat/><valTexte>ABC</valTexte></value></entry></map>"
                                         + "<langue><entry><key><code>02</code></key><value><langue><code>02</code></langue><libStd>A</libStd><libCourt>B</libCourt><libAbrege>C</libAbrege></value></entry>"
                                                 + "<entry><key><code>01</code></key><value><langue><code>01</code></langue><libStd>X</libStd><libCourt>Y</libCourt><libAbrege>Z</libAbrege></value></entry></langue>"
                                         + "<auditInfo.etat>ACTIF</auditInfo.etat>");
        Assertions.assertThat(result.keySet()).hasSize(13).containsExactly("array", "date", "decimal", "etat", "langue", "map", "null", "number", "refArray", "reference", "singleton", "string", "timestamp");
        assertThat(result).scalar("string").isEqualTo("NEGOCE");
        assertThat(result).scalar("decimal").isEqualTo(123);
        assertThat(result).scalar("number").isEqualTo(123.2);
        assertThat(result).scalar("date").isEqualTo(new Date(114, 8, 26, 0, 0));
        assertThat(result).scalar("timestamp").isEqualTo(new Date(114, 11, 3, 15, 32));
        assertThat(result).valueOf("reference").containsExactly(e("code", "a code"));
        assertThat(result).valueOf("null").isEmpty();
        assertThat(result).valueOf("array").asArray().containsExactly("X", "A");
        assertThat(result).valueOf("singleton").asArray().containsExactly("X");
        assertThat(result).valueOf("refArray").asArray().containsExactly(lieu("05", 556), lieu("05", 561));
        assertThat(result).valueOf("map").containsExactly(e("001", zv("001", 5)), e("002", zv("001", "ABC")));
        assertThat(result).valueOf("langue").containsExactly(e("02", langue("A", "B", "C")), e("01", langue("X", "Y", "Z")));
        assertThat(result).scalar("etat").isEqualTo("ACTIF");
    }
    
    private static BasicBSONObject transform(String content)
    {
        return XMLToMongo.transform(document(content));
    }
    private static String document(String content) 
    {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><object class=\"fr.infologic.stocks.fichierbase.modele.Produit\">" + content + "</object>";
    }
    
    private static MapEntry e(String key, Object value)
    {
        return MapEntry.entry(key, value);
    }
    
}
