package fr.infologic.vei.audit.migration.xml;

import static fr.infologic.vei.audit.migration.xml.DBObjectAssert.assertThat;
import static fr.infologic.vei.audit.migration.xml.XMLToMongoTest.e;

import org.bson.BasicBSONObject;
import org.junit.Test;

public class XMLToMongo_CornerCasesTest
{

    @Test
    public void testCDATA()
    {
        String xml = "<contenuRegleDecodage><![CDATA[(?s)(.*)]]></contenuRegleDecodage>";
        
        BasicBSONObject result = XMLToMongoTest.transform(xml);
        
        assertThat(result).scalar("contenuRegleDecodage").isEqualTo("(?s)(.*)").isInstanceOf(String.class);
    }
    
    @Test
    public void transformReal()
    {
        BasicBSONObject result = XMLToMongoTest.transform("<number>123.200000</number>");
        assertThat(result).containsExactly(e("number", 123.2));
    }
    
    @Test
    public void testDoubleCDATA()
    {
        String xml = "<contenuRegleConversion><![CDATA[A![CDATA[B]]]]><![CDATA[C]]></contenuRegleConversion>";
        BasicBSONObject result = XMLToMongoTest.transform(xml);
        assertThat(result).scalar("contenuRegleConversion").isEqualTo("A![CDATA[B]]C").isInstanceOf(String.class);
    }
    
    @Test
    public void testNULL()
    {
        String xml = "<champVide>NULL</champVide>";
        BasicBSONObject result = XMLToMongoTest.transform(xml);
        assertThat(result).scalar("champVide").isEqualTo("NULL").isInstanceOf(String.class);
    }
}