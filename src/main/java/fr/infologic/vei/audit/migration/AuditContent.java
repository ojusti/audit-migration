package fr.infologic.vei.audit.migration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.zip.GZIPInputStream;

public class AuditContent extends AuditKey implements Comparable<AuditContent>
{
    Long sourceIK;
    private Long ik;
    Timestamp dat, datCre;
    String codeUtil;
    String ip;
    String guid;
    String typ = "CREATION";
    private byte[] xMLZip;

    @Override
    public String toString()
    {
        return String.format("AuditContent [metadataId=%s, sourceIK=%d, dat=%s, sourceDosResIK=%d, sourceEK=%s, codeUtil=%s, ip=%s, guid=%s, typ=%s, xml=%s]",
                             metadataId, sourceIK, dat, dossier, ek, codeUtil, ip, guid, typ, xml());
    }
    public void setSourceIK(Long sourceIK)
    {
        this.sourceIK = sourceIK;
    }
    public void setIk(Long ik)
    {
        this.ik = ik;
    }
    public void setDat(Timestamp dat)
    {
        this.dat = dat;
    }
    public void setDatCre(Timestamp dat)
    {
        this.dat = dat;
    }
    public void setCodeUtil(String codeUtil)
    {
        this.codeUtil = codeUtil;
    }
    public void setIp(String ip)
    {
        this.ip = ip;
    }
    public void setGuid(String guid)
    {
        this.guid = guid;
    }
    public void setxMLZip(Blob xMLZip)
    {
        if(xMLZip == null)
        {
            return;
        }
        try
        {
            this.xMLZip = xMLZip.getBytes(1, (int) xMLZip.length());
        }
        catch (SQLException e)
        {
            throw new MigrationAuditException(e);
        }
    }
    
    String xml()
    {
        if(xMLZip == null)
        {
            return "<object/>";
        }
        try(GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(xMLZip));
            ObjectInputStream reader = new ObjectInputStream(gzip))
        {
            return (String) reader.readObject();
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new MigrationAuditException(e);
        }
    }
    public void setTyp(int typ)
    {
        switch(typ)
        {
            case 0: this.typ = "CREATION"; break;
            case 1: this.typ = "MODIFICATION"; break;
            case 2: this.typ = "SUPPRESSION"; break;
        }
    }
    @Override
    public int compareTo(AuditContent o)
    {
        Timestamp date = dat == null ? datCre : dat;
        Timestamp otherDate = o.dat == null ? o.datCre : o.dat;
        return date != null && otherDate != null ? date.compareTo(otherDate) : ik.compareTo(o.ik);
    }
    
}
