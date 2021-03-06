package fr.infologic.vei.audit.migration;

import fr.infologic.vei.audit.api.TrailKey;

public class AuditKey implements TrailKey
{
    String metadataId;
    Long dossier;
    String ek;
    private boolean hasDossier = true;
    
    public void setMetadataId(String metadataId) 
    {
        this.metadataId = metadataId;
    }

    public void setSourceDosResIK(Long dossier)
    {
        this.dossier = dossier;
    }
    public void setSourceEK(String ek)
    {
        this.ek = ek;
    }

    @Override
    public String getType()
    {
        return metadataId;
    }

    @Override
    public String getGroup()
    {
        return !hasDossier || dossier == null ? null : dossier.toString();
    }

    @Override
    public String getKey()
    {
        return ek;
    }
    @Override
    public String toString()
    {
        return String.format("AuditKey [metadataId=%s, sourceDosResIK=%s, sourceEK=%s]",
                         getType(), getGroup(), getKey());
    }

    boolean hasDossier()
    {
        return hasDossier;
    }

    AuditKey withoutDossier()
    {
        hasDossier = false;
        return this;
    }
    
}
