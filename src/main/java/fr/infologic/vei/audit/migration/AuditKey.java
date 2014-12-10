package fr.infologic.vei.audit.migration;

import fr.infologic.vei.audit.api.TrailKey;

class AuditKey implements TrailKey
{
    String metadataId;
    Long dossier;
    String ek;
    
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
        return dossier == null ? null : dossier.toString();
    }

    @Override
    public String getKey()
    {
        return ek;
    }
    
    
}
