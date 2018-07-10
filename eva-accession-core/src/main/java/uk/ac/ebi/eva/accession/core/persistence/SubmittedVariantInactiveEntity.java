/*
 *
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package uk.ac.ebi.eva.accession.core.persistence;

import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.utils.ISubmittedVariantComparator;

@Document
public class SubmittedVariantInactiveEntity extends InactiveSubDocument<Long> implements ISubmittedVariant {

    @Field("asm")
    private String assemblyAccession;

    @Field("tax")
    private int taxonomyAccession;

    @Field("study")
    private String projectAccession;

    private String contig;

    private long start;

    @Field("ref")
    private String referenceAllele;

    @Field("alt")
    private String alternateAllele;

    @Field("rs")
    private Long clusteredVariantAccession;

    @Field("evidence")
    private Boolean supportedByEvidence;

    @Field("asmMatch")
    private Boolean assemblyMatch;

    @Field("allelesMatch")
    private Boolean allelesMatch;

    @Field("validated")
    private Boolean validated;

    SubmittedVariantInactiveEntity() {
    }

    public SubmittedVariantInactiveEntity(SubmittedVariantEntity submittedVariantEntity) {
        super(submittedVariantEntity);
        this.assemblyAccession = submittedVariantEntity.getAssemblyAccession();
        this.taxonomyAccession = submittedVariantEntity.getTaxonomyAccession();
        this.projectAccession = submittedVariantEntity.getProjectAccession();
        this.contig = submittedVariantEntity.getContig();
        this.start = submittedVariantEntity.getStart();
        this.referenceAllele = submittedVariantEntity.getReferenceAllele();
        this.alternateAllele = submittedVariantEntity.getAlternateAllele();
        this.clusteredVariantAccession = submittedVariantEntity.getClusteredVariantAccession();
        this.supportedByEvidence = submittedVariantEntity.isSupportedByEvidence();
        this.assemblyMatch = submittedVariantEntity.isAssemblyMatch();
        this.allelesMatch = submittedVariantEntity.isAllelesMatch();
        this.validated = submittedVariantEntity.isValidated();
    }

    @Override
    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    @Override
    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    @Override
    public String getProjectAccession() {
        return projectAccession;
    }

    @Override
    public String getContig() {
        return contig;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public String getReferenceAllele() {
        return referenceAllele;
    }

    @Override
    public String getAlternateAllele() {
        return alternateAllele;
    }

    @Override
    public Long getClusteredVariantAccession() {
        return clusteredVariantAccession;
    }

    @Override
    public Boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }

    @Override
    public Boolean isAssemblyMatch() {
        return assemblyMatch;
    }

    @Override
    public Boolean isAllelesMatch() {
        return allelesMatch;
    }

    @Override
    public Boolean isValidated() {
        return validated;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ISubmittedVariant)) {
            return false;
        }

        return ISubmittedVariantComparator.equals(this, (ISubmittedVariant) o);
    }

    @Override
    public int hashCode() {
        return ISubmittedVariantComparator.hashCode(this);
    }

}
