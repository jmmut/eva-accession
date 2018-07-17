/*
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
package uk.ac.ebi.eva.accession.dbsnp.persistence;

import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantOperationEntity;

import java.util.List;

public class DbsnpVariantsWrapper {

    private List<? extends ISubmittedVariant> submittedVariants;

    private IClusteredVariant clusteredVariants;

    private List<SubmittedVariantOperationEntity> operations;

    public DbsnpVariantsWrapper() {
    }

    public List<? extends ISubmittedVariant> getSubmittedVariants() {
        return submittedVariants;
    }

    public void setSubmittedVariants(List<? extends ISubmittedVariant> submittedVariants) {
        this.submittedVariants = submittedVariants;
    }

    public IClusteredVariant getClusteredVariants() {
        return clusteredVariants;
    }

    public void setClusteredVariants(IClusteredVariant clusteredVariants) {
        this.clusteredVariants = clusteredVariants;
    }

    public List<SubmittedVariantOperationEntity> getOperations() {
        return operations;
    }

    public void setOperations(
            List<SubmittedVariantOperationEntity> operations) {
        this.operations = operations;
    }
}