/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
 */
package uk.ac.ebi.eva.accession.release.batch.io.deprecated;

import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.batch.io.MongoDbCursorItemReader;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;
import uk.ac.ebi.eva.accession.release.collectionNames.EvaCollectionNames;

public class DeprecatedVariantMongoReader extends MongoDbCursorItemReader<
        EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>> {

    public static DeprecatedVariantMongoReader evaDeprecatedVariantMongoReader(String assemblyAccession,
                                                                               MongoTemplate mongoTemplate) {
        return new DeprecatedVariantMongoReader(assemblyAccession, mongoTemplate, new EvaCollectionNames(),
                                                ClusteredVariantOperationEntity.class);
    }

    public static DeprecatedVariantMongoReader dbsnpDeprecatedVariantMongoReader(String assemblyAccession,
                                                                                 MongoTemplate mongoTemplate) {
        return new DeprecatedVariantMongoReader(assemblyAccession, mongoTemplate, new EvaCollectionNames(),
                                                DbsnpClusteredVariantOperationEntity.class);
    }

    public DeprecatedVariantMongoReader(String assemblyAccession, MongoTemplate mongoTemplate, CollectionNames names,
                                        Class<? extends EventDocument<IClusteredVariant, Long,
                                                ? extends ClusteredVariantInactiveEntity>> clusteredVariantOperationEntityClass) {
        setCollection(names.getClusteredVariantOperationEntity());
        setTemplate(mongoTemplate);
        setTargetType(clusteredVariantOperationEntityClass);

        setQuery(String.format("{ \"inactiveObjects.asm\" : \"%s\", eventType : \"%s\" }", assemblyAccession,
                               EventType.DEPRECATED));
    }

}
