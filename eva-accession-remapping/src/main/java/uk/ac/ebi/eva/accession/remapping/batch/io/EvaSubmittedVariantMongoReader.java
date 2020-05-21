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
package uk.ac.ebi.eva.accession.remapping.batch.io;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;

import uk.ac.ebi.eva.accession.core.batch.io.MongoDbCursorItemReader;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class EvaSubmittedVariantMongoReader extends MongoDbCursorItemReader<SubmittedVariantEntity> {

    public static final String PROJECT_FIELD = getProjectField();

    public static final String REFERENCE_SEQUENCE_FIELD = getReferenceSequenceField();

    public EvaSubmittedVariantMongoReader(String assemblyAccession, MongoTemplate mongoTemplate) {
        setTemplate(mongoTemplate);
        setTargetType(SubmittedVariantEntity.class);
        setQuery(query(where(REFERENCE_SEQUENCE_FIELD).is(assemblyAccession)));
    }

    public EvaSubmittedVariantMongoReader(String assemblyAccession, MongoTemplate mongoTemplate,
                                          List<String> projects) {
        setTemplate(mongoTemplate);
        setTargetType(SubmittedVariantEntity.class);
        setQuery(query(where(REFERENCE_SEQUENCE_FIELD).is(assemblyAccession).and(PROJECT_FIELD).in(projects)));
    }

    private static String getProjectField() {
        return getMongoField(SubmittedVariantEntity.class, "projectAccession");
    }
    private static String getReferenceSequenceField() {
        return getMongoField(SubmittedVariantEntity.class, "referenceSequenceAccession");
    }

    private static String getMongoField(Class<SubmittedVariantEntity> clazz, String classField) {
        java.lang.reflect.Field projectField = null;
        try {
            projectField = clazz.getDeclaredField(classField);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Does '" + clazz.getSimpleName() + "." + classField + "' exist?", e);
        }
        boolean annotationPresent = projectField.isAnnotationPresent(Field.class);
        if (!annotationPresent) {
            throw new IllegalStateException(
                    "Couldn't use reflection to get the field name. Does the '@Field' annotation exist for '"
                            + clazz.getSimpleName() + "." + classField + "'?");
        }
        Field annotation = projectField.getAnnotation(Field.class);

        String mappedField = annotation.value();
        return mappedField;
    }

}
