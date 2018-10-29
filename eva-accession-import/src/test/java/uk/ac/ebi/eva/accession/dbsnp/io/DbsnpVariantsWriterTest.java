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
 */
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;
import static uk.ac.ebi.eva.accession.dbsnp.io.DbsnpClusteredVariantDeclusteredWriter.DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
public class DbsnpVariantsWriterTest {

    private static final int TAXONOMY_1 = 3880;

    private static final int TAXONOMY_2 = 3882;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final int START_1 = 100;

    private static final int START_2 = 200;

    private static final Long CLUSTERED_VARIANT_ACCESSION_1 = 12L;

    private static final Long CLUSTERED_VARIANT_ACCESSION_2 = 13L;

    private static final Long CLUSTERED_VARIANT_ACCESSION_3 = 14L;

    private static final VariantType VARIANT_TYPE = VariantType.SNV;

    private static final Long SUBMITTED_VARIANT_ACCESSION_1 = 15L;

    private static final Long SUBMITTED_VARIANT_ACCESSION_2 = 16L;

    private static final Long SUBMITTED_VARIANT_ACCESSION_3 = 17L;

    private static final String PROJECT_1 = "project_1";

    private static final String PROJECT_2 = "project_2";

    private DbsnpVariantsWriter dbsnpVariantsWriter;

    private Function<ISubmittedVariant, String> hashingFunctionSubmitted;

    private Function<IClusteredVariant, String> hashingFunctionClustered;

    @Autowired
    private MongoTemplate mongoTemplate;

    private ImportCounts importCounts;

    @Autowired
    private DbsnpSubmittedVariantOperationRepository operationRepository;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository;

    @Autowired
    private DbsnpClusteredVariantOperationRepository clusteredOperationRepository;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository;

    @Before
    public void setUp() throws Exception {
        importCounts = new ImportCounts();
        dbsnpVariantsWriter = new DbsnpVariantsWriter(mongoTemplate, operationRepository, submittedVariantRepository,
                                                      clusteredOperationRepository, clusteredVariantRepository,
                                                      importCounts);
        hashingFunctionSubmitted = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        hashingFunctionClustered = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantOperationEntity.class);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
    }

    @Test
    public void writeBasicVariant() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertClusteredVariantStored(1, wrapper);
        assertClusteredVariantDeclusteredStored(0);
    }

    private SubmittedVariant defaultSubmittedVariant() {
        return new SubmittedVariant("assembly", TAXONOMY_1, PROJECT_1, "contig", START_1,
                                    "reference", "alternate", CLUSTERED_VARIANT_ACCESSION_1,
                                    DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                    DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED, null);
    }

    private DbsnpVariantsWrapper buildSimpleWrapper(List<DbsnpSubmittedVariantEntity> submittedVariantEntities) {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setSubmittedVariants(submittedVariantEntities);
        wrapper.setClusteredVariant(clusteredVariantEntity);
        return wrapper;
    }

    private void assertSubmittedVariantsStored(int expectedVariants, DbsnpSubmittedVariantEntity... submittedVariants) {
        List<DbsnpSubmittedVariantEntity> ssEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(expectedVariants, ssEntities.size());
        assertEquals(expectedVariants, submittedVariants.length);
        assertEquals(expectedVariants, importCounts.getSubmittedVariantsWritten());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(ssEntities.contains(submittedVariants[i]));
        }
    }

    private void assertClusteredVariantStored(int expectedVariants, DbsnpVariantsWrapper... wrappers) {
        List<DbsnpClusteredVariantEntity> rsEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(expectedVariants, rsEntities.size());
        assertEquals(expectedVariants, wrappers.length);
        assertEquals(expectedVariants, importCounts.getClusteredVariantsWritten());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(rsEntities.contains(wrappers[i].getClusteredVariant()));
        }
    }

    @Test
    public void writeComplexVariant() throws Exception {
        SubmittedVariant submittedVariant1 = defaultSubmittedVariant();
        SubmittedVariant submittedVariant2 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                  "reference", "alternate_2",
                                                                  CLUSTERED_VARIANT_ACCESSION_1,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED, null);

        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                hashingFunctionSubmitted.apply(submittedVariant1),
                                                submittedVariant1, 1);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity2 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                hashingFunctionSubmitted.apply(submittedVariant2),
                                                submittedVariant2, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Arrays.asList(dbsnpSubmittedVariantEntity1,
                                                                        dbsnpSubmittedVariantEntity2));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertSubmittedVariantsStored(2, dbsnpSubmittedVariantEntity1, dbsnpSubmittedVariantEntity2);
        assertClusteredVariantStored(1, wrapper);
    }

    @Test
    public void declusterVariantWithMismatchingAlleles() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                 "reference", "alternate", EXPECTED_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                hashingFunctionSubmitted.apply(submittedVariant),
                                                submittedVariant, 1);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        dbsnpSubmittedVariantEntity1 = new SubmittedVariantDeclusterProcessor().decluster(dbsnpSubmittedVariantEntity1,
                                                                                          operations,
                                                                                          new ArrayList<>());

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(dbsnpSubmittedVariantEntity1));

        DbsnpSubmittedVariantOperationEntity operationEntity = operations.get(0);
        wrapper.setOperations(Collections.singletonList(operationEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        assertSubmittedVariantDeclusteredStored(wrapper);
        assertClusteredVariantStored(0);
        assertDeclusteringHistoryStored(wrapper.getClusteredVariant().getAccession(),
                                        wrapper.getSubmittedVariants().get(0));
        assertClusteredVariantDeclusteredStored(1, wrapper);
    }

    private DbsnpSubmittedVariantOperationEntity createOperation(SubmittedVariant submittedVariant1) {
        DbsnpSubmittedVariantOperationEntity operationEntity = new DbsnpSubmittedVariantOperationEntity();
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpSubmittedVariantInactiveEntity dbsnpSubmittedVariantInactiveEntity =
                new DbsnpSubmittedVariantInactiveEntity(dbsnpSubmittedVariantEntity);
        operationEntity.fill(EventType.UPDATED, SUBMITTED_VARIANT_ACCESSION_1, null, "Declustered",
                             Collections.singletonList(dbsnpSubmittedVariantInactiveEntity));
        return operationEntity;
    }

    private void assertSubmittedVariantDeclusteredStored(DbsnpVariantsWrapper wrapper) {
        List<DbsnpSubmittedVariantEntity> ssEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(1, ssEntities.size());
        assertEquals(1, wrapper.getSubmittedVariants().size());
        assertEquals(wrapper.getSubmittedVariants().get(0), ssEntities.get(0));
        assertNull(ssEntities.get(0).getClusteredVariantAccession());
        assertEquals(1, importCounts.getSubmittedVariantsWritten());
    }

    private void assertDeclusteringHistoryStored(Long clusteredVariantAccession,
                                                 DbsnpSubmittedVariantEntity... dbsnpSubmittedVariantEntities) {
        for (DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity : dbsnpSubmittedVariantEntities) {
            assertNull(dbsnpSubmittedVariantEntity.getClusteredVariantAccession());
        }

        List<DbsnpSubmittedVariantEntity> ssEntities =
                mongoTemplate.find(new Query(), DbsnpSubmittedVariantEntity.class);
        List<DbsnpSubmittedVariantOperationEntity> operationEntities =
                mongoTemplate.find(new Query(), DbsnpSubmittedVariantOperationEntity.class);

        assertNull(ssEntities.get(0).getClusteredVariantAccession());
        assertEquals(ssEntities.get(0).getAccession(), operationEntities.get(0).getAccession());

        assertEquals(1, operationEntities.size());
        assertEquals(EventType.UPDATED, operationEntities.get(0).getEventType());
        assertEquals(1, operationEntities.get(0).getInactiveObjects().size());
        assertEquals(clusteredVariantAccession,
                     operationEntities.get(0).getInactiveObjects().get(0).getClusteredVariantAccession());
        assertEquals(1, importCounts.getOperationsWritten());
    }

    private void assertClusteredVariantDeclusteredStored(int expectedVariants, DbsnpVariantsWrapper... wrappers) {
        List<DbsnpClusteredVariantEntity> rsDeclusteredEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertEquals(expectedVariants, rsDeclusteredEntities.size());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(rsDeclusteredEntities.contains(wrappers[i].getClusteredVariant()));
        }
    }

    @Test
    public void repeatedClusteredVariants() throws Exception {
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate",
                                                                  CLUSTERED_VARIANT_ACCESSION_1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(1, wrapper1);
    }

    @Test
    public void repeatedClusteredVariantsPartiallyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(1, wrapper1);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void repeatedClusteredVariantsCompletelyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));
        wrapper2.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(0);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void multiallelicClusteredVariantsPartiallyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Arrays.asList(submittedVariantEntity1,
                                                                        submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity = createOperation(submittedVariant1);
        wrapper.setOperations(Collections.singletonList(operationEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertClusteredVariantStored(1, wrapper);
        assertClusteredVariantDeclusteredStored(1, wrapper);
    }

    @Test
    public void multiallelicClusteredVariantsCompletelyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);

        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity1,
                                                                         submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper1));
        assertClusteredVariantStored(0);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void mergeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // Try to write wrapper twice, the second time it will be considered a duplicate and ignored
        // wrapper2 will be merged into the previous accession
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, submittedVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private void assertSubmittedVariantMergeOperationStored(int expectedOperations,
                                                            DbsnpSubmittedVariantEntity submittedVariantEntity) {
        List<DbsnpSubmittedVariantOperationEntity> operationEntities = mongoTemplate.find(
                new Query(), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedOperations, operationEntities.size());
        operationEntities
                .stream()
                .filter(op -> op.getMergedInto().equals(submittedVariantEntity.getAccession()))
                .forEach(operation -> {
                    assertEquals(EventType.MERGED, operation.getEventType());
                    List<DbsnpSubmittedVariantInactiveEntity> inactiveObjects = operation.getInactiveObjects();
                    assertEquals(1, inactiveObjects.size());
                    DbsnpSubmittedVariantInactiveEntity inactiveEntity = inactiveObjects.get(0);
                    assertNotEquals(submittedVariantEntity.getAccession(), inactiveEntity.getAccession());

                    assertEquals(submittedVariantEntity.getReferenceSequenceAccession(),
                                 inactiveEntity.getReferenceSequenceAccession());
                    assertEquals(submittedVariantEntity.getTaxonomyAccession(), inactiveEntity.getTaxonomyAccession());
                    assertEquals(submittedVariantEntity.getProjectAccession(), inactiveEntity.getProjectAccession());
                    assertEquals(submittedVariantEntity.getContig(), inactiveEntity.getContig());
                    assertEquals(submittedVariantEntity.getStart(), inactiveEntity.getStart());
                    assertEquals(submittedVariantEntity.getReferenceAllele(), inactiveEntity.getReferenceAllele());
                    assertEquals(submittedVariantEntity.getAlternateAllele(), inactiveEntity.getAlternateAllele());
                    assertEquals(submittedVariantEntity.getClusteredVariantAccession(),
                                 inactiveEntity.getClusteredVariantAccession());
                    assertEquals(submittedVariantEntity.isSupportedByEvidence(), inactiveEntity.isSupportedByEvidence());
                    assertEquals(submittedVariantEntity.isAssemblyMatch(), inactiveEntity.isAssemblyMatch());
                    assertEquals(submittedVariantEntity.isAllelesMatch(), inactiveEntity.isAllelesMatch());
                    assertEquals(submittedVariantEntity.isValidated(), inactiveEntity.isValidated());
                });
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // wrapper3 should not issue another identical merge event
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, submittedVariantEntity);
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2,
                                                                         submittedVariantEntity2));

        // should not issue another identical merge event for the second variant in wrapper2
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2));

        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_3, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(2, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_3, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2,
                                                                         submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(2, submittedVariantEntity);
    }


    @Test
    public void mergeDuplicateClusteredVariantsInTheSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1, VARIANT_TYPE,
                                                                 DEFAULT_VALIDATED, null);

        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    expectedSubmittedVariantEntity2);
        final Long[] longs = new Long[]{clusteredVariantEntity2.getAccession()};
        final int length = longs.length;
        assertSubmittedOperationsHaveClusteredVariantAccession(length, clusteredVariantEntity2.getAccession());
        assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private DbsnpSubmittedVariantEntity changeRS(DbsnpSubmittedVariantEntity submittedVariant, Long mergedInto) {
        // Need to create a new one because DbsnpSubmittedVariantEntity has no setters
        SubmittedVariant variant = new SubmittedVariant(submittedVariant);
        variant.setClusteredVariantAccession(mergedInto);

        Long accession = submittedVariant.getAccession();
        String hash = submittedVariant.getHashedMessage();
        int version = submittedVariant.getVersion();
        return new DbsnpSubmittedVariantEntity(accession, hash, variant, version);
    }

    private void assertSubmittedVariantsHaveActiveClusteredVariantsAccession(
            Long accession, DbsnpSubmittedVariantEntity... dbsnpSubmittedVariantEntities) {
        for (DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity : dbsnpSubmittedVariantEntities) {
            assertEquals(accession, dbsnpSubmittedVariantEntity.getClusteredVariantAccession());
        }
    }

    private void assertSubmittedOperationsHaveClusteredVariantAccession(int expectedCount,
                                                                        Long... expectedClusteredVariantAccessions) {
        List<DbsnpSubmittedVariantOperationEntity> submittedOperations = mongoTemplate.find(
                new Query(), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedCount, submittedOperations.size());
        Set<Long> oldClusteredAccessions = new HashSet<>();
        for (DbsnpSubmittedVariantOperationEntity submittedOperation : submittedOperations) {
            assertEquals(EventType.UPDATED, submittedOperation.getEventType());
            assertEquals(1, submittedOperation.getInactiveObjects().size());
            oldClusteredAccessions.add(submittedOperation.getInactiveObjects().get(0).getClusteredVariantAccession());
        }
        assertEquals(expectedClusteredVariantAccessions.length, oldClusteredAccessions.size());
        for (Long expectedClusteredVariantAccession : expectedClusteredVariantAccessions) {
            assertTrue(oldClusteredAccessions.contains(expectedClusteredVariantAccession));
        }
    }

    @Test
    public void mergeDuplicateClusteredVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1, VARIANT_TYPE,
                                                                 DEFAULT_VALIDATED, null);

        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    expectedSubmittedVariantEntity2);
        assertSubmittedOperationsHaveClusteredVariantAccession(1, clusteredVariantEntity2.getAccession());
        assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    @Test
    public void mergeThreeDuplicateClusteredVariantsInSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1, VARIANT_TYPE,
                                                                 DEFAULT_VALIDATED, null);

        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = defaultSubmittedVariant();
        submittedVariant3.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_3);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant3), submittedVariant3, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_3, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    wrapper2.getSubmittedVariants().get(0),
                                                                    wrapper3.getSubmittedVariants().get(0));
        assertSubmittedOperationsHaveClusteredVariantAccession(2, clusteredVariantEntity2.getAccession(),
                                                               clusteredVariantEntity3.getAccession());

        assertClusteredVariantMergeOperationStored(2, 2, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private void assertClusteredVariantMergeOperationStored(int expectedTotalOperations, int expectedMatchingOperations,
                                                            DbsnpClusteredVariantEntity mergedInto) {
        List<DbsnpClusteredVariantOperationEntity> operationEntities = mongoTemplate.find(
                new Query(), DbsnpClusteredVariantOperationEntity.class);
        assertEquals(expectedTotalOperations, operationEntities.size());

        long matchingOperations = operationEntities
                .stream()
                .filter(op -> op.getMergedInto().equals(mergedInto.getAccession()))
                .map(operation -> {
                    assertEquals(EventType.MERGED, operation.getEventType());
                    List<DbsnpClusteredVariantInactiveEntity> inactiveObjects = operation.getInactiveObjects();
                    assertEquals(1, inactiveObjects.size());
                    DbsnpClusteredVariantInactiveEntity inactiveEntity = inactiveObjects.get(0);
                    assertNotEquals(mergedInto.getAccession(), inactiveEntity.getAccession());

                    assertEquals(mergedInto.getAssemblyAccession(), inactiveEntity.getAssemblyAccession());
                    assertEquals(mergedInto.getContig(), inactiveEntity.getContig());
                    assertEquals(mergedInto.getStart(), inactiveEntity.getStart());
                    assertEquals(mergedInto.getTaxonomyAccession(), inactiveEntity.getTaxonomyAccession());
                    assertEquals(mergedInto.getType(), inactiveEntity.getType());
                    assertEquals(mergedInto.isValidated(), inactiveEntity.isValidated());
                    return 1;
                })
                .count();
        assertEquals(expectedMatchingOperations, matchingOperations);
    }

    /**
     * The test data is not real, but this exact thing happened with rs193927678.
     *
     * rs193927678 is mapped in 2 positions, so corresponds to 2 clustered variants with different hash and same RS
     * accession. This results in 2 submitted variants with different hash and same SS accession. Each entry of this
     * RS also makes hash collision with rs1095750933 and rs347458720.
     *
     * So to decide which active RS should we link in each SS, we have to take into account the hash as well. One SS
     * will be linked to rs1095750933 and the other to rs347458720.
     *
     * Note that there will be 2 clustered variant operations, as there are 2 hashes for rs193927678. Each operation
     * is a merge into rs1095750933 and rs347458720. Moreover, there are no declustered clustered variant operations.
     */
    @Test
    public void mergeRs193927678() throws Exception {
        Long clusteredVariantAccession1 = 347458720L;
        Long clusteredVariantAccession2 = 1095750933L;
        Long clusteredVariantAccession3 = 193927678L;
        Long submittedVariantAccession1 = 2688593462L;
        Long submittedVariantAccession2 = 2688600186L;
        Long submittedVariantAccession3 = 252447620L;

        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);

        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession2, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        ClusteredVariant clusteredVariant2 = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_2,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession2, hashingFunctionClustered.apply(clusteredVariant2), clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = defaultSubmittedVariant();
        submittedVariant3.setClusteredVariantAccession(clusteredVariantAccession3);
        submittedVariant3.setProjectAccession(PROJECT_2);
        SubmittedVariant submittedVariant4 = defaultSubmittedVariant();
        submittedVariant4.setStart(START_2);
        submittedVariant4.setProjectAccession(PROJECT_2);
        submittedVariant4.setClusteredVariantAccession(clusteredVariantAccession3);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession3, hashingFunctionSubmitted.apply(submittedVariant3), submittedVariant3, 1);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession3, hashingFunctionSubmitted.apply(submittedVariant4), submittedVariant4, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariantEntity),
                clusteredVariantEntity);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariantEntity2),
                clusteredVariantEntity2);
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3, wrapper4));

        assertClusteredVariantStored(2, wrapper, wrapper2);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3,
                                                                               clusteredVariantEntity.getAccession());
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity4 = changeRS(submittedVariantEntity4,
                                                                               clusteredVariantEntity2.getAccession());
        assertSubmittedVariantsStored(4, submittedVariantEntity, submittedVariantEntity2,
                                      expectedSubmittedVariantEntity3, expectedSubmittedVariantEntity4);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    expectedSubmittedVariantEntity3);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper2.getClusteredVariant().getAccession(),
                                                                    expectedSubmittedVariantEntity4);
        assertSubmittedOperationsHaveClusteredVariantAccession(2, clusteredVariantAccession3);
        assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity);
        assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity2);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    /**
     * The test data is not real, but this exact thing happened with rs638662487.
     *
     * rs638662487 should be declustered from ss1387800177 because one orientation is unknown. This is tracked writing
     * the RS in dbsnpClusteredVariantEntityDeclustered instead of dbsnpClusteredVariantEntity.
     *
     * However, another equivalent RS (rs268262202) was already declustered, and as this case is a collision of same
     * hash but different accession, it should be written in the clustered operations collection as a merge.
     */
    @Test
    public void declusterRs638662487() throws Exception {
        Long clusteredVariantAccession1 = 268262202L;
        Long clusteredVariantAccession2 = 638662487L;
        Long submittedVariantAccession1 = 528860089L;
        Long submittedVariantAccession2 = 1387800177L;

        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        submittedVariant.setClusteredVariantAccession(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);

        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);

        ArrayList<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariant =
                new SubmittedVariantDeclusterProcessor().decluster(submittedVariantEntity, operations,
                                                                   new ArrayList<>());

        wrapper.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariant));
        wrapper.setOperations(operations);


        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setProjectAccession(PROJECT_2);
        submittedVariant2.setClusteredVariantAccession(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession2, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        ClusteredVariant clusteredVariant2 = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession2, hashingFunctionClustered.apply(clusteredVariant2), clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);

        ArrayList<DbsnpSubmittedVariantOperationEntity> operations2 = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariant2 =
                new SubmittedVariantDeclusterProcessor().decluster(submittedVariantEntity2, operations2,
                                                                   new ArrayList<>());
        wrapper2.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariant2));
        wrapper2.setOperations(operations2);


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));

        assertSubmittedVariantsStored(2, declusteredSubmittedVariant, declusteredSubmittedVariant2);
        assertEquals(1, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
        assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
    }

    /**
     * Clustered variant rs136611820 was merged into several other clustered variants with the same hash: [42568024,
     * 42568025]
     *
     * This happened because rs136611820 had several locations. One matched the location of rs42568024 and other matched
     * the location of rs42568025. This makes it harder to decide which RS should be the active one and what to do with
     * the other RSs.
     *
     * The desired result is that an RS can be merged several times into other RSs if they all have the same hash, but
     * in the main collection only one of those will be present.
     *
     * The real case is more complicated because it involves also declusterings
     */
    @Test
    public void simplifiedRs136611820() throws Exception {
        // given
        Long clusteredVariantAccession1 = 42568024L;
        Long clusteredVariantAccession2 = 42568025L;
        Long clusteredVariantAccession3 = 136611820L;
        Long submittedVariantAccession1 = 64289612L;
        Long submittedVariantAccession2 = 64289614L;
        Long submittedVariantAccession3 = 266911375L;
        Long submittedVariantAccession4 = 266602754L;

        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);

        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        submittedVariant.setClusteredVariantAccession(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setClusteredVariantAccession(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession2, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = defaultSubmittedVariant();
        submittedVariant3.setProjectAccession(PROJECT_2);
        submittedVariant3.setClusteredVariantAccession(clusteredVariantAccession3);
        SubmittedVariant submittedVariant4 = defaultSubmittedVariant();
        submittedVariant4.setProjectAccession(PROJECT_2);
        submittedVariant4.setClusteredVariantAccession(clusteredVariantAccession3);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession3, hashingFunctionSubmitted.apply(submittedVariant3), submittedVariant3, 1);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession4, hashingFunctionSubmitted.apply(submittedVariant4), submittedVariant4, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3, wrapper4));

        // then
        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity3);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    expectedSubmittedVariantEntity2,
                                                                    expectedSubmittedVariantEntity3);
        assertSubmittedOperationsHaveClusteredVariantAccession(3, clusteredVariantAccession2,
                                                               clusteredVariantAccession3);
        assertClusteredVariantMergeOperationStored(4, 2, clusteredVariantEntity2);
        assertClusteredVariantMergeOperationStored(4, 2, clusteredVariantEntity3);
        // could be 3 operations total? 2 copies for clusteredVariantEntity3

        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    /**
     * Clustered variant rs136611820 was merged into several other clustered variants with the same hash: [42568024,
     * 42568025]
     *
     * This happened because rs136611820 had several locations. One matched the location of rs42568024 and other matched
     * the location of rs42568025. This makes it harder to decide which RS should be the active one and what to do with
     * the other RSs.
     *
     * The desired result is that an RS can be merged several times into other RSs if they all have the same hash, but
     * in the main collection only one of those will be present.
     *
     * This test is similar to the previous one, but closer to the real case, because this one involves also the
     * declusterings.
     */
    @Test
    public void rs136611820() throws Exception {
        // given
        Long clusteredVariantAccession1 = 42568024L;
        Long clusteredVariantAccession2 = 42568025L;
        Long clusteredVariantAccession3 = 136611820L;
        Long submittedVariantAccession1 = 64289612L;
        Long submittedVariantAccession2 = 64289614L;
        Long submittedVariantAccession3 = 266911375L;
        Long submittedVariantAccession4 = 266602754L;

        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);

        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        submittedVariant.setClusteredVariantAccession(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariant =
                new SubmittedVariantDeclusterProcessor().decluster(submittedVariantEntity, operations,
                                                                   new ArrayList<>());
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariant));
        wrapper.setOperations(operations);

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setClusteredVariantAccession(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession2, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = defaultSubmittedVariant();
        submittedVariant3.setProjectAccession(PROJECT_2);
        submittedVariant3.setClusteredVariantAccession(clusteredVariantAccession3);
        SubmittedVariant submittedVariant4 = defaultSubmittedVariant();
        submittedVariant4.setProjectAccession(PROJECT_2);
        submittedVariant4.setClusteredVariantAccession(clusteredVariantAccession3);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession3, hashingFunctionSubmitted.apply(submittedVariant3), submittedVariant3, 1);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations3 = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariantEntity3 =
                new SubmittedVariantDeclusterProcessor().decluster(submittedVariantEntity3, operations,
                                                                   new ArrayList<>());
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = new DbsnpSubmittedVariantEntity(
                submittedVariantAccession4, hashingFunctionSubmitted.apply(submittedVariant4), submittedVariant4, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariantEntity3));
        wrapper.setOperations(operations3);

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3, wrapper4));

        // then
        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity3);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    expectedSubmittedVariantEntity2,
                                                                    expectedSubmittedVariantEntity3);
        assertSubmittedOperationsHaveClusteredVariantAccession(4, clusteredVariantAccession2,
                                                               clusteredVariantAccession3, null);
        assertSubmittedOperationType(EventType.UPDATED, 2L);
        assertSubmittedOperationType(EventType.MERGED, 2L);

        assertClusteredVariantMergeOperationStored(3, 2, clusteredVariantEntity);
        assertClusteredVariantMergeOperationStored(3, 1, clusteredVariantEntity2);
        // or, instead of the 2 previous asserts, only assertClusteredVariantMergeOperationStored(2, 2, clusteredVariantEntity)

        List<DbsnpClusteredVariantEntity> declustered = mongoTemplate.findAll(
                DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertEquals(1, declustered.size());
        assertEquals(clusteredVariantAccession2, declustered.get(0).getAccession());
    }

    private void assertSubmittedOperationType(EventType operationType, long expectedCount) {
        List<DbsnpSubmittedVariantOperationEntity> submittedOperations =
                mongoTemplate.find(query(where("eventType").is(operationType.toString())),
                                   DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedCount, submittedOperations.size());
    }
}
