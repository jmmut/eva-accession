package uk.ac.ebi.eva.accession.remapping.batch.io;

import org.springframework.data.mongodb.core.mapping.Field;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

/**
 * This class provides the field used in the MongoDB documents, given its java class field counterpart.
 *
 * Even though a string of the class field has to be provided (instead of a string for the mongo field), this is an
 * improvement because a mistake (like a typo in the string) allows stopping the program at initialisation time,
 * instead of a query that doesn't behave as expected at runtime (returning 0 elements, or not performing
 * the correct filter).
 */
public class AccessionMongoFields {

    public static String getMongoField(Class<?> clazz, String classField) {
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
