/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.openmetadata.adapters.repositoryconnector;

import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.PropertyErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.MatchCriteria;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceProperties;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstancePropertyCategory;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstancePropertyValue;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.PrimitivePropertyValue;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;


public class DSLQueryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(DSLQueryHelper.class);

    public DSLQueryHelper() {

    }

    public String createWhereClause(String typeName, InstanceProperties matchProperties, MatchCriteria matchCriteria, List<String> limitResultsByClassification)
            throws
            PropertyErrorException,
            RepositoryErrorException
    {

        // Work out whether there are any criteria (match properties or classifications) that require
        // a WHERE clause. If so we need to begin and end the WHERE clause and populate it

        StringBuilder whereClauseBuilder = null;
        String whereClause = null;

        try {
            String propertyClause = createPropertyClause(matchProperties, matchCriteria);
            String classificationClause = createClassificationClause(limitResultsByClassification, typeName);

            if (propertyClause != null || classificationClause != null) {
                // There is at least some reason to include a WHERE clause
                whereClauseBuilder = new StringBuilder();
                whereClauseBuilder.append(" WHERE ( ");

                // If there is a propertyClause only - simply include it.
                // If there is a classificationClause only - simply include it
                // If there are both then surround each with parentheses and combine with AND operator
                String combinator = null;
                if (propertyClause != null && classificationClause != null) {
                    combinator = "AND";
                    StringBuilder nestedPropertyClauseBuilder = new StringBuilder();
                    nestedPropertyClauseBuilder.append("(");
                    nestedPropertyClauseBuilder.append(propertyClause);
                    nestedPropertyClauseBuilder.append(")");
                    propertyClause = nestedPropertyClauseBuilder.toString();
                    StringBuilder nestedClassificationClauseBuilder = new StringBuilder();
                    nestedClassificationClauseBuilder.append("(");
                    nestedClassificationClauseBuilder.append(classificationClause);
                    nestedClassificationClauseBuilder.append(")");
                    classificationClause = nestedClassificationClauseBuilder.toString();
                }

                if (propertyClause != null)
                    whereClauseBuilder.append(propertyClause);

                if (combinator != null)
                    whereClauseBuilder.append(" " + combinator + " ");

                if (classificationClause != null)
                    whereClauseBuilder.append(classificationClause);

                // Finalize...
                whereClauseBuilder.append(" ) ");
                // Harvest the resulting string
                whereClause = whereClauseBuilder.toString();
            }
            LOG.debug("createWhereClause: where clause {}", whereClause);
            return whereClause;
        }
        catch (PropertyErrorException e) {
            LOG.error("createWhereClause: re-throwing exception from createPropertyClause {}", e);
            throw e;
        }
    }

    private String createPropertyClause(InstanceProperties matchProperties, MatchCriteria matchCriteria)
            throws PropertyErrorException, RepositoryErrorException
    {

        // Where a property is of type String the match value should be used as a substring (fuzzy) match.
        // For all other types of property the match value needs to be an exact match.

        final String methodName = "createPropertyClause";

        StringBuilder propertyClauseBuilder;
        String propertyClause = null;

        if (matchProperties != null) {
            // Handle matchProperties (and matchCriteria)
            String condition;
            String operator;
            boolean negate = false;
            if (matchCriteria == null) {
                // Adopt default value for matchCriteria of ALL
                matchCriteria = MatchCriteria.ALL;
            }
            switch (matchCriteria) {
                case ALL:
                    condition = "AND";
                    operator = "=";
                    break;
                case ANY:
                    condition = "OR";
                    operator = "=";
                    break;
                case NONE:
                    // Currently does not support NONE - would need to negate substring match patterns
                    // This would be based on condition = "AND" and negate = true
                    // It may be possible to support this if a not() step were introduced to Atlas gremlin.
                    // Deliberately drop through to default case...
                default:
                    LOG.debug("createPropertyClause: only supports matchCriteria ALL, ANY");

                    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("matchCriteria", "matchCriteria", methodName, "LocalAtlasOMRSMetadataCollection");

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

            }

            // Add each property in the matchProperties to the search expression in the form:
            //   propName = propValue , combining entries with the op operator.
            Iterator<String> matchNames = matchProperties.getPropertyNames();

            if (matchNames.hasNext()) {

                propertyClauseBuilder = new StringBuilder();

                boolean first = true;

                while (matchNames.hasNext()) {
                    // Extract the name and value as strings for the query
                    String mName = matchNames.next();
                    boolean stringProp = false;
                    String strValue = null;
                    InstancePropertyValue matchValue = matchProperties.getPropertyValue(mName);
                    InstancePropertyCategory cat = matchValue.getInstancePropertyCategory();
                    switch (cat) {
                        case PRIMITIVE:
                            // matchValue is a PPV
                            PrimitivePropertyValue ppv = (PrimitivePropertyValue) matchValue;
                            // Find out if this is a string property
                            PrimitiveDefCategory pdc = ppv.getPrimitiveDefCategory();
                            if (pdc == PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING)
                                stringProp = true;
                            // We need a string for DSL query - this does not reflect the property type
                            strValue = ppv.getPrimitiveValue().toString();
                            break;
                        case ARRAY:
                        case MAP:
                        case ENUM:
                        case STRUCT:
                        case UNKNOWN:
                        default:
                            LOG.error("createPropertyClause: match property of cat {} not supported", cat);

                            OMRSErrorCode errorCode = OMRSErrorCode.BAD_PROPERTY_FOR_INSTANCE;

                            String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(mName, "matchProperties", methodName, "LocalAtlasOMRSMetadataCollection");

                            throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                    this.getClass().getName(),
                                    methodName,
                                    errorMessage,
                                    errorCode.getSystemAction(),
                                    errorCode.getUserAction());
                    }

                    // Insert each property and check if we need an op
                    if (first) {
                        first = false;
                    } else {
                        propertyClauseBuilder.append(" " + condition + " ");
                    }

                    // Override the operator (locally, only for this property) if it is a string
                    String localOperator = stringProp ? " LIKE " : operator;
                    // Surround the search string with single quotes and asterisks for regex
                    String updatedStrValue = stringProp ? "'*"+strValue+"*'" : strValue;
                    if (!negate)
                        propertyClauseBuilder.append(" " + mName + localOperator + updatedStrValue );
                    else // negating for NONE
                        propertyClauseBuilder.append(" " + mName + localOperator + updatedStrValue );
                }

                // Finalize...
                propertyClause = propertyClauseBuilder.toString();
            }
        }
        LOG.debug("createPropertyClause: property clause {}", propertyClause);
        return propertyClause;
    }

    private String createClassificationClause(List<String> limitResultsByClassification, String typeName)
    {
        StringBuilder classificationClauseBuilder = null;
        String classificationClause = null;

        if (limitResultsByClassification != null) {
            classificationClauseBuilder = new StringBuilder();
            boolean first = true;
            for (String classificationName : limitResultsByClassification ) {
                if (first) {
                    first = false;
                } else {
                    classificationClauseBuilder.append(" OR ");
                }
                classificationClauseBuilder.append( typeName + " ISA " + "'"+classificationName+"'" );
            }

            // Finalize...
            classificationClause = classificationClauseBuilder.toString();
        }

        LOG.debug("createClassificationClause: classification clause {}", classificationClause);
        return classificationClause;
    }
}
