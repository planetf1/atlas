
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.openmetadata.adapters.repositoryconnector;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.RelationshipDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.RelationshipEndDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;

import org.odpi.openmetadata.repositoryservices.ffdc.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefCategory.RELATIONSHIP_DEF;


public class AtlasRelationshipMapper {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipMapper.class);

    /*
     *  Mapper with method for output as OM Relationship
     *  Always construct from an AtlasRelationship
     *
     *  To use this class, construct a new AtlasRelationshipMapper and then invoke the
     *  method corresponding to output OM Relationship
     */

    /* AtlasRelationship has:
     *
     * String              typeName
     * Map<String, Object> attributes
     * String              guid
     * AtlasObjectId       end1
     * AtlasObjectId       end2
     * String              label
     * PropagateTags       propagateTags
     * Status              status
     * String              createdBy
     * String              updatedBy
     * Date                createTime
     * Date                updateTime
     * Long                version
     *
     * OM Relationship needs:
     *
     *
     * InstanceAuditHeader fields
     *
     * InstanceType              type
     * String                    createdBy
     * String                    updatedBy
     * Date                      createTime
     * Date                      updateTime
     * Long                      version
     * InstanceStatus            currentStatus
     * InstanceStatus            statusOnDelete
     *
     * Instance Header fields
     *
     * InstanceProvenanceType    instanceProvenanceType
     * String                    metadataCollectionId
     * String                    guid
     * String                    instanceURL
     *
     * Relationship fields
     *
     *   InstanceProperties    relationshipProperties
     *   String                entityOnePropertyName   -- Retrieve this from the RelDef.RelEndDef for end1
     *   EntityProxy           entityOneProxy
     *   String                entityTwoPropertyName   --  Retrieve this from the RelDef.RelEndDef for end2
     *   EntityProxy           entityTwoProxy
     */

    private LocalAtlasOMRSMetadataCollection metadataCollection   = null;
    private String                           metadataCollectionId = null;
    private String                           userId               = null;
    private AtlasEntityStore                 entityStore          = null;
    private AtlasRelationship                atlasRelationship    = null;
    private RelationshipDef                  relationshipDef      = null;
    private AtlasEntity                      atlasEntity1         = null;
    private AtlasEntity                      atlasEntity2         = null;


    public AtlasRelationshipMapper(LocalAtlasOMRSMetadataCollection metadataCollection,
                                   String                           userId,
                                   AtlasRelationship                atlasRelationship,
                                   AtlasEntityStore                 entityStore)

            throws
            TypeErrorException,
            RepositoryErrorException,
            InvalidParameterException,
            InvalidRelationshipException,
            EntityNotKnownException

    {

        final String methodName = "AtlasRelationshipMapper";

        LOG.debug("AtlasRelationshipMapper: userId={}, atlasRelationship={} ", userId, atlasRelationship);

        if (metadataCollection == null) {
            // We are not going to get far...
            LOG.error("AtlasRelationshipMapper: metadataCollection is null ");

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.NULL_PARAMETER;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("metadataCollection", methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        if (atlasRelationship == null) {
            // We are not going to get far...
            LOG.error("AtlasRelationshipMapper: atlasRelationship is null ");

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.NULL_PARAMETER;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasRelationship", methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        this.metadataCollection = metadataCollection;
        try {
            this.metadataCollectionId = metadataCollection.getMetadataCollectionId();
        }
        catch (RepositoryErrorException e) {
            LOG.error("AtlasRelationshipMapper: caught repository exception - could not get metadataCollectionId", e);
            // re-throw error
            throw e;
        }
        this.entityStore = entityStore;
        this.atlasRelationship = atlasRelationship;
        this.userId = userId;
        // Get the relationship def
        try {
            this.relationshipDef = getRelationshipDef(atlasRelationship);
        }
        catch (TypeErrorException e) {
            LOG.error("AtlasRelationshipMapper: caught type exception - could not retrieve relationship def for typename {}", atlasRelationship.getTypeName(), e);
            // Re-throw - exception will be caught in the MDC
            throw e;
        }
        // Get the two entities
        try {
            this.atlasEntity1 = getAtlasEntity(atlasRelationship,1);
            this.atlasEntity2 = getAtlasEntity(atlasRelationship,2);
        }
        catch (EntityNotKnownException | InvalidParameterException | InvalidRelationshipException e) {
            LOG.error("AtlasRelationshipMapper: caught type exception - could not retrieve entity from relationship", e);
            // Re-throw - exception will be caught in the MDC
            throw e;
        }
    }


    /**
     * Method to convert the mapper's AtlasRelationship to an OM Relationship
     * @return The retrieved and converted OM Relationship
     * @throws TypeErrorException       - there is something wrong with the typedefs
     * @throws RepositoryErrorException - the repository threw an exception
     * @throws InvalidEntityException   - one of the relationship's entities was invalid
     */
    public Relationship toOMRelationship() throws TypeErrorException, RepositoryErrorException, InvalidEntityException {
        if (this.atlasRelationship == null) {
            return null;
        }
        Relationship omRelationship = new Relationship();
        try {
            completeRelationship(omRelationship);
            return omRelationship;
        }
        catch (TypeErrorException e) {
            LOG.error("toOMRelationship: caught TypeErrorException {}", e);
            throw e;
        }
        catch (RepositoryErrorException e) {
            LOG.error("toOMRelationship: caught RepositoryErrorException {}", e);
            throw e;
        }
        catch (InvalidEntityException e) {
            LOG.error("toOMRelationship: caught InvalidEntityException {}", e);
            throw e;
        }
    }

    /*
     * Private method to retrieve RelationshipDef corresponding to specified relationship.
     */
    private RelationshipDef getRelationshipDef(AtlasRelationship atlasRelationship) throws TypeErrorException {

        final String methodName = "getRelationshipDef";

        // Get the RelationshipDef using the typeName
        String relationshipTypeName = atlasRelationship.getTypeName();
        LOG.debug("getRelationshipDef: atlas relationship has typename {}", relationshipTypeName);
        TypeDef typeDef;
        try {

            typeDef = metadataCollection._getTypeDefByName(userId, relationshipTypeName);

        } catch (TypeDefNotKnownException | RepositoryErrorException e) {

            LOG.error("getRelationshipDef: Caught exception from getTypeDefByName {}", e);
            // handle the exception below
            typeDef = null;
        }

        // Validate it
        if (typeDef == null || typeDef.getCategory() != RELATIONSHIP_DEF) {

            LOG.error("getRelationshipDef: Could not find relationship def with name {} ", relationshipTypeName);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeName, "relationshipTypeName", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        /* Have done all we can to ensure this is a RelationshipDef, but protect against a
         * possible class cast exception anyway...
         */
        RelationshipDef relationshipDef;
        try {
            relationshipDef = (RelationshipDef) typeDef;
        }
        catch (ClassCastException e) {
            LOG.error("getRelationshipDef: TypeDef with  name {} is not a RelationshipDef", relationshipTypeName, e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeName, "unknown", "relationshipTypeName", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        LOG.debug("getRelationshipDef: located relationshipDef {}", relationshipDef);

        // Validate RelationshipDef
        // Make sure it has two end defs - each with an attributeName
        RelationshipEndDef relEndDef1 = relationshipDef.getEndDef1();
        RelationshipEndDef relEndDef2 = relationshipDef.getEndDef2();
        if (relEndDef1 == null || relEndDef1.getAttributeName() == null ||
            relEndDef2 == null || relEndDef2.getAttributeName() == null) {

            LOG.error("getRelationshipDef: RelationshipDef {} does not have valid ends", relationshipTypeName);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeName, "unknown", "relationshipTypeName", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        return relationshipDef;
    }


    /*
     * Private method to retrieve entity from specified end of relationship.
     */
    private AtlasEntity getAtlasEntity(AtlasRelationship atlasRelationship,
                                       int               end)
            throws
            InvalidParameterException,
            InvalidRelationshipException,
            EntityNotKnownException
    {
        // Retrieve the AtlasEntity objects from the repository and create an EntityProxy object for each...
        // The AtlasRelationship has AtlasObjectID for end1 and end2.
        final String methodName = "getAtlasEntity";

        String entityGuid;

        AtlasObjectId atlasEnd;
        switch (end) {
            case 1:
                atlasEnd = atlasRelationship.getEnd1();
                break;
            case 2:
                atlasEnd = atlasRelationship.getEnd2();
                break;
            default:
                LOG.error("getAtlasEntity: invalid end identifier {}", end);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_PARAMETER;

                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("" + end, "end", methodName);

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }
        if (atlasEnd != null) {
            entityGuid = atlasEnd.getGuid();
        }
        else {
            LOG.error("getAtlasEntity: relationship end {} is null", end);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(atlasRelationship.getGuid(), metadataCollectionId, methodName);

            throw new InvalidRelationshipException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;
        try {

            atlasEntWithExt = this.entityStore.getById(entityGuid);

        } catch (AtlasBaseException e) {

            LOG.error("getAtlasEntity: Caught exception from Atlas entityStore get by guid {}, {}", entityGuid, e);
            // handle below
            atlasEntWithExt = null;

        }
        if (atlasEntWithExt == null) {

            LOG.error("getAtlasEntity: Could not find entity with guid {} ", entityGuid);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGuid, "entityGuid", methodName, metadataCollectionId);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
        // Extract the entity
       return atlasEntWithExt.getEntity();

    }

    /**
     * completeRelationship
     * @param omRelationship - Relationship object to be completed
     * @throws TypeErrorException       - a type error occurred
     * @throws RepositoryErrorException - a repository error occurred
     */
    private void completeRelationship(Relationship omRelationship) throws TypeErrorException,RepositoryErrorException, InvalidEntityException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> completeRelationship(omRelationship={})", omRelationship);
        }
        // Create instance type
        InstanceType instanceType = createInstanceType(relationshipDef);
        omRelationship.setType(instanceType);

        // Set fields from InstanceAuditHeader
        omRelationship.setType(instanceType);
        omRelationship.setCreatedBy(atlasRelationship.getCreatedBy());
        omRelationship.setCreateTime(atlasRelationship.getCreateTime());
        omRelationship.setUpdatedBy(atlasRelationship.getUpdatedBy());
        omRelationship.setUpdateTime(atlasRelationship.getUpdateTime());
        omRelationship.setVersion(atlasRelationship.getVersion());

        if (atlasRelationship.getStatus() == AtlasRelationship.Status.ACTIVE) {
            omRelationship.setStatus(InstanceStatus.ACTIVE);
        } else {
            omRelationship.setStatus(InstanceStatus.DELETED);
        }

        // Set fields from InstanceHeader
        omRelationship.setMetadataCollectionId(atlasRelationship.getHomeId());
        omRelationship.setGUID(atlasRelationship.getGuid());
        omRelationship.setInstanceURL(null);

        // Set fields from Relationship
        // Take the attributes from AtlasRelationship and set the Properties for OM Relationship
        Map<String, Object> atlasAttrs = atlasRelationship.getAttributes();
        AtlasAttributeMapper atlasAttributeMapper = new AtlasAttributeMapper(metadataCollection, userId);
        InstanceProperties omRelProps = atlasAttributeMapper.convertAtlasAttributesToOMProperties(relationshipDef, atlasAttrs, false);
        omRelationship.setProperties(omRelProps);
        LOG.debug("completeRelationship: completed properties {}", omRelationship);

        // Add the ends to the Relationship
        RelationshipEndDef relEndDef1 = relationshipDef.getEndDef1();
        String endDef1AttributeName = relEndDef1.getAttributeName();
        omRelationship.setEntityOnePropertyName(endDef1AttributeName);

        RelationshipEndDef relEndDef2 = relationshipDef.getEndDef2();
        String endDef2AttributeName = relEndDef2.getAttributeName();
        omRelationship.setEntityTwoPropertyName(endDef2AttributeName);


        // Convert each AtlasEntity into an OM EntityProxy
        try {
            AtlasEntityMapper atlasEntityMapper1 = new AtlasEntityMapper(metadataCollection, userId, atlasEntity1);
            EntityProxy end1Proxy = atlasEntityMapper1.toEntityProxy();
            LOG.debug("completeRelationship: om entity1 {}", end1Proxy);
            omRelationship.setEntityOneProxy(end1Proxy);

            AtlasEntityMapper atlasEntityMapper2 = new AtlasEntityMapper(metadataCollection, userId, atlasEntity2);
            EntityProxy end2Proxy = atlasEntityMapper2.toEntityProxy();
            LOG.debug("completeRelationship: om entity2 {}", end2Proxy);
            omRelationship.setEntityTwoProxy(end2Proxy);
        }
        catch (TypeErrorException e) {
            LOG.error("completeRelationship: caught TypeErrorException from entity mapper {}", e);
            throw e;
        }
        catch (RepositoryErrorException e) {
            LOG.error("completeRelationship: caught RepositoryErrorException from entity mapper {}", e);
            throw e;
        }
        catch (InvalidEntityException e) {
            LOG.error("completeRelationship: caught InvalidEntityException from entity mapper {}", e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== completeRelationship(): return={}", omRelationship);
        }

    }

    /**
     * Helper method to create InstanceType from an RelationshipDef
     * @param relationshipDef - the RelationshipDef for which an InstanceType is needed
     * @return - the created InstanceType
     */
    private InstanceType createInstanceType(RelationshipDef relationshipDef) {
        // Create an instance type - this uses a combination of things from the relationship type and the atlas relationship

        // Collate the valid instance properties - no supertypes to traverse for a relationship def
        ArrayList<String> validInstanceProperties = null;
        List<TypeDefAttribute> typeDefAttributes = relationshipDef.getPropertiesDefinition();
        if (typeDefAttributes != null) {
            validInstanceProperties = new ArrayList<>();
            for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                String attrName = typeDefAttribute.getAttributeName();
                validInstanceProperties.add(attrName);
            }
        }

        // An OM RelationshipDef has no superType - so we just set that to null
        InstanceType instanceType = new InstanceType(
                relationshipDef.getCategory(),
                relationshipDef.getGUID(),
                relationshipDef.getName(),
                relationshipDef.getVersion(),
                relationshipDef.getDescription(),
                relationshipDef.getDescriptionGUID(),
                null,
                relationshipDef.getValidInstanceStatusList(),
                validInstanceProperties);

        return instanceType;
    }
}