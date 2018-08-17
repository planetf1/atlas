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


import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.typedef.*;
import org.apache.atlas.openmetadata.adapters.eventmapper.AtlasOMRSRepositoryEventMapper;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStreamForImport;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.util.AtlasRepositoryConfiguration;

import static org.apache.atlas.model.discovery.SearchParameters.Operator.EQ;
import static org.apache.atlas.model.instance.AtlasRelationship.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasRelationship.Status.DELETED;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality.*;

import org.eclipse.jetty.util.StringUtil;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSMetadataCollection;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.MatchCriteria;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.SequencingOrder;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.AttributeTypeDefCategory.ENUM_DEF;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefCategory.*;

import org.odpi.openmetadata.repositoryservices.ffdc.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 *  The LocalAtlasOMRSMetadataCollection represents a local metadata repository.
 *  Requests to this metadata collection are mapped to requests to the local repository.
 *
 *  The metadata collection reads typedefs from Atlas and will attempt to convert them to OM typedefs - and
 *  vice versa. During these conversions it will check the validity of the content of the type defs as far as
 *  possible, giving up on a typedef that it cannot verify or convert.
 *
 *  This implementation of the metadata collection does not use the java implementations of the REST APIs because
 *  the REST interface is servlet and http oriented. This implementation uses lower level classes in Atlas.
 *  The behaviour still respects transactions because they are implemented at levels below the REST implementation.
 *  The transactions are implemented by annotation in the underlying implementations of the AtlasTypeDefStore
 *  interface.
 */


public class LocalAtlasOMRSMetadataCollection extends OMRSMetadataCollection {

    private static final Logger LOG = LoggerFactory.getLogger(LocalAtlasOMRSMetadataCollection.class);

    /*
     * The MetadataCollection makes use of a pair of TypeDefsByCategory objects.
     *
     * A TypeDefsByCategory is a more fully explicated rendering of the TypeDefGallery - it makes
     * it easier to store and search for a type def of a particular category. The TDBC objects are only
     * used internally - they do not form part of the API; being rendered as a TypeDefGallery just before
     * return from getTypeDefs. The MDC uses two TDBC objects - one is transient and the other is longer lived.
     *
     * The typeDefsCache is a long-lived TypeDefsByCategory used to remember the TypeDefs from Atlas (that can be
     * modeled in OM). It is allocated at the start of loadTypeDefs so can be refreshed by a fresh call to loadTypeDefs.
     * The typeDefsCache is therefore effectively a cache of type defs. To refresh it call loadTypeDefs again - this
     * will clear it and reload it.
     * The typeDefsCache is retained across API calls; unlike typeDefsForAPI which is not.
     */


    /*
     * typeDefsForAPI is a transient TDBC object - it is reallocated at the start of each external API call.
     * It's purpose is to marshall the results for the current API (only), which can then be extracted or
     * turned into a TDG, depending on return type of the API.
     */
    private TypeDefsByCategory typeDefsForAPI = null;

    /*
     * Declare the Atlas stores the connector will use - these are injected via AtlasStoresProxy.
     */
    private AtlasTypeRegistry typeRegistry = null;
    private AtlasTypeDefStore typeDefStore = null;
    private AtlasEntityStore entityStore = null;
    private AtlasRelationshipStore relationshipStore = null;
    private EntityDiscoveryService entityDiscoveryService = null;

    enum AtlasDeleteOption { SOFT , HARD }

    private AtlasDeleteOption atlasDeleteConfiguration;

    private boolean useRegistry = true;

    // EventMapper will be set by the event mapper itself calling the metadataCollection once the mapper is started.
    private AtlasOMRSRepositoryEventMapper eventMapper = null;



    // package private
    LocalAtlasOMRSMetadataCollection(LocalAtlasOMRSRepositoryConnector parentConnector,
                                     String                            repositoryName,
                                     OMRSRepositoryHelper              repositoryHelper,
                                     OMRSRepositoryValidator           repositoryValidator,
                                     String                            metadataCollectionId)
    {

        /*
         * The metadata collection Id is the unique Id for the metadata collection.  It is managed by the super class.
         */
        super(parentConnector, repositoryName, metadataCollectionId, repositoryHelper, repositoryValidator);

        /*
         *  Initialize the Atlas stores
         */
        this.typeRegistry = SpringBridge.services().getTypeRegistry();
        this.typeDefStore = SpringBridge.services().getTypeDefStore();
        this.entityStore = SpringBridge.services().getEntityStore();
        this.relationshipStore = SpringBridge.services().getRelationshipStore();
        this.entityDiscoveryService = SpringBridge.services().getEntityDiscoveryService();

        /*
         * Read the Atlas Configuration to initialize the recorded delete configuration of the repository.
         * The following approach ensures that you get a class - because the getDeleteHandlerV1Impl method
         * adopts the default class if no property has been set. This is preferable to looking directly
         * for the property and (if not set) deciding here what default to adopt - this should come from Atlas.
         */

        LOG.debug("LocalAtlasOMRSMetadataCollection: Find which Atlas deleteHandler is configured");
        Class<? extends DeleteHandlerV1>  deleteHandlerClass = AtlasRepositoryConfiguration.getDeleteHandlerV1Impl();
        if (deleteHandlerClass != null) {
            LOG.debug("LocalAtlasOMRSMetadataCollection: delete handler is {}", deleteHandlerClass.getName());
            atlasDeleteConfiguration =
                    deleteHandlerClass.getName().equals("SoftDeleteHandlerV1") ? AtlasDeleteOption.SOFT : AtlasDeleteOption.HARD;
        }
        else {
            // Cannot access configuration so will not know how to handle deletes. Give it up.
            LOG.error("LocalAtlasOMRSMetadataCollection: delete handler configuration not found!");

            String actionDescription = "LocalAtlasOMRSMetadataCollection Constructor";

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ATLAS_CONFIGURATION;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(repositoryName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    actionDescription,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
    }


    /*
     * Helper method for event mapper
     */
    public boolean isAtlasConfiguredForHardDelete() {

        if (atlasDeleteConfiguration == AtlasDeleteOption.HARD) {
            LOG.debug("isAtlasConfiguredForHardDelete: Repository is configured for hard deletes");
            return true;
        }
        return false;
    }



    /* ======================================================================
     * Group 1: Confirm the identity of the metadata repository being called.
     */

    /**
     * Returns the identifier of the metadata repository.  This is the identifier used to register the
     * metadata repository with the metadata repository cohort.  It is also the identifier used to
     * identify the home repository of a metadata instance.
     *
     * @return String - metadata collection id.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     */
    public String      getMetadataCollectionId()
        throws
            RepositoryErrorException
    {

        final String methodName = "getMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        /*
         * Perform operation
         */
        return super.metadataCollectionId;
    }



    /* ==============================
     * Group 2: Working with typedefs
     */

    /**
     * Returns the list of different types of TypeDefs organized by TypeDef Category.
     *
     * @param userId - unique identifier for requesting user.
     * @return TypeDefs - List of different categories of TypeDefs.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery getAllTypes(String userId)
            throws
            RepositoryErrorException,
            UserNotAuthorizedException
    {



        final String                       methodName = "getAllTypes";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> {} (userId={})", methodName, userId);
        }
        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);

        /*
         * Perform operation
         */

        /*
         * The MDC will request the TypeDefs from Atlas. It then parses each of the lists in the TypesDef and
         * behaves as follows:
         * For entities, relationships and classifications it will parse each TypeDef and will produce a
         * corresponding OM type def - which will be of concrete type (e.g. EntityDef). It can only do this for Atlas defs
         * that can be modelled in OM - some things are not possible, in which case no OM def is produced for that Atlas def.
         * It will validate that the generated OM type is valid - i.e. that all mandatory fields are present, that no constraints are
         * violated, etc - Note that this validation step is not very explicit yet - it will be refined/extended later.
         * The MDC must then check whether the generated OM type is known in the RCM. It uses the RepoHelper and Validator
         * classes for this - it can new up a RepoHelper and this will have a static link to the RCM. It should then issue
         * a get by name of the typedef against the RCM. I think this might use isKnownType().
         * If the type is found within the RCM, then we know that it already exists and we must then perform a deep
         * compare of the known type and the newly generated type. If they are the same then we can safely use the
         * known type; we adopt it's GUID and add the typedef to the TDG. If they do not match exactly then we issue
         * an audit log entry and ignore the generated type (it will not be added to the TDG). If the type is NOT found in the
         * RCM then we are dealing with a newly discovered type and we can generate a GUID (using the usual means). The generated
         * typedef is added to the TDG.
         * In addition to looking in the RCM we need to look at any types we already added to the TDG (in this pass) because each
         * type only appears once in the TDG. This can be a compare by name only and does not need a compare of content (which is needed
         * vs a type found in the RCM by name) because the type namespace in Atlas means that two references by the same name must by
         * definition be references to the same type.
         * The above processing is similar for EntityDef, RelationshipDef and ClassificationDef typedefs. The MDC is either referring to
         * known types or generating newly discovered types for those types not found in the RCM.
         *
         * The above types can (and probably will) contain attribute defs. For each attribute the MDC needs to decide what type
         * the attribute has. It will either be an Enum, a Prim or a Coll of Prim, or it may be a type reference or coll of type reference.
         * In the last two cases (involving type refs) the enclosing typedef will be skipped (not added to the TypeDefGallery). This is
         * because type references are specific to older Atlas types and in OM the types should use first-class relationships.
         * The older Atlas types are therefore not pulled into OM.
         *
         *  Whenever an enclosing typedef (EntityDef, RelationshipDef or ClassificationDef) is added to the TypeDefGallery it will have a
         * propertiesDef which is a list of TDA, each having the attribute name and an AttributeTypeDef which is on the TypeDefGallery's
         * attributeTypeDefs list.
         * This is similar to how one TypDef refers to another TypeDef (as opposed to a property/attribute): when a TypeDef refers to another
         * TypeDef a similar linkage is established through a TypeDefLink that refers to a TypeDef on the TypeDefGallery's typeDefs list.
         * For attribute types that are either consistent and known or valid and new we add them to the TypeDefGallery's list of attributeTypeDefs.
         * Where multiple 'top-level' typedefs contain attributes of the same type - e.g. EntityDef1 has an int called 'retention' and
         * EntityDef2 has an int called 'count' - then they will both refer to the same single entry for the int primitive attr type in the
         * TypeDefGallery. References to types in the TypeDefGallery are by TypeDefLink (which needs a cat, name and guid).
         *
         * So the attribute types to be processed are:
         * Prim - for each of these the MDC will match the Atlas typename (a string) against the known PrimDefCats and construct a PrimDef with
         * the appropriate PrimDefCat. Look in the RCM and if a Prim with the same string typename exists then adopt its GUID.
         * [ There is a shortcut here I think as the RCM will have used the same GUID as the PrimDefCat so you can find the
         * GUID directly. ] I think it is very unlikely that there will be new Primitives discovered. In fact I think that is impossible.
         * Collection - a Collection can be either an ARRAY<Primitive> or a MAP<Primitive,Primitive>. In both cases we can create a consistent
         * name for the collection type by using the pattern found in CollectionDef[Cat]. The MDC uses that collection type name to
         * check whether the collection type is already known in the RCM (again this is a get by name, since we do not have a GUID).
         * If the collection is a known type then we will use the GUID from the RCM and add it to the √. It is likely that the RCM
         * will already know about a small number of collections - eg. Map<String,String> - so for these the MDC will reuse the
         * existing attributeDef - it will still add the coll def to the √ and will refer to it using a TDL with the cat, name and guid.
         * Any further coll defs will be generated and given a new GUID.
         * Enum - An Enum Def is an easier beast to convert than a Prim or a Coll because in Atlas it is a real type and has a guid. Processing for an
         * enum will be more like that for EntityDef, etc. and will require validation (of possible values, default value, etc) and an existence
         * check in the RCM. If known we will do a deep compare and on match will use the existing def, on mismatch generate audit log entry and
         * skip not just the enum but also any type that contains it. If it does not exist in the RCM we will generate a new EnumDef with a new GUID.
         *
         * Unit Testing
         * ------------
         * 3 phases of elaboration.
         * 1. Just assume (assert?) that each type does not exist in the RCM and hence we will project it into the generated √. This is a good way
         * to see and verify the generation of types by the MDC. The generator and comparator should be able to generate/compare √.
         * 2. Mock up the RCM so that we can control what it 'already knows'. This will allow us to test the existence match behaviour of MDC and
         * the apparent-match-but-different-content logic and audit log entry generation.
         * 3. Like UT1 (assume that everything is new) but have all the OM types loaded into Atlas. This will allow us to test whether the integration
         * with Atlas is working and whether the types are coming through OK. This is actually an integration test I think.
         *
         */


        // Clear the per-API records of TypeDefs
        typeDefsForAPI = new TypeDefsByCategory();

        /*
         * This method is a thin wrapper around loadAtlasTypeDefs which will clear and reload the cache
         * and will construct it's result in typeDefsForAPI and save it into the typeDefsCache.
         * The loadAtlasTypeDefs method can be used at other times when we need to cache the known TypeDefs
         * - e.g. for verify calls.
         */


        try {
            loadAtlasTypeDefs(userId);
        } catch (RepositoryErrorException e) {
            LOG.error("getAllTypes: caught exception from Atlas {}", e);
            throw e;
        }

        /*
         * typeDefsForAPI will have been loaded with all discovered TypeDefs. It can be converted into a TypeDefGallery
         * and can be reset on the next API to be called.
         */


        // Convert the typeDefsForAPI to a gallery
        TypeDefGallery typeDefGallery = typeDefsForAPI.convertTypeDefsToGallery();
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== {} typeDefGallery={}", methodName, typeDefGallery);
        }
        return typeDefGallery;
    }

    /**
     * Returns a list of type definitions that have the specified name.  Type names should be unique.  This
     * method allows wildcard character to be included in the name.  These are * (asterisk) for an
     * arbitrary string of characters and ampersand for an arbitrary character.
     *
     * @param userId - unique identifier for requesting user.
     * @param name - name of the TypeDefs to return (including wildcard characters).
     * @return TypeDefGallery - List of different categories of type definitions.
     * @throws InvalidParameterException - the name of the TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDefGallery findTypesByName(String  userId,
                                          String  name)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        /*
         * Retrieve the typedefs from Atlas, and return a TypeDefGallery that contains two lists, each sorted by category
         * as follows:
         * TypeDefGallery.attributeTypeDefs contains:
         * 1. PrimitiveDefs
         * 2. CollectionDefs
         * 3. EnumDefs
         * TypeDefGallery.newTypeDefs contains:
         * 1. EntityDefs
         * 2. RelationshipDefs
         * 3. ClassificationDefs
         */

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findTypesByName(userId={}, name={})", userId, name);
        }

        final String   methodName        = "findTypesByName";
        final String   sourceName        = metadataCollectionId;
        final String   nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        /*
         * Perform operation
         */

        // Clear the per-API records of TypeDefs
        typeDefsForAPI = new TypeDefsByCategory();

        // Strategy: use searchTypesDef with a SearchFilter with name parameter
        SearchFilter searchFilter = new SearchFilter();
        searchFilter.setParam(SearchFilter.PARAM_NAME, name);
        AtlasTypesDef atd;
        try {
            atd = typeDefStore.searchTypesDef(searchFilter);

        } catch (AtlasBaseException e) {

            LOG.error("findTypesByName: caught exception from Atlas type def store {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(name, nameParameterName, methodName, sourceName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());


        }

        /*
         * Parse the Atlas TypesDef
         * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
         * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
         * is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
         * supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
         * things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
         * anything that contains something that it does not understand (e.g. a reference attribute or a collection that
         * contains anything other than primitives).
         */

        // This method will populate the typeDefsForAPI object.
        if (atd != null) {
            convertAtlasTypeDefs(userId, atd);
        }

        // Convert the typeDefsForAPI to a gallery
        TypeDefGallery typeDefGallery = typeDefsForAPI.convertTypeDefsToGallery();
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findTypesByName(userId={}, name={}): typeDefGallery={}", userId, name, typeDefGallery);
        }
        return typeDefGallery;
    }


    /**
     * Returns all of the TypeDefs for a specific category.
     *
     * @param userId - unique identifier for requesting user.
     * @param category - enum value for the category of TypeDef to return.
     * @return TypeDefs list.
     * @throws InvalidParameterException - the TypeDefCategory is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> findTypeDefsByCategory(String          userId,
                                                TypeDefCategory category)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        // Use Atlas typedefstore search API and then post-filter by type category
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findTypeDefsByCategory(userId={}, category={})", userId, category);
        }

        final String methodName            = "findTypeDefsByCategory";
        final String categoryParameterName = "category";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

        /*
         * Perform operation
         */

        // Clear the per-API records of TypeDefs
        typeDefsForAPI = new TypeDefsByCategory();

        List<TypeDef> retList;
        try {
            retList = _findTypeDefsByCategory(userId, category);
        }
        catch (Exception e) {
            LOG.debug("findTypeDefsByCategory: re-throwing exception from _findTypeDefsByCategory ");
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findTypeDefsByCategory : retList={}", retList);
        }
        return retList;
    }

    private List<TypeDef> _findTypeDefsByCategory(String          userId,
                                                  TypeDefCategory category)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "_findTypeDefsByCategory";
        final String sourceName = metadataCollectionId;
        final String categoryParameterName = "category";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _findTypeDefsByCategory(userId={}, category={})", userId, category);
        }

        // Strategy: use searchTypesDef with a SearchFilter with type parameter set according to category
        String typeForSearchParameter;
        switch (category) {
            case ENTITY_DEF:
                typeForSearchParameter = "ENTITY";
                break;
            case RELATIONSHIP_DEF:
                typeForSearchParameter = "RELATIONSHIP";
                break;
            case CLASSIFICATION_DEF:
                typeForSearchParameter = "CLASSIFICATION";
                break;
            default:
                LOG.error("_findTypeDefsByCategory: unsupported category {}", category);
                return null;
        }
        SearchFilter searchFilter = new SearchFilter();
        searchFilter.setParam(SearchFilter.PARAM_TYPE, typeForSearchParameter);
        AtlasTypesDef atd;
        try {

            atd = typeDefStore.searchTypesDef(searchFilter);

        } catch (AtlasBaseException e) {

            LOG.error("_findTypeDefsByCategory: caught exception from Atlas type def store {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("category", categoryParameterName, methodName, sourceName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

           }

        /*
         * Parse the Atlas TypesDef
         * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
         * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
         * is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
         * supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
         * things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
         * anything that contains something that it does not understand (e.g. a reference attribute or a collection that
         * contains anything other than primitives).
         */


        // This method will populate the typeDefsForAPI object.
        if (atd != null) {
            convertAtlasTypeDefs(userId, atd);
        }

        // Retrieve the list of typedefs from the appropriate list in the typeDefsForAPI
        List<TypeDef> ret;
        switch (category) {
            case ENTITY_DEF:
                ret = typeDefsForAPI.getEntityDefs();
                break;
            case RELATIONSHIP_DEF:
                ret = typeDefsForAPI.getRelationshipDefs();
                break;
            case CLASSIFICATION_DEF:
                ret = typeDefsForAPI.getClassificationDefs();
                break;
            default:
                LOG.error("_findTypeDefsByCategory: unsupported category {}", category);
                return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== _findTypeDefsByCategory: ret={}", userId, category, ret);
        }
        return ret;
    }


    /**
     * Returns all of the AttributeTypeDefs for a specific category.
     *
     * @param userId - unique identifier for requesting user.
     * @param category - enum value for the category of an AttributeTypeDef to return.
     * @return AttributeTypeDefs list.
     * @throws InvalidParameterException - the TypeDefCategory is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<AttributeTypeDef> findAttributeTypeDefsByCategory(String                   userId,
                                                                  AttributeTypeDefCategory category)
        throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String methodName            = "findAttributeTypeDefsByCategory";
        final String categoryParameterName = "category";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findAttributeTypeDefsByCategory(userId={}, category={})", userId, category);
        }


        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

        /*
         * Perform operation
         */

        // Clear the per-API records of TypeDefs
        typeDefsForAPI = new TypeDefsByCategory();

        List<AttributeTypeDef> retList;
        try {
            retList = _findAttributeTypeDefsByCategory(userId, category);
        }
        catch (Exception e) {
            LOG.debug("findAttributeTypeDefsByCategory: re-throwing exception from _findAttributeTypeDefsByCategory ");
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findAttributeTypeDefsByCategory : retList={}", retList);
        }
        return retList;

    }

    private List<AttributeTypeDef> _findAttributeTypeDefsByCategory(String                   userId,
                                                                    AttributeTypeDefCategory category)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName            = "_findAttributeTypeDefsByCategory";
        final String categoryParameterName = "category";
        final String sourceName            = metadataCollectionId;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _findAttributeTypeDefsByCategory(userId={}, category={})", userId, category);
        }
        /*
         * Strategy:
         * Atlas handles Enum defs as types - whereas in OM they are attribute type defs, so for enums (only) use a search
         * like above for findTypeDefByCategory
         * For all other categories we need to do a different search - see below...
         */


        List<AttributeTypeDef> ret;


        // If category is ENUM_DEF use searchTypesDef with a SearchFilter with type parameter set according to category
        if (category == ENUM_DEF) {
            String typeForSearchParameter = "ENUM";
            SearchFilter searchFilter = new SearchFilter();
            searchFilter.setParam(SearchFilter.PARAM_TYPE, typeForSearchParameter);
            AtlasTypesDef atd;
            try {

                atd = typeDefStore.searchTypesDef(searchFilter);

            } catch (AtlasBaseException e) {

                LOG.error("_findAttributeTypeDefsByCategory: caught exception from Atlas type def store {}", e);

                OMRSErrorCode errorCode = OMRSErrorCode.ATTRIBUTE_TYPEDEF_NAME_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("category", categoryParameterName, methodName, sourceName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            /*
             * Parse the Atlas TypesDef
             * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
             * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
             * is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
             * supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
             * things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
             * anything that contains something that it does not understand (e.g. a reference attribute or a collection that
             * contains anything other than primitives).
             */


            // This method will populate the typeDefsForAPI object.
            if (atd != null) {
                convertAtlasTypeDefs(userId, atd);
            }

            // Retrieve the list of typedefs from the appropriate list in the typeDefsForAPI

            ret = typeDefsForAPI.getEnumDefs();
        }

        else {

            /*
             * Category is not ENUM - it should be PRIMITIVE or COLLECTION - or could be UNKNOWN_DEF or invalid.
             * In the case where we are looking for all attribute type defs by category PRIMITIVE or COLLECTION,
             * the best way may be to get all types and then return the appropriate section of the TDBC...
             * ... expensive operation but cannot currently see another way to achieve it.
             */

            try {
                loadAtlasTypeDefs(userId);
            } catch (RepositoryErrorException e) {
                LOG.error("getAllTypes: caught exception from Atlas {}", e);
                throw e;
            }

            switch (category) {
                case PRIMITIVE:
                    ret = typeDefsForAPI.getPrimitiveDefs();
                    break;
                case COLLECTION:
                    ret = typeDefsForAPI.getCollectionDefs();
                    break;
                default:
                    LOG.error("findAttributeTypeDefsByCategory: unsupported category {}", category);
                    ret = null;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findAttributeTypeDefsByCategory: ret={}", ret);
        }
        return ret;
    }


    /**
     * Return the TypeDefs that have the properties matching the supplied match criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param matchCriteria - TypeDefProperties - a list of property names.
     * @return TypeDefs list.
     * @throws InvalidParameterException - the matchCriteria is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> findTypeDefsByProperty(String             userId,
                                                TypeDefProperties  matchCriteria)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String  methodName                 = "findTypeDefsByProperty";
        final String  matchCriteriaParameterName = "matchCriteria";
        final String  sourceName                 = metadataCollectionId;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _findTypeDefsByProperty(userId={}, matchCriteria={})", userId, matchCriteria);
        }
        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateMatchCriteria(repositoryName, matchCriteriaParameterName, matchCriteria, methodName);

        /*
         * Perform operation
         */

        /*
         * Implementation: perform a search of the Atlas type defs and convert each into
         * corresponding OM TypeDef, filtering the results by properties in the matchCriteria
         * Return is a List<TypeDef>
         * AttributeTypeDefs are not included, so the list just contains
         *  1. EntityDefs
         *  2. RelationshipDefs
         *  3. ClassificationDefs
         */


        // The result of the load is constructed in typeDefsForAPI, so clear that first.
        typeDefsForAPI = new TypeDefsByCategory();

        // Strategy: use searchTypesDef with a null (default) SearchFilter.
        SearchFilter emptySearchFilter = new SearchFilter();
        AtlasTypesDef atd;
        try {

            atd = typeDefStore.searchTypesDef(emptySearchFilter);

        } catch (AtlasBaseException e) {

            LOG.error("findTypeDefsByProperty: caught exception from Atlas type def store {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("category", matchCriteriaParameterName, methodName, sourceName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Parse the Atlas TypesDef
         * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
         * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
         * is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
         * supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
         * things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
         * anything that contains something that it does not understand (e.g. a reference attribute or a collection that
         * contains anything other than primitives).
         */


        /*
         * This method will populate the typeDefsForAPI object.
         * This is also converting the ATDs which appears wasteful but does allow any problems
         * to be handled, resulting in the skipping of the problematic TypeDef.
         */
        if (atd != null) {
            convertAtlasTypeDefs(userId, atd);
        }


        // For the Entity, Relationship and Classification type defs, filter them through matchCriteria
        //List<String> matchPropertyNames = matchCriteria.getTypeDefProperties();
        Map<String,Object> matchProperties = matchCriteria.getTypeDefProperties();
        List<String> matchPropertyNames = new ArrayList(matchProperties.keySet());


        List<TypeDef> returnableTypeDefs = new ArrayList<>();

        // Filter EntityDefs
        List<TypeDef> entityDefs = typeDefsForAPI.getEntityDefs();
        if (entityDefs != null) {
            for (TypeDef typeDef : entityDefs) {
                // Get the property names for this TypeDef
                List<String> currentTypePropNames = getPropertyNames(userId, typeDef);
                if (propertiesContainAllMatchNames(currentTypePropNames,matchPropertyNames)) {
                    // Amalgamate each survivor into the returnable list...
                    returnableTypeDefs.add(typeDef);
                }
            }
        }

        // Filter RelationshipDefs
        List<TypeDef> relationshipDefs = typeDefsForAPI.getRelationshipDefs();
        if (relationshipDefs != null) {
            for (TypeDef typeDef : relationshipDefs) {
                // Get the property names for this TypeDef
                List<String> currentTypePropNames = getPropertyNames(userId, typeDef);
                if (propertiesContainAllMatchNames(currentTypePropNames,matchPropertyNames)) {
                    // Amalgamate each survivor into the returnable list...
                    returnableTypeDefs.add(typeDef);
                }
            }
        }

        // Filter ClassificationDefs
        List<TypeDef> classificationDefs = typeDefsForAPI.getClassificationDefs();
        if (classificationDefs != null) {
            for (TypeDef typeDef : classificationDefs) {
                // Get the property names for this TypeDef
                List<String> currentTypePropNames = getPropertyNames(userId, typeDef);
                if (propertiesContainAllMatchNames(currentTypePropNames,matchPropertyNames)) {
                    // Amalgamate each survivor into the returnable list...
                    returnableTypeDefs.add(typeDef);
                }
            }
        }


        List<TypeDef> returnValue = returnableTypeDefs.isEmpty() ? null : returnableTypeDefs;
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== _findTypeDefsByProperty: return={})",returnValue);
        }
        return returnValue;
    }

    /*
     * Utility method to recursively gather property names from a TypeDef
     */
    private List<String> getPropertyNames(String userId, TypeDef typeDef)
            throws
            RepositoryErrorException
    {
        List<String> discoveredPropNames = new ArrayList<>();
        List<TypeDefAttribute> typeDefAttributeList = typeDef.getPropertiesDefinition();
        if (typeDefAttributeList != null) {
            for (TypeDefAttribute tda : typeDefAttributeList) {
                String attrName = tda.getAttributeName();
                discoveredPropNames.add(attrName);
            }
        }
        TypeDefLink superTypeLink = typeDef.getSuperType();
        if (superTypeLink != null) {
            // Retrieve the supertype - the TDL gives us its GUID and name
            if (superTypeLink.getName() != null) {
                TypeDef superTypeDef;
                try {
                    superTypeDef = this._getTypeDefByName(userId, superTypeLink.getName());

                } catch (Exception e) {
                    LOG.error("getPropertyNames: caught exception from getTypeDefByName {}", e);
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("name", "getPropertyNames", metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "getPropertyNames",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                if (superTypeDef != null) {
                    List<String> superTypesPropNames = getPropertyNames(userId, superTypeDef);
                    if (superTypesPropNames != null) {
                        discoveredPropNames.addAll(superTypesPropNames);
                    }
                }
            }
        }
        if (discoveredPropNames.isEmpty())
            discoveredPropNames = null;
        return discoveredPropNames;
    }

    /*
     * Utility method to check whether all match names are contained in a list of property names
     */
    private boolean propertiesContainAllMatchNames(List<String> propNames, List<String> matchNames) {

        // Check the currentTypePropNames contains ALL the names in matchCriteria
        if (matchNames == null) {
            // There are no matchCriteria - the list of property names implicitly passes the filter test
            return true;
        }
        else {
            // There are matchCriteria - inspect the list of property names
            if (propNames == null) {
                // The prop list has no properties - instant match failure
                return false;
            }
            // It has been established that both currentTypePropNames and matchPropertyNames are not null
            boolean allMatchPropsFound = true;
            for (String matchName : matchNames) {
                boolean thisMatchPropFound = false;
                for (String propName : propNames) {
                    if (propName.equals(matchName)) {
                        thisMatchPropFound = true;
                        break;
                    }
                }
                if (!thisMatchPropFound) {
                    allMatchPropsFound = false;
                    break;
                }
            }
            return allMatchPropsFound;

        }
    }

    /**
     * Return the types that are linked to the elements from the specified standard.
     *
     * @param userId - unique identifier for requesting user.
     * @param standard - name of the standard - null means any, but either standard or organization must be specified
     * @param organization - name of the organization - null means any, but either standard or organization must be specified.
     * @param identifier - identifier of the element in the standard - must be specified (cannot be null)
     * @return TypeDefs list - each entry in the list contains a TypeDef.  This is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException - all attributes of the external id are null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef>  findTypesByExternalID(String userId,
                                                String standard,
                                                String organization,
                                                String identifier)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName = "findTypesByExternalID";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findTypesByExternalID(userId={},standard={},organization={},identifier={})",
                    userId, standard, organization, identifier);
        }

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateExternalId(repositoryName, standard, organization, identifier, methodName);

        /*
         * Perform operation
         */

        List<TypeDef> returnValue = null;

        // Pack the filter criteria into an ExternalStandardMapping
        ExternalStandardMapping filterCriteria = new ExternalStandardMapping();
        filterCriteria.setStandardName(standard);
        filterCriteria.setStandardOrganization(organization);
        filterCriteria.setStandardTypeName(identifier);

        /*
         * There is no point asking Atlas for type defs and then filtering on External Standards information,
         * because that information only exists in the RepositoryContentManager. So instead, ask the Repository
         * Helper, and then filter.
         */
        TypeDefGallery knownTypes = repositoryHelper.getKnownTypeDefGallery();
        List<TypeDef> knownTypeDefs = knownTypes.getTypeDefs();
        if (knownTypeDefs != null) {

            List<TypeDef> returnableTypeDefs = new ArrayList<>();

            /*
             * Look through the knownTypeDefs checking for the three strings we need to match...
             * According to the validator we are expecting at least one of standard OR organization to be non-null AND
             * for identifier to be non-null. So identifier must be present AND either standard OR organization must
             * be present.
             * This has been asserted by calling the validator's validateExternalId method.
             * For a TypeDef to match it must possess the specified standard mapping within its list of ESMs.
             */
            for (TypeDef typeDef : knownTypeDefs) {
                // Get the external standards fields for this TypeDef
                List<ExternalStandardMapping> typeDefExternalMappings = typeDef.getExternalStandardMappings();

                if (externalStandardsSatisfyCriteria(typeDefExternalMappings,filterCriteria)) {
                    // Amalgamate each survivor into the returnable list...
                    returnableTypeDefs.add(typeDef);
                }
            }

            if (!(returnableTypeDefs.isEmpty())) {
                returnValue = returnableTypeDefs;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findTypesByExternalID: return={})",returnValue);
        }
        return returnValue;

    }

    /*
     * Utility method to compare a list of external standards mappings with a set of filter criteria.
     * If the list contains at least one member that satisfies the criteria, the method returns true.
     * This method is based on the assumption that at least one of the filterCriteria is non null, as
     * established by the validator, in the caller.
     */
    private boolean externalStandardsSatisfyCriteria(List<ExternalStandardMapping> typeDefExternalMappings, ExternalStandardMapping filterCriteria) {

        String standardCriterion = filterCriteria.getStandardName();
        String organizationCriterion = filterCriteria.getStandardOrganization();
        String identifierCriterion = filterCriteria.getStandardTypeName();
        /*
         * In case the caller does not establish that there is at least one non-null filter criterion, take care of that here
         * If there are no criteria then the list will pass
         */
        if (standardCriterion == null && organizationCriterion == null && identifierCriterion == null ) {
            // Degenerate filterCriteria, test automatically passes
            return true;
        }

        /*
         * For each ESM in the list, check whether it satisfies the criteria in the filterCriteria.
         * If any ESM in the list matches then the list passes.
         * Return a boolean for the list.
         * When a filter criterion is null it means 'anything goes', so it is only the specific value(s)
         * that need to match.
         * If there is no filterCriteria.standard or there is and it matches the ESM passes
         * If there is no filterCriteria.organization or there is and it matches the ESM passes
         * If there is no filterCriteria.identifier or there is and it matches the ESM passes
         */
        if (typeDefExternalMappings == null) {
            // It has already been established that at least one of the filterCriteria is not null.
            // If that is the case and there are no ESMs in the list, then the list fails.
            return false;
        }
        else {
            boolean listOK = false;

            for (ExternalStandardMapping currentESM : typeDefExternalMappings ) {

                boolean standardSatisfied = ( standardCriterion == null || currentESM.getStandardName().equals(standardCriterion) );
                boolean organizationSatisfied = ( organizationCriterion == null || currentESM.getStandardOrganization().equals(organizationCriterion) );
                boolean identifierSatisfied = ( identifierCriterion == null || currentESM.getStandardTypeName().equals(identifierCriterion) );

                if (standardSatisfied && organizationSatisfied && identifierSatisfied ) {
                    // This ESM matches the criteria so the whole list passes...
                    listOK = true;
                    break;
                }
                // This ESM does not match the criteria but continue with the remainder of the list...
            }

            return listOK;
        }
    }


    /**
     * Return the TypeDefs that match the search criteria.
     *
     * @param userId - unique identifier for requesting user.
     * @param searchCriteria - String - search criteria.
     * @return TypeDefs list - each entry in the list contains a TypeDef.  This is is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException - the searchCriteria is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<TypeDef> searchForTypeDefs(String userId,
                                           String searchCriteria)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        final String methodName                  = "searchForTypeDefs";
        final String searchCriteriaParameterName = "searchCriteria";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> searchForTypeDefs(userId={},searchCriteria={})", userId, searchCriteria);
        }

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateSearchCriteria(repositoryName, searchCriteriaParameterName, searchCriteria, methodName);

        /*
         * Perform operation
         */

        /*
         * The searchCriteria is interpreted as a String to be used in a wildcard comparison with the name of the TypeDef.
         * It is only the name of the TypeDef that is compared.
         *
         */

        /*
         * Implementation: perform a search of the Atlas type defs and convert each into
         * corresponding OM TypeDef, filtering the results by testing whether the searchCriteria
         * string is contained in (or equal to) the name of each TypeDef.
         * Return is a List<TypeDef>
         * AttributeTypeDefs are not included, so the list just contains
         *  1. EntityDefs
         *  2. RelationshipDefs
         *  3. ClassificationDefs
         */


        // The result of the load is constructed in typeDefsForAPI, so clear that first.
        typeDefsForAPI = new TypeDefsByCategory();

        // Strategy: use searchTypesDef with a null (default) SearchFilter.
        SearchFilter emptySearchFilter = new SearchFilter();
        AtlasTypesDef atd;
        try {

            atd = typeDefStore.searchTypesDef(emptySearchFilter);

        } catch (AtlasBaseException e) {
            LOG.error("searchForTypeDefs:caught exception from attempt to retrieve all TypeDefs from Atlas repository", e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(searchCriteria, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        /*
         * Parse the Atlas TypesDef
         * Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
         * and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
         *  is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
         *  supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
         *  things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
         *  anything that contains something that it does not understand (e.g. a reference attribute or a collection that
         *  contains anything other than primitives).
         *
         * This method will populate the typeDefsForAPI object.
         * This is also converting the ATDs which appears wasteful but does allow any problems
         * to be handled, resulting in the skipping of the problematic TypeDef.
         */
        if (atd != null) {
            convertAtlasTypeDefs(userId, atd);
        }


        // For the Entity, Relationship and Classification type defs, filter them through matchCriteria

        List<TypeDef> returnableTypeDefs = new ArrayList<>();

        // Filter EntityDefs
        List<TypeDef> entityDefs = typeDefsForAPI.getEntityDefs();
        if (entityDefs != null) {
            for (TypeDef typeDef : entityDefs) {
                if (typeDef.getName().matches(searchCriteria)) {
                    returnableTypeDefs.add(typeDef);
                }
            }
        }

        // Filter RelationshipDefs
        List<TypeDef> relationshipDefs = typeDefsForAPI.getEntityDefs();
        if (relationshipDefs != null) {
            for (TypeDef typeDef : relationshipDefs) {
                if (typeDef.getName().matches(searchCriteria)) {
                    returnableTypeDefs.add(typeDef);
                }
            }
        }

        // Filter ClassificationDefs
        List<TypeDef> classificationDefs = typeDefsForAPI.getEntityDefs();
        if (classificationDefs != null) {
            for (TypeDef typeDef : classificationDefs) {
                if (typeDef.getName().matches(searchCriteria)) {
                    returnableTypeDefs.add(typeDef);
                }
            }
        }


        List<TypeDef> returnValue = null;
        if (!(returnableTypeDefs.isEmpty())) {
            returnValue =  returnableTypeDefs;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== searchForTypeDefs: return={})", returnValue);
        }
        return returnValue;

    }


    /**
     * Return the TypeDef identified by the GUID.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid - String unique id of the TypeDef
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException - the guid is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException - The requested TypeDef is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef getTypeDefByGUID(String userId,
                                    String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getTypeDefByGUID(userId={}, guid={})", userId, guid);
        }

        final String methodName        = "getTypeDefByGUID";
        final String guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */


        // Initialisation

        // Clear the transient type defs
        this.typeDefsForAPI = new TypeDefsByCategory();


        // Invoke internal helper method
        TypeDef ret;
        try {
            // The underlying method handles Famous Five conversions.
            ret = _getTypeDefByGUID(userId, guid);
        }
        catch (RepositoryErrorException | TypeDefNotKnownException e) {
            LOG.error("getTypeDefByGUID: re-throwing exception from internal method", e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getTypeDefByGUID: ret={}", ret);
        }
        return ret;
    }


    /*
     * Internal implementation of getTypeDefByGUID()
     * This is separated so that other methods can use it without resetting the typeDefsForAPI object
     */
    private TypeDef _getTypeDefByGUID(String userId,
                                      String guid)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException {
        // Strategy:
        // Check guid is not null. null => return null
        // Use Atlas typedef store getByName()
        // If you get back a typedef of a category that can be converted to OM typedef then convert it and return the type def.
        // If Atlas type is not of a category that can be converted to an OM TypeDef then return throw TypeDefNotKnownException.

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _getTypeDefByGUID(userId={}, guid={})", userId, guid);
        }

        // This is an internal helper method so should never get null guid; but if so return null.
        if (guid == null)
            return null;


        // Retrieve the AtlasBaseTypeDef
        AtlasBaseTypeDef abtd;

        try {

            if (!useRegistry) {
                // Look in the Atlas type def store
                abtd = typeDefStore.getByGuid(guid);
            }
            else {
                // Using registry
                abtd = typeRegistry.getTypeDefByGuid(guid);
            }

        } catch (AtlasBaseException e) {

            if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_GUID_NOT_FOUND) {
                LOG.error("_getTypeDefByGUID: Atlas does not have the type with guid {} ", guid, e);
                // The AttributeTypeDef was not found - return null
                abtd = null;
            } else {

                LOG.debug("_getTypeDefByGUID: caught exception from Atlas getByGuid using guid {}", guid);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("guid", "_getTypeDefByGuid", metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getTypeDefByGUID",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }


        if (abtd == null) {
            LOG.debug("_getTypeDefByGUID: Atlas does not have the type with guid {} ", guid);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("guid", "_getTypeDefByGuid",metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getTypeDefByGUID",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Underlying method handles Famous Five conversions
        TypeDef ret;
        try {
            ret = convertAtlasTypeDefToOMTypeDef(userId, abtd);
        }
        catch (TypeErrorException e) {
            LOG.error("_getTypeDefByGUID: Failed to convert the Atlas type {} to an OM TypeDef", abtd.getName(), e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("guid", "_getTypeDefByGuid",metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getTypeDefByGUID",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== _getTypeDefByGUID: ret={}", ret);
        }
        return ret;

    }



    /**
     * Return the AttributeTypeDef identified by the GUID
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String name of the AttributeTypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException  - the name is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested AttributeTypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public AttributeTypeDef getAttributeTypeDefByGUID(String userId,
                                                      String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAttributeTypeDefByGUID(userId={}, guid={})", userId, guid);
        }

        final String methodName        = "getAttributeTypeDefByGUID";
        final String guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */

        // Initialisation

        // Clear the transient type defs
        this.typeDefsForAPI = new TypeDefsByCategory();

        // Return object
        AttributeTypeDef ret;
        try {
            ret = _getAttributeTypeDefByGUID(userId, guid);
        }
        catch (RepositoryErrorException | TypeDefNotKnownException e) {
            LOG.error("getAttributeTypeDefByGUID: re-throwing exception from internal method", e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAttributeTypeDefByGUID): ret={}", userId, guid, ret);
        }
        return ret;
    }

    /*
     * Internal implementation of getAttributeTypeDefByGUID
     * This is separated so that other methods can use it without resetting the typeDefsForAPI object
     */
    private AttributeTypeDef _getAttributeTypeDefByGUID(String userId,
                                                        String guid)
        throws
            RepositoryErrorException,
            TypeDefNotKnownException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _getAttributeTypeDefByGUID(userId={}, guid={})", userId, guid);
        }

        // Strategy:
        // Check guid is not null. null => return null (it should already have been checked)
        // Use Atlas typedef store getByName()
        // If you get back a AttributeTypeDef of a category that can be converted to OM AttributeTypeDef
        // then convert it and return the AttributeTypeDef.
        // If Atlas type is not of a category that can be converted to an OM AttributeTypeDef then return throw TypeDefNotKnownException.

        if (guid == null) {
            LOG.error("_getAttributeTypeDefByGUID: guid is null");
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByGUID", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getAttributeTypeDefByGUID",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Look in the Atlas type def store
        AtlasBaseTypeDef abtd;
        try {
            if (!useRegistry) {
                // Look in the Atlas type def store
                abtd = typeDefStore.getByGuid(guid);
            } else {
                // Using registry
                abtd = typeRegistry.getTypeDefByGuid(guid);
            }

        } catch (AtlasBaseException e) {

            if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_GUID_NOT_FOUND) {
                LOG.debug("_getAttributeTypeDefByGUID: Atlas does not have the type with guid {} ", guid, e);
                // The AttributeTypeDef was not found - return null
                abtd = null;
            } else {
                LOG.error("_getAttributeTypeDefByGUID: caught exception trying to retrieve type with guid {}", guid, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByGUID", metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getAttributeTypeDefByGUID",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        if (abtd == null) {
            LOG.debug("_getAttributeTypeDefByGUID: Atlas does not have the type with guid {} ", guid);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("guid", "_getAttributeTypeDefByGUID", metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getAttributeTypeDefByGUID",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // abtd is known to be good

        AttributeTypeDef ret;

        // Generate a candidate OM TypeDef
        AttributeTypeDef candidateAttributeTypeDef;

        // Find the category of the Atlas typedef and invoke the relevant conversion method.
        // The only Atlas type categories that we can convert to OM AttributeTypeDef are:
        // PRIMITIVE: ENUM: ARRAY: MAP:
        // Anything else we will bounce and return null.
        // This will populate TDBC - you then need to retrieve the TD and return it.
        TypeCategory atlasCat = abtd.getCategory();
        switch (atlasCat) {

            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
                // For any of these categories get a candidate ATD
                //candidateAttributeTypeDef = convertAtlasBaseTypeDefToAttributeTypeDef(abtd);
                AtlasBaseTypeDefMapper abtdMapper = new AtlasBaseTypeDefMapper(abtd);
                candidateAttributeTypeDef = abtdMapper.toAttributeTypeDef();
                break;

            case ENTITY:
            case RELATIONSHIP:
            case CLASSIFICATION:
            case STRUCT:
            case OBJECT_ID_TYPE:
            default:
                LOG.error("_getAttributeTypeDefByGUID: Atlas type has category cannot be converted to OM AttributeTypeDef, category {} ", atlasCat);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByGUID", metadataCollectionId);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getAttributeTypeDefByGUID",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }


        if (candidateAttributeTypeDef == null) {
            LOG.error("_getAttributeTypeDefByGUID: candidateAttributeTypeDef is null");
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByGUID", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getAttributeTypeDefByGUID",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        } else {
            // Finally, check if the converted attributeTypeDef is known by the repos helper and whether it matches exactly.
            // If it doesn't exist in RH, return the copy we got from the TDBC.
            // If it does exist in RH then perform a deep compare; exact match => return the ATD from the RH
            // If it does exist in RH but is not an exact match => audit log and return null;

            // Ask RepositoryContentManager whether there is an AttributeTypeDef with supplied name
            String source = metadataCollectionId;
            AttributeTypeDef existingAttributeTypeDef;
            String name = candidateAttributeTypeDef.getName();
            try {
                existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, name);
            } catch (OMRSLogicErrorException e) {
                LOG.error("_getAttributeTypeDefByGUID: Caught exception from repository helper for attribute type def with name {}", name, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByGUID", metadataCollectionId);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getAttributeTypeDefByGUID",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            if (existingAttributeTypeDef == null) {
                // The RH does not have a typedef by the supplied name - use the candidateTypeDef
                LOG.debug("_getAttributeTypeDefByGUID: repository content manager returned name not found - use the candidate AttributeTypeDef");
                candidateAttributeTypeDef.setDescriptionGUID(null);
                // candidateAttributeTypeDef will be returned at end of method
                ret = candidateAttributeTypeDef;
            } else {
                // RH returned an AttributeTypeDef; cast it by category and compare against candidate
                // If match we will use RH TD; if not match we will generate audit log entry
                LOG.debug("_getAttributeTypeDefByGUID: RepositoryHelper returned a TypeDef with name {} : {}", name, existingAttributeTypeDef);
                LOG.debug("_getAttributeTypeDefByGUID: RepositoryHelper TypeDef has category {} ", existingAttributeTypeDef.getCategory());
                boolean typematch;
                Comparator comp = new Comparator();
                switch (existingAttributeTypeDef.getCategory()) {

                    case PRIMITIVE:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        PrimitiveDef existingPrimitiveDef = (PrimitiveDef) existingAttributeTypeDef;
                        PrimitiveDef newPrimitiveDef = (PrimitiveDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingPrimitiveDef, newPrimitiveDef);
                        break;

                    case COLLECTION:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        CollectionDef existingCollectionDef = (CollectionDef) existingAttributeTypeDef;
                        CollectionDef newCollectionDef = (CollectionDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingCollectionDef, newCollectionDef);
                        break;

                    case ENUM_DEF:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        EnumDef existingEnumDef = (EnumDef) existingAttributeTypeDef;
                        EnumDef newEnumDef = (EnumDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingEnumDef, newEnumDef);
                        break;

                    default:
                        LOG.error("_getAttributeTypeDefByGUID: repository content manager found TypeDef has category {} ", existingAttributeTypeDef.getCategory());
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByGUID", metadataCollectionId);

                        throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                "_getAttributeTypeDefByGUID",
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                }

                // If compare matches use the known type
                if (typematch) {
                    // We will add the attributeTypeDef to the TypeDefGallery
                    LOG.debug("_getAttributeTypeDefByGUID: return the repository content manager TypeDef with name {}", name);
                    candidateAttributeTypeDef = existingAttributeTypeDef;
                    ret = candidateAttributeTypeDef;

                } else {
                    // If compare failed generate AUDIT log entry and abandon
                    LOG.error("_getAttributeTypeDefByGUID: repository content manager found clashing def with name {}", name);
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("atlasEntityDef", "_getAttributeTypeDefByGUID", metadataCollectionId);

                    throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "_getAttributeTypeDefByGUID",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== _getAttributeTypeDefByGUID: ret={}", userId, guid, ret);
        }
        return ret;
    }


    /**
     * Return the TypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name   - String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException  - the name is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested TypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef getTypeDefByName(String userId,
                                    String name)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getTypeDefByName(userId={}, name={}", userId, name);
        }

        final String  methodName = "getTypeDefByName";
        final String  nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        /*
         * Perform operation
         */

        // Clear the transient type defs
        this.typeDefsForAPI = new TypeDefsByCategory();

        TypeDef ret;
        try {
            ret = _getTypeDefByName(userId, name);
        }
        catch (RepositoryErrorException | TypeDefNotKnownException e) {
            LOG.error("getTypeDefByName: re-throwing exception from internal method", e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getTypeDefByName: ret={}", ret);
        }
        return ret;
    }



    /**
     * Internal implementation of getTypeDefByName()
     * @param userId     - unique identifier for requesting user.
     * @param omTypeName - String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     */
    // package private
    TypeDef _getTypeDefByName(String userId,
                              String omTypeName)
        throws
            RepositoryErrorException,
            TypeDefNotKnownException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _getTypeDefByName(userId={}, omTypeName={})", userId, omTypeName);
        }
        // Strategy:
        // Check name is not null. null => throw
        // Use Atlas typedef store getByName()
        // If you get back a typedef of a category that can be converted to OM typedef then convert it and return the type def.
        // If Atlas type is not of a category that can be converted to an OM TypeDef then return throw TypeDefNotKnownException.


        // Look in the Atlas type def store

        // If the OM typeName is in famous five then you need to look for the corresponding Atlas name
        String atlasTypeName = omTypeName;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName)) {
            // The type to be looked up is in the famous five.
            // We do not have the OM GUID but all we need at the moment is the Atlas type name to look up so GUID can be null
            atlasTypeName = FamousFive.getAtlasTypeName(omTypeName, null);
        }

        AtlasBaseTypeDef abtd;
        try {
            if (!useRegistry) {
                // Look in the Atlas type def store
                abtd = typeDefStore.getByName(atlasTypeName);
            }
            else {
                // Using registry
                abtd = typeRegistry.getTypeDefByName(atlasTypeName);
            }

        } catch (AtlasBaseException e) {
            if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_NAME_NOT_FOUND) {
                LOG.debug("_getTypeDefByName: Atlas does not have the type with name {} ", atlasTypeName);
                // The AttributeTypeDef was not found - ensure abtd is null, exception to be handled below
                abtd = null;
            }
            else {
                LOG.error("_getTypeDefByName: Caught exception trying to retrieve Atlas type with name {} ", atlasTypeName, e);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getTypeDefByName",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        // Separate null check ensures we have covered both cases (registry and store)
        if (abtd == null) {

            LOG.debug("_getTypeDefByName: received null return from Atlas getByName using name {}", atlasTypeName);
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(atlasTypeName, "unknown", "name", "_getTypeDefByName", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // From here on we know that abtd is not null


        // Underlying method handles Famous Five conversions
        TypeDef ret;
        try {
            ret = convertAtlasTypeDefToOMTypeDef(userId, abtd);
        }
        catch (TypeErrorException e) {
            LOG.error("_getTypeDefByName: Failed to convert the Atlas type {} to an OM TypeDef", abtd.getName(), e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("atlasEntityDef", "getTypeDefByGuid",metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== _getTypeDefByName: ret={}", ret);
        }

        return ret;

    }


    /**
     * Return the AttributeTypeDef identified by the unique name.
     *
     * @param userId - unique identifier for requesting user.
     * @param name   - String name of the AttributeTypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException  - the name is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested AttributeTypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public AttributeTypeDef getAttributeTypeDefByName(String userId,
                                                      String name)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAttributeTypeDefByName(userId={}, name={})", userId, name);
        }

        final String  methodName = "getAttributeTypeDefByName";
        final String  nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        /*
         * Perform operation
         */

        // Initialisation

        // Clear the transient type defs
        this.typeDefsForAPI = new TypeDefsByCategory();


        // Return object
        AttributeTypeDef ret;
        try {
            ret = _getAttributeTypeDefByName(userId, name);
        }
        catch (RepositoryErrorException | TypeDefNotKnownException e) {
            LOG.error("getAttributeTypeDefByName: re-throwing exception from internal method", e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAttributeTypeDefByName: ret={}", ret);
        }
        return ret;
    }

    /*
     * Internal implementation of getAttributeTypeDefByName
     */
    private AttributeTypeDef _getAttributeTypeDefByName(String userId,
                                                        String name)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException

    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _getAttributeTypeDefByName(userId={}, name={})", userId, name);
        }

        // Strategy:
        // Check name is not null. null => throw exception
        // Use Atlas typedef store getByName()
        // If you get an AttributeTypeDef of a category that can be converted to OM AttributeTypeDef
        // then convert it and return the AttributeTypeDef.
        // If Atlas type is not of a category that can be converted to an OM AttributeTypeDef then return throw TypeDefNotKnownException.

       if (name == null) {
           LOG.error("_getAttributeTypeDefByName: Cannot get Atlas type with null name");
           OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
           String errorMessage = errorCode.getErrorMessageId()
                   + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByName", metadataCollectionId);

           throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                   this.getClass().getName(),
                   "_getAttributeTypeDefByName",
                   errorMessage,
                   errorCode.getSystemAction(),
                   errorCode.getUserAction());
       }


        // Look in the Atlas type def store or registry.
        // Note that on typedef not exists, the registry will return null, whereas the store will throw an exception
        AtlasBaseTypeDef abtd;
        try {
            if (!useRegistry) {
                // Look in the Atlas type def store
                abtd = typeDefStore.getByName(name);
            }
            else {
                // Using registry
                abtd = typeRegistry.getTypeDefByName(name);
            }

        } catch (AtlasBaseException e) {
            if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_NAME_NOT_FOUND) {
                LOG.debug("_getAttributeTypeDefByName: Atlas does not have the type with name {} ", name);
                // The AttributeTypeDef was not found - ensure abtd is null, exception to be handled below
                abtd = null;
            }
            else {
                LOG.error("_getAttributeTypeDefByName: Caught exception trying to retrieve Atlas type with name {} ", name, e);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getAttributeTypeDefByName",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        // Separate null check ensures we have covered both cases (registry and store)
        if (abtd == null) {

            LOG.debug("_getAttributeTypeDefByName: received null return from Atlas getByName using name {}", name);
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(name, "unknown", "name", "_getAttributeTypeDefByName", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getAttributeTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // From this point, we know that abtd is non-null

        AttributeTypeDef ret;

        // Generate a candidate OM TypeDef
        AttributeTypeDef candidateAttributeTypeDef;

        // Find the category of the Atlas typedef and invoke the relevant conversion method.
        // The only Atlas type categories that we can convert to OM AttributeTypeDef are:
        // PRIMITIVE: ENUM: ARRAY: MAP:
        // Anything else we will bounce and return null.
        // This will populate TDBC - you then need to retrieve the TD and return it.
        TypeCategory atlasCat = abtd.getCategory();
        switch (atlasCat) {
            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
                // For any of these categories get a candidate ATD
                //candidateAttributeTypeDef = convertAtlasBaseTypeDefToAttributeTypeDef(abtd);
                AtlasBaseTypeDefMapper abtdMapper = new AtlasBaseTypeDefMapper(abtd);
                candidateAttributeTypeDef = abtdMapper.toAttributeTypeDef();
                break;
            case ENTITY:
            case RELATIONSHIP:
            case CLASSIFICATION:
            case STRUCT:
            case OBJECT_ID_TYPE:
            default:
                LOG.debug("_getAttributeTypeDefByName: Atlas type has category cannot be converted to OM AttributeTypeDef, category {} ", atlasCat);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByName", metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getAttributeTypeDefByName",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }

        if (candidateAttributeTypeDef == null) {
            LOG.debug("_getAttributeTypeDefByName: received null return from attempt to convert AtlasBaseTypeDef to AttributeTypeDef");
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByName", metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getAttributeTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        } else {
            // Finally, check if the converted attributeTypeDef is known by the repos helper and whether it matches exactly.
            // If it doesn't exist in RH, return the copy we got from the TDBC.
            // If it does exist in RH then perform a deep compare; exact match => return the ATD from the RH
            // If it does exist in RH but is not an exact match => audit log and return null;

            // Ask RepositoryContentManager whether there is an AttributeTypeDef with supplied name
            String source = metadataCollectionId;
            AttributeTypeDef existingAttributeTypeDef;
            try {
                existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, name);
            } catch (OMRSLogicErrorException e) {
                LOG.error("_getAttributeTypeDefByName: caught exception from repository helper for type name {}", name, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByName", metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getAttributeTypeDefByName",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            if (existingAttributeTypeDef == null) {
                // The RH does not have a typedef by the supplied name - use the candidateTypeDef
                LOG.debug("_getAttributeTypeDefByName: repository content manager returned name not found - use the candidate AttributeTypeDef");
                candidateAttributeTypeDef.setDescriptionGUID(null);
                // candidateAttributeTypeDef will be returned at end of method
                ret = candidateAttributeTypeDef;
            } else {
                // RH returned an AttributeTypeDef; cast it by category and compare against candidate
                // If match we will use RH TD; if not match we will generate audit log entry
                LOG.debug("_getAttributeTypeDefByName: RepositoryHelper returned a TypeDef with name {} : {}", name, existingAttributeTypeDef);
                boolean typematch;
                Comparator comp = new Comparator();
                switch (existingAttributeTypeDef.getCategory()) {
                    case PRIMITIVE:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        PrimitiveDef existingPrimitiveDef = (PrimitiveDef) existingAttributeTypeDef;
                        PrimitiveDef newPrimitiveDef = (PrimitiveDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingPrimitiveDef, newPrimitiveDef);
                        break;
                    case COLLECTION:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        CollectionDef existingCollectionDef = (CollectionDef) existingAttributeTypeDef;
                        CollectionDef newCollectionDef = (CollectionDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingCollectionDef, newCollectionDef);
                        break;
                    case ENUM_DEF:
                        // Perform a deep compare of the known type and new type
                        //Comparator comp = new Comparator();
                        EnumDef existingEnumDef = (EnumDef) existingAttributeTypeDef;
                        EnumDef newEnumDef = (EnumDef) candidateAttributeTypeDef;
                        typematch = comp.compare(true, existingEnumDef, newEnumDef);
                        break;
                    default:
                        LOG.debug("_getAttributeTypeDefByName: repository content manager found TypeDef has category {} ", existingAttributeTypeDef.getCategory());
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByName", metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                "_getAttributeTypeDefByName",
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                }

                // If compare matches use the known type
                if (typematch) {
                    // We will add the attributeTypeDef to the TypeDefGallery
                    LOG.debug("_getAttributeTypeDefByName: return the repository content manager TypeDef with name {}", name);
                    candidateAttributeTypeDef = existingAttributeTypeDef;
                    ret = candidateAttributeTypeDef;
                } else {
                    // If compare failed generate AUDIT log entry and abandon
                    LOG.debug("_getAttributeTypeDefByName: repository content manager found clashing def with name {}", name);
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("name", "_getAttributeTypeDefByName", metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "_getAttributeTypeDefByName",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== _getAttributeTypeDefByName: ret={}", ret);
        }
        return ret;
    }


    /**
     * Create a collection of related types.
     *
     * @param userId - unique identifier for requesting user.
     * @param newTypes - TypeDefGalleryResponse structure describing the new AttributeTypeDefs and TypeDefs.
     * @throws InvalidParameterException - the new TypeDef is null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException - the new TypeDef has invalid contents
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void addTypeDefGallery(String         userId,
                                  TypeDefGallery newTypes)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException

    {

        final String  methodName = "addTypeDefGallery";
        final String  galleryParameterName = "newTypes";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addTypeDefGallery(userId={}, newTypes={})", userId, newTypes);
        }

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefGallery(repositoryName, galleryParameterName, newTypes, methodName);

        /*
         * Perform operation
         */

        /*
         * Parse the TypeDefGallery and for each category of TypeDef and AttributeTypeDef perform the
         * corresponding add operation.
         * AttributeTypeDefs and handled first, then TypeDefs.
         *
         * All exceptions thrown by the called methods match the signature of this method, so let them
         * throw through.
         */

        List<AttributeTypeDef> attributeTypeDefs = newTypes.getAttributeTypeDefs();
        if (attributeTypeDefs != null) {
            for (AttributeTypeDef attributeTypeDef : attributeTypeDefs) {
                 addAttributeTypeDef(userId,attributeTypeDef);
            }
        }

        List<TypeDef> typeDefs = newTypes.getTypeDefs();
        if (typeDefs != null) {
            for (TypeDef typeDef : typeDefs) {
                addTypeDef(userId,typeDef);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addTypeDefGallery");
        }
    }



    /**
     * Create a definition of a new TypeDef.
     *
     * @param userId     - unique identifier for requesting user.
     * @param newTypeDef - TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException    - the new TypeDef is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException        - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException     - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException      - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public void addTypeDef(String  userId,
                           TypeDef newTypeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException {


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addTypeDef(userId={}, newTypeDef={})", userId, newTypeDef);
        }

        final String methodName = "addTypeDef";
        final String typeDefParameterName = "newTypeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDef(repositoryName, typeDefParameterName, newTypeDef, methodName);
        repositoryValidator.validateUnknownTypeDef(repositoryName, typeDefParameterName, newTypeDef, methodName);

        /*
         * Perform operation
         */

        /*
         * Validate the status fields in the passed TypeDef
         */
        boolean statusFieldsValid = validateStatusFields(newTypeDef);
        if (!statusFieldsValid) {
            // We cannot accept this typedef because it contains status fields that cannot be modelled by Atlas
            LOG.error("addTypeDef: The TypeDef with name {}, initialStatus {} and validInstanceStatusList {} could not be modelled in Atlas", newTypeDef, newTypeDef.getInitialStatus(), newTypeDef.getValidInstanceStatusList());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(newTypeDef.getName(), newTypeDef.getGUID(), typeDefParameterName, methodName, repositoryName, newTypeDef.toString());
            throw new TypeDefNotSupportedException(
                    errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        /*
         * Given an OM TypeDef - convert it to corresponding Atlas type def, store it, then read it back and convert
         * it back to OM. The write-through and read-back is so that we pick up any Atlas initializations, e.g. of
         * fields like createdBy, description (if originally null), etc.
         */


        /*
         *  To do this we need to put the Atlas type definition into an AtlasTypesDef
         *  Create an empty AtlasTypesDef container for type to be converted below
         */
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();

        /*
         *  Get category of OM TypeDef, then depending on category instantiate the corresponding Atlas typedef, which will
         * be one of AtlasEntityDef, AtlasRelationshipDef or AtlasClassificationDef
         */
        String newTypeName = newTypeDef.getName();
        TypeDefCategory category = newTypeDef.getCategory();
        LOG.debug("addTypeDef: was passed an OM TypeDef with name {} and category {}", newTypeName, category);
        switch (category) {

            case ENTITY_DEF:
                // The following method will detect a Famous Five type and convert accordingly.
                AtlasEntityDef atlasEntityDef = convertOMEntityDefToAtlasEntityDef((EntityDef) newTypeDef);
                if (atlasEntityDef != null) {
                    atlasTypesDef.getEntityDefs().add(atlasEntityDef);
                }
                break;

            case RELATIONSHIP_DEF:
                try {
                    AtlasRelationshipDef atlasRelationshipDef = convertOMRelationshipDefToAtlasRelationshipDef((RelationshipDef) newTypeDef);
                    if (atlasRelationshipDef != null) {
                        atlasTypesDef.getRelationshipDefs().add(atlasRelationshipDef);
                    }

                } catch (RepositoryErrorException | TypeErrorException e) {
                    // Log the error and re-throw
                    LOG.debug("addTypeDef: caught exception from attempt to convert OM RelationshipDef to Atlas, name {}", newTypeDef.getName());
                    OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("TypeDef", "retrieval", metadataCollectionId);

                    throw new InvalidTypeDefException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "addTypeDef",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                break;

            case CLASSIFICATION_DEF:
                AtlasClassificationDef atlasClassificationDef = convertOMClassificationDefToAtlasClassificationDef((ClassificationDef) newTypeDef);
                if (atlasClassificationDef != null) {
                    atlasTypesDef.getClassificationDefs().add(atlasClassificationDef);
                }
                break;

            case UNKNOWN_DEF:
                LOG.debug("addTypeDef: cannot convert an OM TypeDef with category {}", category);
                break;
        }


        AtlasBaseTypeDef retrievedAtlasTypeDef;

        try {
            /*
             * Add the AtlasTypesDef to the typeDefStore.
             */
            LOG.debug("addTypeDef: add AtlasTypesDef {} to store", atlasTypesDef);
            typeDefStore.createTypesDef(atlasTypesDef);
            /*
             * Read the AtlasTypeDef back and convert it back into an OM TypeDef.
             */
            retrievedAtlasTypeDef = typeDefStore.getByName(newTypeName);
            LOG.debug("addTypeDef: retrieved created type from store {}", atlasTypesDef);

        } catch (AtlasBaseException e) {

            LOG.error("addTypeDef: exception from store and retrieve AtlasTypesDef {}", e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("TypeDef", "retrieval", metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "addTypeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Formulate the return value - the retrieved Atlas Type is converted into an OM TypeDef
         */
        TypeDef returnableTypeDef = null;
        boolean fatalError = false;
        switch (retrievedAtlasTypeDef.getCategory()) {

            case ENTITY:
                AtlasEntityDef retrievedAtlasEntityDef = (AtlasEntityDef) retrievedAtlasTypeDef;
                EntityDef returnableEntityDef;
                try {
                    AtlasEntityDefMapper atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, retrievedAtlasEntityDef);
                    returnableEntityDef = atlasEntityDefMapper.toOMEntityDef();
                    returnableTypeDef = returnableEntityDef;
                } catch (Exception e) {
                    fatalError = true;
                }
                break;

            case RELATIONSHIP:
                AtlasRelationshipDef retrievedAtlasRelationshipDef = (AtlasRelationshipDef) retrievedAtlasTypeDef;
                RelationshipDef returnableRelationshipDef;
                try {
                    AtlasRelationshipDefMapper atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, retrievedAtlasRelationshipDef);
                    returnableRelationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
                    returnableTypeDef = returnableRelationshipDef;
                } catch (Exception e) {
                    fatalError = true;
                }
                break;

            case CLASSIFICATION:
                AtlasClassificationDef retrievedAtlasClassificationDef = (AtlasClassificationDef) retrievedAtlasTypeDef;
                ClassificationDef returnableClassificationDef;
                try {
                    AtlasClassificationDefMapper atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, retrievedAtlasClassificationDef);
                    returnableClassificationDef = atlasClassificationDefMapper.toOMClassificationDef();
                    returnableTypeDef = returnableClassificationDef;
                } catch (Exception e) {
                    fatalError = true;
                }
                break;

            default:
                LOG.debug("addTypeDef: cannot convert an OM TypeDef with category {}", category);
                fatalError = true;
        }

        if (fatalError || returnableTypeDef == null) {

            LOG.error("addTypeDef: could not initialise mapper or convert retrieved AtlasBaseTypeDef {} to OM TypeDef", newTypeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("TypeDef", "addTypeDef", metadataCollectionId);
            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "addTypeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addTypeDef(userId={}, newTypeDef={})", userId, returnableTypeDef);
        }

    }


    /**
     * Create a definition of a new AttributeTypeDef.
     *
     * @param userId              - unique identifier for requesting user.
     * @param newAttributeTypeDef - TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException    - the new TypeDef is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException        - the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException     - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException      - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public void addAttributeTypeDef(String           userId,
                                    AttributeTypeDef newAttributeTypeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addAttributeTypeDef(userId={}, newAttributeTypeDef={})", userId, newAttributeTypeDef);
        }

        final String  methodName           = "addAttributeTypeDef";
        final String  typeDefParameterName = "newAttributeTypeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDef(repositoryName, typeDefParameterName, newAttributeTypeDef, methodName);
        repositoryValidator.validateUnknownAttributeTypeDef(repositoryName, typeDefParameterName, newAttributeTypeDef, methodName);

        /*
         * Perform operation
         */

        /*
         *  Given an OM AttributeTypeDef - convert it to corresponding AtlasAttributeDef
         *
         *  If asked to create a Primitive or Collection there is not really a corresponding Atlas type, so
         *  need to detect whether we are creating an Enum... otherwise no action.
         *
         * Approach:
         * Get the category of OM AttributeTypeDef, then depending on category instantiate the corresponding
         * AtlasAttributeDef
         *
         *  AttributeTypeDefCategory category        --> switch between Atlas cases and CTOR will set Atlas category
         *  The category is one of: UNKNOWN_DEF, PRIMITIVE, COLLECTION, ENUM_DEF
         *   String                   guid            --> IGNORED
         *   String                   name            --> Atlas name
         *   String                   description     --> Atlas description
         *   String                   descriptionGUID --> IGNORED (these are always set to null for now)
         *
         */


        // Create an empty AtlasTypesDef container for whatever we convert below..
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();

        AttributeTypeDefCategory category = newAttributeTypeDef.getCategory();

        if (category == ENUM_DEF) {
            EnumDef omEnumDef = (EnumDef) newAttributeTypeDef;
            // Building an AtlasEnumDef we need all the fields of the AtlasBaseTypeDef plus the
            // AtlasEnumDef specific extensions, i.e.
            // TypeCategory category; --> set by CTOR
            // String  guid           --> set from OM guid
            // String  createdBy      --> set to user
            // String  updatedBy      --> set to user
            // Date    createTime     --> set to NOW
            // Date    updateTime     --> set to NOW
            // Long    version        --> set to 1     - this is the initial create
            // String  name;          --> set from OM name
            // String  description;   --> set from OM description
            // String  typeVersion;   --> set to "1"
            // Map<String, String> options;  --> set to null
            // List<AtlasEnumElementDef> elementDefs;  --> set from OM elementDefs
            // String                    defaultValue;  --> set from OM defaultValue
            //
            // The OM EnumDef provides
            // AttributeTypeDefCategory category
            // String                   guid
            // String                   name
            // String                   description
            // String                   descriptionGUID --> IGNORED
            // ArrayList<EnumElementDef> elementDefs
            // EnumElementDef            defaultValue
            //

            AtlasEnumDef atlasEnumDef = new AtlasEnumDef();
            atlasEnumDef.setGuid(omEnumDef.getGUID());
            atlasEnumDef.setCreatedBy(userId);
            atlasEnumDef.setUpdatedBy(userId);
            Date now = new Date();
            atlasEnumDef.setCreateTime(now);
            atlasEnumDef.setUpdateTime(now);
            atlasEnumDef.setVersion(1L);
            atlasEnumDef.setName(omEnumDef.getName());
            atlasEnumDef.setDescription(omEnumDef.getDescription());
            atlasEnumDef.setTypeVersion("1");
            atlasEnumDef.setOptions(null);
            // EnumElements
            List<AtlasEnumDef.AtlasEnumElementDef> atlasElemDefs = null;
            List<EnumElementDef> omEnumElems = omEnumDef.getElementDefs();
            if (omEnumElems != null && !(omEnumElems.isEmpty())) {
                atlasElemDefs = new ArrayList<>();
                for (EnumElementDef omElemDef : omEnumDef.getElementDefs()) {
                    AtlasEnumDef.AtlasEnumElementDef atlasElemDef = new AtlasEnumDef.AtlasEnumElementDef();
                    atlasElemDef.setValue(omElemDef.getValue());
                    atlasElemDef.setDescription(omElemDef.getDescription());
                    atlasElemDef.setOrdinal(omElemDef.getOrdinal());
                    atlasElemDefs.add(atlasElemDef);
                }
            }
            atlasEnumDef.setElementDefs(atlasElemDefs);
            // Default value
            EnumElementDef omDefaultValue = omEnumDef.getDefaultValue();
            LOG.debug("addAttributeTypeDef: omDefaultValue {}",omDefaultValue);
            if (omDefaultValue != null) {
                atlasEnumDef.setDefaultValue(omDefaultValue.getValue());
                LOG.debug("addAttributeTypeDef: Atlas default value set to {}",atlasEnumDef.getDefaultValue());
            }

            LOG.debug("addAttributeTypeDef: create AtlasEnumDef {}",atlasEnumDef);

            // Add the AtlasEnumDef to the AtlasTypesDef so it can be added to the TypeDefStore...
            atlasTypesDef.getEnumDefs().add(atlasEnumDef);

            // Add the AtlasTypesDef to the typeDefStore.
            // To do this we need to put the EntityDef into an AtlasTypesDef
            LOG.debug("addAttributeTypeDef: create Atlas types {}",atlasTypesDef);
            try {
                typeDefStore.createTypesDef(atlasTypesDef);

            } catch (AtlasBaseException e) {

                LOG.error("addAttributeTypeDef: caught exception from Atlas, error code {}",e);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_TYPEDEF_CREATE_FAILED;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasEnumDef.toString(), methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());}
        }
        else {
            LOG.debug("addAttributeTypeDef: category is {}, so no action needed", category);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addAttributeTypeDef(userId={}, newAttributeTypeDef={})", userId, newAttributeTypeDef);
        }

    }

    /**
     * Verify whether a definition of a TypeDef is known. The method tests whether a type with the
     * specified name is known and whether that type matches the presented type, in which case the
     * it is verified and returns true.
     * If the type (name) is not known the method returns false.
     * If the type name is one of the Famous Five then convert the name to extended form. If the
     * extended type does not exist return false, to promote an addTypeDef(). During the add
     * the name is again converted so that the extended type is added.
     * If the type is not valid or conflicts with an existing type (of the same name) then the
     * method throws the relevant exception.
     *
     * @param userId  - unique identifier for requesting user.
     * @param typeDef - TypeDef structure describing the TypeDef to test.
     * @return boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known.
     * @throws InvalidParameterException    - the TypeDef is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefConflictException     - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException      - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public boolean verifyTypeDef(String   userId,
                                 TypeDef  typeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> verifyTypeDef(userId={}, typeDef={})", userId, typeDef);
        }

        final String  methodName           = "verifyTypeDef";
        final String  typeDefParameterName = "typeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDef(repositoryName, typeDefParameterName, typeDef, methodName);

        /*
         * Perform operation
         */

        String typeName = typeDef.getName();

        /*
         * Validate the status fields in the passed TypeDef
         */
        boolean statusFieldsValid = validateStatusFields(typeDef);
        if (!statusFieldsValid) {
            // We cannot accept this typedef because it contains status fields that cannot be modelled by Atlas
            LOG.error("verifyTypeDef: The TypeDef with name {}, initialStatus {} and validInstanceStatusList {} could not be modelled in Atlas", typeName, typeDef.getInitialStatus(), typeDef.getValidInstanceStatusList());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
            throw new TypeDefNotSupportedException(
                    errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Clear the per-API records of TypeDefs
        typeDefsForAPI = new TypeDefsByCategory();


        boolean ret = false;
        boolean conflict = false;
        TypeDef existingTypeDef;

        // Verify whether the supplied TypeDef is known in the cache.

        TypeDefCategory tdCat = typeDef.getCategory();
        switch (tdCat) {

            case ENTITY_DEF:
                /*
                 * If the type name matches one of the Famous Five then convert the name to extended form.
                 * It is the extended type that we must look for in the repo. If the extended type does not
                 * exist then return false. This will provoke an addTypeDef (of the same type) which will
                 * allow us to create an extended type in Atlas.
                 * If the extended type does exist then convert it back prior to the compare.
                 *
                 */

                String typeNameToLookFor = typeName;
                if (FamousFive.omTypeRequiresSubstitution(typeName)) {
                    LOG.debug("verifyTypeDef: type name {} requires substitution - returning false", typeName);
                    typeNameToLookFor = FamousFive.getAtlasTypeName(typeName, typeDef.getGUID());
                }

                // Ask Atlas...
                try {
                    existingTypeDef = _getTypeDefByName(userId, typeNameToLookFor);
                }
                catch (TypeDefNotKnownException e) {
                    // Handle below
                    existingTypeDef = null;
                }
                if (existingTypeDef == null) {
                    // Log using both the original typeName and extended name where different
                    if (typeNameToLookFor.equals(typeName))
                        LOG.debug("verifyTypeDef: no existing TypeDef with name {}", typeName);
                    else
                        LOG.debug("verifyTypeDef: requested TypeDef name {} mapped to name {} which is not in the repo, ", typeName, typeNameToLookFor);
                    ret = false;
                }
                else if (existingTypeDef.getCategory() == ENTITY_DEF) {
                    EntityDef existingEntityDef = (EntityDef) existingTypeDef;
                    LOG.debug("verifyTypeDef: existing EntityDef: {}", existingEntityDef);

                    // The definition is not new - compare the existing def with the one passed
                    // A compare will be too strict - instead use an equivalence check...
                    Comparator comp = new Comparator();
                    EntityDef passedEntityDef = (EntityDef) typeDef;
                    LOG.debug("verifyTypeDef: new typedef: {}", passedEntityDef);
                    boolean match = comp.equivalent(existingEntityDef, passedEntityDef);
                    LOG.debug("verifyTypeDef: equivalence result {}", match);
                    if (match) {
                        // The definition is known and matches - return true
                        ret = true;
                    }
                    else {
                        conflict = true;
                    }
                }
                else {
                    conflict = true;
                }
                break;

            case RELATIONSHIP_DEF:
                // Ask Atlas...
                try {
                    existingTypeDef = _getTypeDefByName(userId, typeName);
                }
                catch (TypeDefNotKnownException e) {
                    // Handle below
                    existingTypeDef = null;
                }
                if (existingTypeDef == null) {
                    LOG.debug("verifyTypeDef: no existing TypeDef with name {}", typeName);
                    ret = false;
                }
                else if (existingTypeDef.getCategory() == RELATIONSHIP_DEF) {
                    RelationshipDef existingRelationshipDef = (RelationshipDef) existingTypeDef;
                    LOG.debug("verifyTypeDef: existing RelationshipDef: {}", existingRelationshipDef);

                    // The definition is not new - compare the existing def with the one passed
                    // A compare will be too strict - instead use an equivalence check...
                    Comparator comp = new Comparator();
                    RelationshipDef passedRelationshipDef = (RelationshipDef) typeDef;
                    LOG.debug("verifyTypeDef: new typedef: {}", passedRelationshipDef);
                    boolean match = comp.equivalent(existingRelationshipDef, passedRelationshipDef);
                    LOG.debug("verifyTypeDef: equivalence result {}", match);
                    if (match) {
                        // The definition is known and matches - return true
                        ret = true;
                    }
                    else {
                        conflict = true;
                    }
                }
                else {
                    conflict = true;
                }
                break;

            case CLASSIFICATION_DEF:
                // Ask Atlas...
                try {
                    existingTypeDef = _getTypeDefByName(userId, typeName);
                }
                catch (TypeDefNotKnownException e) {
                    // Handle below
                    existingTypeDef = null;
                }
                if (existingTypeDef == null) {
                    LOG.debug("verifyTypeDef: no existing TypeDef with name {}", typeName);
                    ret = false;
                }
                else if (existingTypeDef.getCategory() == CLASSIFICATION_DEF) {
                    ClassificationDef existingClassificationDef = (ClassificationDef) existingTypeDef;
                    LOG.debug("verifyTypeDef: existing ClassificationDef: {}", existingClassificationDef);

                    // The definition is not new - compare the existing def with the one passed
                    // A compare will be too strict - instead use an equivalence check...
                    Comparator comp = new Comparator();
                    ClassificationDef passedClassificationDef = (ClassificationDef) typeDef;
                    LOG.debug("verifyTypeDef: new typedef: {}", passedClassificationDef);
                    boolean match = comp.equivalent(existingClassificationDef, passedClassificationDef);
                    LOG.debug("verifyTypeDef: equivalence result {}", match);
                    if (match) {
                        // The definition is known and matches - return true
                        ret = true;
                    }
                    else {
                        conflict = true;
                    }
                }
                else {
                    conflict = true;
                }
                break;

            default:
                // The typedef category is not supported - raise an exception
                LOG.error("verifyTypeDef: The supplied TypeDef with name {} has an unsupported category {}",typeName, tdCat);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_TYPEDEF_CATEGORY;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(tdCat.toString(), typeDef.toString(), methodName, repositoryName);

                throw new TypeDefNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }


        if (conflict) {
            // The definition is known but conflicts - raise an exception
            // The typeDef was known but conflicted in some way with the existing type - raise an exception
            LOG.error("verifyTypeDef: The TypeDef with name {} conflicts with an existing type {}", typeName, existingTypeDef);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, typeDef.getGUID(), typeDefParameterName, methodName, repositoryName, typeDef.toString());
            throw new TypeDefConflictException(
                    errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== verifyTypeDef(userId={}, typeDef={}): ret={}", userId, typeDef, ret);
        }
        return ret;
    }

    /**
     * Verify whether a definition of an AttributeTypeDef is known. The method tests whether a type with the
     * specified name is known and whether that type matches the presented type, in which case the
     * it is verified and returns true.
     * If the type (name) is not known the method returns false.
     * If the type name is one of the Famous Five then return false, to provoke an addTypeDef().
     * If the type is not valid or conflicts with an existing type (of the same name) then the
     * method throws the relevant exception.
     *
     * @param userId           - unique identifier for requesting user.
     * @param attributeTypeDef - TypeDef structure describing the TypeDef to test.
     * @return boolean - true means the TypeDef matches the local definition - false means the TypeDef is not known.
     * @throws InvalidParameterException    - the TypeDef is null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws TypeDefNotSupportedException - the repository is not able to support this TypeDef.
     * @throws TypeDefConflictException     - the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException      - the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public boolean verifyAttributeTypeDef(String           userId,
                                          AttributeTypeDef attributeTypeDef)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> verifyAttributeTypeDef(userId={}, typeDef={})", userId, attributeTypeDef);
        }

        final String  methodName           = "verifyAttributeTypeDef";
        final String  typeDefParameterName = "attributeTypeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDef(repositoryName, typeDefParameterName, attributeTypeDef, methodName);

        /*
         * Perform operation
         */

        // Clear the per-API records of TypeDefs
        typeDefsForAPI = new TypeDefsByCategory();

        /*
         * If a PRIMITIVE or COLLECTION (MAP or ARRAY) then return false to trigger an immediate
         * addAttributeTypeDef(). We will then add the ATD with the specified GUID.
         * The benefit of this is that the LocalConnector then populates the RCM with the OM types
         * instead of the AtlasConnector fabricating them.
         * If we are dealing with an ENUM then we need to do more work. Note that in Atlas an
         * ENUM is a Type whereas in OM it is an Attribute Type.
         */
        boolean ret = false;
        String atdName = attributeTypeDef.getName();
        AttributeTypeDefCategory atdCat = attributeTypeDef.getCategory();
        switch (atdCat) {

            case PRIMITIVE:
                LOG.debug("verifyAttributeTypeDef: supplied AttributeTypeDef has category PRIMITIVE - returning false");
                ret = false;
                break;

            case COLLECTION:
                LOG.debug("verifyAttributeTypeDef: supplied AttributeTypeDef has category COLLECTION - returning false");
                ret = false;
                break;

            case ENUM_DEF:
                boolean conflict = false;
                LOG.debug("verifyAttributeTypeDef: supplied AttributeTypeDef has category ENUM_DEF - checking for existence");
                // Look in Atlas to see whether we have an ENUM of this name and if so
                // perform a comparison.
                AttributeTypeDef existingATD;
                try {
                    existingATD = _getAttributeTypeDefByName(userId, atdName);
                }
                catch (TypeDefNotKnownException e) {
                    // handle below
                    existingATD = null;
                }
                if (existingATD == null) {
                    LOG.debug("verifyAttributeTypeDef: no existing enum def with the name {}", atdName);
                    ret = false;
                }
                else if (existingATD.getCategory() == ENUM_DEF) {
                    EnumDef existingEnumDef = (EnumDef) existingATD;
                    LOG.debug("verifyAttributeTypeDef: existing enum def: {}", existingEnumDef);

                    // The definition is not new - compare the existing def with the one passed
                    // A compare will be too strict - instead use an equivalence check...
                    Comparator comp = new Comparator();
                    EnumDef passedEnumDef = (EnumDef) attributeTypeDef;
                    LOG.debug("verifyAttributeTypeDef: new enum def: {}", passedEnumDef);
                    boolean match = comp.equivalent(existingEnumDef, passedEnumDef);
                    LOG.debug("verifyAttributeTypeDef: equivalence result: {}", match);
                    if (match) {
                        // The definition is known and matches - return true
                        ret = true;
                    }
                    else {
                        conflict = true;
                    }
                }
                else {
                    conflict = true;
                }
                if (conflict) {
                    // The typeDef was known but conflicted in some way with the existing type - raise an exception
                    LOG.error("verifyAttributeTypeDef: The supplied AttributeTypeDef conflicts with an existing type with name {}", atdName);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("typeDef", methodName, repositoryName);

                    throw new TypeDefConflictException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                break;

            case UNKNOWN_DEF:
                LOG.error("verifyAttributeTypeDef: The supplied AttributeTypeDef {} has unsupported category {}", atdName, atdCat);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ATTRIBUTE_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("typeDef", methodName, repositoryName);

                throw new TypeDefConflictException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== verifyAttributeTypeDef(userId={}, typeDef={}): ret={}", userId, attributeTypeDef, ret);
        }
        return ret;

    }

    /**
     * Update one or more properties of the TypeDef.  The TypeDefPatch controls what types of updates
     * are safe to make to the TypeDef.
     *
     * @param userId       - unique identifier for requesting user.
     * @param typeDefPatch - TypeDef patch describing change to TypeDef.
     * @return updated TypeDef
     * @throws InvalidParameterException  - the TypeDefPatch is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested TypeDef is not found in the metadata collection.
     * @throws PatchErrorException        - the TypeDef can not be updated because the supplied patch is incompatible
     *                                    with the stored TypeDef.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef updateTypeDef(String       userId,
                                 TypeDefPatch typeDefPatch)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            PatchErrorException,
            UserNotAuthorizedException
    {

        final String  methodName           = "updateTypeDef";
        final String  typeDefParameterName = "typeDefPatch";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefPatch(repositoryName, typeDefPatch, methodName);

        /*
         * Perform operation
         */

        /*
         * A TypeDefPatch describes a change (patch) to a typeDef's properties, options, external
         * standards mappings or list of valid instance statuses.
         * A patch can be applied to an EntityDef, RelationshipDef or ClassificationDef.
         * Changes to a TypeDef's category or superclasses requires a new type definition.
         * In addition it is not possible to delete an attribute through a patch.
         *
         * The TypeDefPatch contains:
         *
         *   TypeDefPatchAction                 action
         *   String                             typeDefGUID
         *   String                             typeName
         *   long                               applyToVersion
         *   long                               updateToVersion
         *   String                             newVersionName
         *   String                             description
         *   String                             descriptionGUID
         *   ArrayList<TypeDefAttribute>        typeDefAttributes
         *   Map<String, String>                typeDefOptions
         *   ArrayList<ExternalStandardMapping> externalStandardMappings
         *   ArrayList<InstanceStatus>          validInstanceStatusList    - note that no action is defined for this
         *
         * The validateTypeDefPatch above merely checks the patch is not null. Within the patch itself
         * some fields are always mandatory and the presence of others depend on the action being performed.
         * Mandatory fields: action, typeDefGUID, typeName, applyToVersion, updateToVersion, newVersionName
         * Remaining fields are optional, subject to action (e.g. if add_options then options are needed)
         *
         * The TypeDefPatchAction can be one of:
         *  ADD_ATTRIBUTES                    ==> typeDefAttributes must be supplied
         *  ADD_OPTIONS                       ==> typeDefOptions must be supplied
         *  UPDATE_OPTIONS                    ==> typeDefOptions must be supplied
         *  DELETE_OPTIONS                    ==> typeDefOptions must be supplied
         *  ADD_EXTERNAL_STANDARDS            ==> externalStandardMappings must be supplied
         *  UPDATE_EXTERNAL_STANDARDS         ==> externalStandardMappings must be supplied
         *  DELETE_EXTERNAL_STANDARDS         ==> externalStandardMappings must be supplied
         *  UPDATE_DESCRIPTIONS               ==> description must be supplied; descriptionGUID is optional
         */

        /*
         * Versions
         *
         * The applyToVersion field in the TypeDefPatch is optional.
         * The Atlas Connector can only retrieve the TypeDef by GUID or name, not by version.
         * The returned TypeDef is assumed to be the current/active version and if applyToVersion
         * is specified (!= 0L) the connector will check that it matches the retrieved TypeDef
         * and throw an exception if it does not match.
         * If applyToVersion is not specified (== 0L) then the patch will be applied to the
         * current (retrieved) TypeDef.
         *
         * The updateToVersion and newVersionName fields in the TypeDefPatch are optional.
         * If updateToVersion and/or newVersionName are not supplied the connector can generate
         * values for them (in the updated TypeDef) because the updated TypeDef is returned to
         * the caller (i.e. LocalOMRSMetadataCollection) which then updates the RepositoryContentManager
         * (RCM). The new version information generated by the connector will therefore be reflected in
         * the RCM.
         */

        /*
         * Identification
         * This method can tolerate either typeDefGUID OR typeName being absent, but not both.
         */
        String typeDefGUID   = typeDefPatch.getTypeDefGUID();
        String typeName      = typeDefPatch.getTypeName();
        if ( typeDefGUID == null && typeName == null )  {

            LOG.error("updateTypeDef: At least one of typeDefGUID and typeName must be supplied");

            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeDefParameterName, methodName, repositoryName);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        /*
         * Find the TypeDef - as described above this will only get the 'current' version.
         * Check the retrieved version matches the applyToVersion, if specified.
         * Perform the requested action.
         */
        AtlasBaseTypeDef atlasPreTypeDef;
        try {
            // Find the TypeDef
            if (typeDefGUID != null) {
                if (!useRegistry) {
                    // Look in the Atlas type def store
                    atlasPreTypeDef = typeDefStore.getByGuid(typeDefGUID);
                }
                else {
                    // Using registry
                    atlasPreTypeDef = typeRegistry.getTypeDefByGuid(typeDefGUID);
                }
            }
            else { // we know that typeName != null

                if (!useRegistry) {
                    // Look in the Atlas type def store
                    atlasPreTypeDef = typeDefStore.getByName(typeName);
                }
                else {
                    // Using registry
                    atlasPreTypeDef = typeRegistry.getTypeDefByName(typeName);
                }
            }

        } catch (AtlasBaseException e) {

            if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_NAME_NOT_FOUND) {
                LOG.debug("_getAttributeTypeDefByName: Atlas does not have the type with name {} ", typeName);
                // The TypeDef was not found - ensure atlasPreTypeDef is null, exception to be handled below
                atlasPreTypeDef = null;

            } else {

                LOG.error("_getAttributeTypeDefByName: Caught exception trying to retrieve Atlas type with name {} ", typeName, e);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getAttributeTypeDefByName",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        // Separate null check ensures we have covered both cases (registry and store)
        if (atlasPreTypeDef == null) {

            LOG.error("_getAttributeTypeDefByName: received null return from Atlas getByName using name {}", typeName);
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, "unknown", "name", "_getAttributeTypeDefByName", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getAttributeTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // From here on we know that atlasPreTypeDef is not null

        // Check the version
        Long applyToVersion = typeDefPatch.getApplyToVersion();
        Long preTypeDefVersion = atlasPreTypeDef.getVersion();
        if (applyToVersion != 0L && !applyToVersion.equals(preTypeDefVersion) ) {

            LOG.error("updateTypeDef: Retrieved TypeDef version {} does not match version {} specified in patch", preTypeDefVersion, applyToVersion);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeName, typeDefGUID, typeDefParameterName, methodName, repositoryName, typeDefPatch.toString());

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Convert the Atlas TypeDef to an OM TypeDef
        // We are only concerned with EntityDef, RelationshipDef and ClassificationDef TypeDefs
        TypeDef omPreTypeDef = null;
        boolean fatalError = false;
        TypeCategory atlasCategory = atlasPreTypeDef.getCategory();
        switch (atlasCategory) {

            case ENTITY:
                AtlasEntityDef retrievedAtlasEntityDef = (AtlasEntityDef) atlasPreTypeDef;
                AtlasEntityDefMapper atlasEntityDefMapper;
                EntityDef returnableEntityDef;
                try {
                    atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, retrievedAtlasEntityDef);
                    returnableEntityDef = atlasEntityDefMapper.toOMEntityDef();
                    omPreTypeDef = returnableEntityDef;
                } catch (Exception e) {
                    fatalError = true;
                }
                break;

            case RELATIONSHIP:
                AtlasRelationshipDef retrievedAtlasRelationshipDef = (AtlasRelationshipDef) atlasPreTypeDef;
                AtlasRelationshipDefMapper atlasRelationshipDefMapper;
                RelationshipDef returnableRelationshipDef;
                try {
                    atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, retrievedAtlasRelationshipDef);
                    returnableRelationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
                    omPreTypeDef = returnableRelationshipDef;
                } catch (Exception e) {
                    fatalError = true;
                }
                break;

            case CLASSIFICATION:
                AtlasClassificationDef retrievedAtlasClassificationDef = (AtlasClassificationDef) atlasPreTypeDef;
                AtlasClassificationDefMapper atlasClassificationDefMapper;
                ClassificationDef returnableClassificationDef;
                try {
                    atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, retrievedAtlasClassificationDef);
                    returnableClassificationDef = atlasClassificationDefMapper.toOMClassificationDef();
                    omPreTypeDef = returnableClassificationDef;
                } catch (Exception e) {
                    fatalError = true;
                }
                break;

            default:
                LOG.debug("updateTypeDef: cannot convert an OM TypeDef with category {}", atlasCategory);
                fatalError = true;
        }

        if (fatalError || omPreTypeDef == null) {

            LOG.error("updateTypeDef: could not initialise mapper or convert retrieved AtlasBaseTypeDef {} to OM TypeDef", atlasPreTypeDef.getName());
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("TypeDef", "updateTypeDef", metadataCollectionId);
            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "updateTypeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Now have established an omPreTypeDef to which action can be performed...
        LOG.debug("updateTypeDef: AtlasEntityDef mapped to OM TypeDef {}", omPreTypeDef);


        // Perform the requested action
        TypeDef omPostTypeDef;
        switch (typeDefPatch.getAction()) {
            case ADD_OPTIONS:
                omPostTypeDef = updateTypeDefAddOptions(omPreTypeDef, typeDefPatch.getTypeDefOptions());
                break;
            case DELETE_OPTIONS:
                omPostTypeDef = updateTypeDefDeleteOptions(omPreTypeDef, typeDefPatch.getTypeDefOptions());
                break;
            case UPDATE_OPTIONS:
                omPostTypeDef = updateTypeDefUpdateOptions(omPreTypeDef, typeDefPatch.getTypeDefOptions());
                break;
            case ADD_ATTRIBUTES:
                omPostTypeDef = updateTypeDefAddAttributes(omPreTypeDef, typeDefPatch.getTypeDefAttributes());
                break;
            case UPDATE_DESCRIPTIONS:
                omPostTypeDef = updateTypeDefUpdateDescriptions(omPreTypeDef, typeDefPatch.getDescription(), typeDefPatch.getDescriptionGUID());
                break;
            case ADD_EXTERNAL_STANDARDS:
                omPostTypeDef = updateTypeDefAddExternalStandards(omPreTypeDef, typeDefPatch.getExternalStandardMappings());
                break;
            case DELETE_EXTERNAL_STANDARDS:
                omPostTypeDef = updateTypeDefDeleteExternalStandards(omPreTypeDef, typeDefPatch.getExternalStandardMappings());
                break;
            case UPDATE_EXTERNAL_STANDARDS:
                omPostTypeDef = updateTypeDefUpdateExternalStandards(omPreTypeDef, typeDefPatch.getExternalStandardMappings());
                break;
            // There maybe should be instance status actions too, but these are not yet defined (see TypeDefPatchAction)
            default:
                LOG.error("updateTypeDef: TypeDefPatch action {} is not recognised", typeDefPatch.getAction());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, typeDefGUID, typeDefParameterName, methodName, repositoryName, typeDefPatch.toString());

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }

        LOG.error("updateTypeDef: omPostTypeDef {} ", omPostTypeDef);

        /*
         * Convert the updated OM TypeDef to Atlas TypeDef and update in Atlas, read it back and convert to OM.
         * The write-through and read-back is so that we pick up any Atlas initializations, e.g. of
         * fields like createdBy, description (if originally null), etc.
         */


        /*
         * Depending on OM TypeDef category, perform the corresponding Atlas update, which will be for one of
         * AtlasEntityDef, AtlasRelationshipDef or AtlasClassificationDef
         */

        AtlasBaseTypeDef retrievedAtlasTypeDef = null;
        try {

            TypeDefCategory category = omPostTypeDef.getCategory();
            LOG.debug("updateTypeDef: convert and store updated OM TypeDef with guid {} and category {}", typeDefGUID, category);
            switch (category) {

                case ENTITY_DEF:
                    // convertOMEntityDefToAtlasEntityDef will detect a Famous Five type and convert accordingly.
                    AtlasEntityDef atlasEntityDef = convertOMEntityDefToAtlasEntityDef((EntityDef) omPostTypeDef);
                    if (atlasEntityDef != null) {
                        LOG.debug("updateTypeDef: Call Atlas updateEntityDefByGuid with guid {} entity {}", typeDefGUID, atlasEntityDef);
                        retrievedAtlasTypeDef = typeDefStore.updateEntityDefByGuid(typeDefGUID, atlasEntityDef);
                        if (retrievedAtlasTypeDef == null)
                            throw new AtlasBaseException();
                    }
                    break;

                case RELATIONSHIP_DEF:
                    try {
                        AtlasRelationshipDef atlasRelationshipDef = convertOMRelationshipDefToAtlasRelationshipDef((RelationshipDef) omPostTypeDef);
                        if (atlasRelationshipDef != null) {
                            LOG.debug("updateTypeDef: Call Atlas updateEntityDefByGuid with guid {} entity {}", typeDefGUID, atlasRelationshipDef);
                            retrievedAtlasTypeDef = typeDefStore.updateRelationshipDefByGuid(typeDefGUID, atlasRelationshipDef);
                            if (retrievedAtlasTypeDef == null)
                                throw new AtlasBaseException();
                        }
                    }
                    catch (RepositoryErrorException | TypeErrorException e) {
                        // Log the error and re-throw
                        LOG.debug("updateTypeDef: caught exception from attempt to convert OM RelationshipDef to Atlas, name {}", omPostTypeDef.getName());
                        OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("TypeDef", "updateTypeDef", metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                "updateTypeDef",
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    break;

                case CLASSIFICATION_DEF:
                    AtlasClassificationDef atlasClassificationDef = convertOMClassificationDefToAtlasClassificationDef((ClassificationDef) omPostTypeDef);
                    if (atlasClassificationDef != null) {
                        LOG.debug("updateTypeDef: Call Atlas updateClassificationDefByGuid with guid {} entity {}", typeDefGUID, atlasClassificationDef);
                        retrievedAtlasTypeDef = typeDefStore.updateClassificationDefByGuid(typeDefGUID, atlasClassificationDef);
                        if (retrievedAtlasTypeDef == null)
                            throw new AtlasBaseException();
                    }
                    break;

                case UNKNOWN_DEF:
                    LOG.error("updateTypeDef: cannot convert and update an OM TypeDef with category {}", category);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(typeName, typeDefGUID, typeDefParameterName, methodName, repositoryName, typeDefPatch.toString());

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "updateTypeDef",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
            }
        }
        catch (AtlasBaseException e) {

            LOG.error("updateTypeDef: exception from store and retrieve AtlasTypesDef {}", e);

            // I'm not sure how to differentiate between the different causes of AtlasBaseException
            // e.g. if there were an authorization exception it is not clear how to tell that
            // in order to throw the correct type of exception. I think that for now if we get an
            // AtlasBaseException we will always throw RepositoryErrorException
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("TypeDef", "updateTypeDef", metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "updateTypeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        /*
         * Convert the retrieved Atlas Type into an OM TypeDef to be returned
         */
        TypeDef returnableTypeDef = null;
        if (retrievedAtlasTypeDef != null) {
            fatalError = false;
            TypeCategory category = retrievedAtlasTypeDef.getCategory();
            switch (category) {

                case ENTITY:
                    AtlasEntityDef retrievedAtlasEntityDef = (AtlasEntityDef) retrievedAtlasTypeDef;
                    EntityDef returnableEntityDef;
                    try {
                        AtlasEntityDefMapper atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, retrievedAtlasEntityDef);
                        returnableEntityDef = atlasEntityDefMapper.toOMEntityDef();
                        returnableTypeDef = returnableEntityDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                case RELATIONSHIP:
                    AtlasRelationshipDef retrievedAtlasRelationshipDef = (AtlasRelationshipDef) retrievedAtlasTypeDef;
                    RelationshipDef returnableRelationshipDef;
                    try {
                        AtlasRelationshipDefMapper atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, retrievedAtlasRelationshipDef);
                        returnableRelationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
                        returnableTypeDef = returnableRelationshipDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                case CLASSIFICATION:
                    AtlasClassificationDef retrievedAtlasClassificationDef = (AtlasClassificationDef) retrievedAtlasTypeDef;
                    ClassificationDef returnableClassificationDef;
                    try {
                        AtlasClassificationDefMapper atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, retrievedAtlasClassificationDef);
                        returnableClassificationDef = atlasClassificationDefMapper.toOMClassificationDef();
                        returnableTypeDef = returnableClassificationDef;
                    } catch (Exception e) {
                        fatalError = true;
                    }
                    break;

                default:
                    LOG.debug("updateTypeDef: cannot convert an OM TypeDef with category {}", category);
                    fatalError = true;
            }
        }

        if (fatalError || returnableTypeDef == null) {

            LOG.error("updateTypeDef: could not initialise mapper or convert retrieved AtlasBaseTypeDef {} to OM TypeDef", typeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("TypeDef", "updateTypeDef", metadataCollectionId);
            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "updateTypeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Finally return the retrieved, updated TypeDef
        LOG.debug("updateTypeDef: Updated OM TypeDef {}", returnableTypeDef);
        return returnableTypeDef;

    }

    /*
     * Utility methods to update TypeDefs
     */
    private TypeDef updateTypeDefAddOptions(TypeDef typeDefToModify, Map<String, String> typeDefOptions) {
        Map<String,String> updatedOptions = typeDefToModify.getOptions();
        if (typeDefOptions != null) {
            updatedOptions.putAll(typeDefOptions);
        }
        typeDefToModify.setOptions(updatedOptions);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefDeleteOptions(TypeDef typeDefToModify, Map<String, String> typeDefOptions) {
        Map<String,String> updatedOptions = typeDefToModify.getOptions();
        if (typeDefOptions != null) {
            for (String s : typeDefOptions.keySet()) {
                updatedOptions.remove(s);
            }
        }
        typeDefToModify.setOptions(updatedOptions);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefUpdateOptions(TypeDef typeDefToModify, Map<String, String> typeDefOptions) {
        typeDefToModify.setOptions(typeDefOptions);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefAddAttributes(TypeDef typeDefToModify, List<TypeDefAttribute> typeDefAttributes)
        throws
            PatchErrorException
    {
        final String methodName = "updateTypeDefAddAttributes";

        List<TypeDefAttribute> updatedAttributes = typeDefToModify.getPropertiesDefinition();
        // This operation must be additive only - check each attribute to be added does not already exist
        if (typeDefAttributes != null) {
            if (updatedAttributes == null) {
                updatedAttributes = typeDefAttributes;
            } else {
                for (TypeDefAttribute tda : typeDefAttributes) {
                    String newAttrName = tda.getAttributeName();
                    boolean nameClash = false;
                    for (TypeDefAttribute existingAttr : updatedAttributes ) {
                        if (existingAttr.getAttributeName().equals(newAttrName)) {
                            // There is a name clash - error
                            nameClash = true;
                            break;
                        }
                    }
                    if (!nameClash) {
                        // add the new attribute
                        updatedAttributes.add(tda);
                    }
                    else {
                        // exception
                        LOG.error("updateTypeDef: Cannot add attribute {} because it already exists", newAttrName);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(typeDefToModify.getName(), typeDefToModify.getGUID(), "typeDefToModify", methodName, repositoryName, typeDefToModify.toString());

                        throw new PatchErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                "updateTypeDefAddAttributes",
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                    }
                }
            }
        }
        typeDefToModify.setPropertiesDefinition(updatedAttributes);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefUpdateDescriptions(TypeDef typeDefToModify, String description, String descriptionGUID) {
        typeDefToModify.setDescription(description);
        typeDefToModify.setDescriptionGUID(descriptionGUID);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefAddExternalStandards(TypeDef typeDefToModify, List<ExternalStandardMapping> externalStandardMappings) {
        List<ExternalStandardMapping> updatedExternalStandardMappings = typeDefToModify.getExternalStandardMappings();
        if (externalStandardMappings != null) {
            updatedExternalStandardMappings.addAll(externalStandardMappings);
        }
        typeDefToModify.setExternalStandardMappings(updatedExternalStandardMappings);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefDeleteExternalStandards(TypeDef typeDefToModify, List<ExternalStandardMapping> externalStandardMappings)
        throws
            PatchErrorException
    {
        final String methodName = "updateTypeDefDeleteExternalStandards";

        List<ExternalStandardMapping> updatedExternalStandardMappings = typeDefToModify.getExternalStandardMappings();
        if (externalStandardMappings != null) {
            if (updatedExternalStandardMappings == null) {
                // there are no existing mappings - exception
                LOG.error("updateTypeDef: Cannot delete external standard mappings because none exist");
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeDefToModify.getName(), typeDefToModify.getGUID(), "typeDefToModify", methodName, repositoryName, typeDefToModify.toString());


                throw new PatchErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "updateTypeDefAddAttributes",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
            else {
                /*
                 * There are existing mappings - find each mapping to be deleted and remove it from the list
                 * Our definition of equality is that all 3 components of the ExternalStandardMapping must
                 * match - the optional fields must be present and match or not be present in both objects,
                 * so we just test equality including equality of null == null. Meanwhile, the mandatory field
                 * must match. The test is therefore actually quite simple.
                 */

                // This loop is tolerant i.e it does not raise an error - if it does not find the mapping to delete
                for (ExternalStandardMapping esm : externalStandardMappings) {
                    // Find the corresponding member of the existing list
                    for (ExternalStandardMapping existingESM : updatedExternalStandardMappings) {
                        if (    existingESM.getStandardTypeName().equals(esm.getStandardTypeName()) &&
                                existingESM.getStandardOrganization().equals(esm.getStandardOrganization()) &&
                                existingESM.getStandardName().equals(esm.getStandardName()) ) {
                            // matching entry found - remove the entry from the list
                            updatedExternalStandardMappings.remove(existingESM);
                            // no break - if there are multiple matching entries delete them all
                        }
                    }
                }
            }
        }
        typeDefToModify.setExternalStandardMappings(updatedExternalStandardMappings);
        return typeDefToModify;
    }

    private TypeDef updateTypeDefUpdateExternalStandards(TypeDef typeDefToModify, List<ExternalStandardMapping> externalStandardMappings) {
        typeDefToModify.setExternalStandardMappings(externalStandardMappings);
        return typeDefToModify;
    }




    /**
     * Delete the TypeDef.  This is only possible if the TypeDef has never been used to create instances or any
     * instances of this TypeDef have been purged from the metadata collection.
     *
     * @param userId              - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the TypeDef.
     * @param obsoleteTypeDefName - String unique name for the TypeDef.
     * @throws InvalidParameterException  - the one of TypeDef identifiers is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested TypeDef is not found in the metadata collection.
     * @throws TypeDefInUseException      - the TypeDef can not be deleted because there are instances of this type in the
     *                                    the metadata collection.  These instances need to be purged before the
     *                                    TypeDef can be deleted.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void deleteTypeDef(String userId,
                              String obsoleteTypeDefGUID,
                              String obsoleteTypeDefName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            TypeDefInUseException,
            UserNotAuthorizedException
    {
        final String    methodName        = "deleteTypeDef";
        final String    guidParameterName = "obsoleteTypeDefGUID";
        final String    nameParameterName = "obsoleteTypeDefName";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                obsoleteTypeDefGUID,
                obsoleteTypeDefName,
                methodName);

        /*
         * Perform operation
         */

        TypeDef omTypeDefToDelete = null;
        try {
            /*
             * Find the obsolete type def so that we know its category
             *
             */
            omTypeDefToDelete = _getTypeDefByGUID(userId, obsoleteTypeDefGUID);

            if (omTypeDefToDelete == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guidParameterName,
                        methodName,
                        repositoryName);

                throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            TypeDefCategory typeDefCategory = omTypeDefToDelete.getCategory();


        /*
         * Construct an AtlasTypesDef containing an Atlas converted copy of the OM TypeDef to be deleted then call the TypeDefStore
         *
         */
            AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
            switch (typeDefCategory) {
                case CLASSIFICATION_DEF:
                    ClassificationDef classificationDefToDelete = (ClassificationDef) omTypeDefToDelete;
                    AtlasClassificationDef atlasClassificationDef = convertOMClassificationDefToAtlasClassificationDef(classificationDefToDelete);
                    List<AtlasClassificationDef> atlasClassificationDefs = atlasTypesDef.getClassificationDefs();
                    atlasClassificationDefs.add(atlasClassificationDef);
                    break;

                case RELATIONSHIP_DEF:
                    RelationshipDef relationshipDefToDelete = (RelationshipDef) omTypeDefToDelete;
                    AtlasRelationshipDef atlasRelationshipDef = convertOMRelationshipDefToAtlasRelationshipDef(relationshipDefToDelete);
                    List<AtlasRelationshipDef> atlasRelationshipDefs = atlasTypesDef.getRelationshipDefs();
                    atlasRelationshipDefs.add(atlasRelationshipDef);
                    break;

                case ENTITY_DEF:
                    EntityDef entityDefToDelete = (EntityDef) omTypeDefToDelete;
                    AtlasEntityDef atlasEntityDef = convertOMEntityDefToAtlasEntityDef(entityDefToDelete);
                    List<AtlasEntityDef> atlasEntityDefs = atlasTypesDef.getEntityDefs();
                    atlasEntityDefs.add(atlasEntityDef);
                    break;

                default:
                    LOG.error("deleteTypeDef: invalid typedef category {}", typeDefCategory);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, omTypeDefToDelete.toString());

                    throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

            }

            /*
             * Ask Atlas to perform the delete operation
             */
            typeDefStore.deleteTypesDef(atlasTypesDef);

        }
        catch (TypeErrorException  e) {
            LOG.error("The retrieved TypeDef could not be converted to Atlas format", e);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, omTypeDefToDelete.toString());

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        catch (AtlasBaseException  e) {
            LOG.error("The Atlas repository could not delete the TypeDef", e);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, omTypeDefToDelete.toString());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Delete an AttributeTypeDef.  This is only possible if the AttributeTypeDef has never been used to create
     * instances or any instances of this AttributeTypeDef have been purged from the metadata collection.
     *
     * @param userId              - unique identifier for requesting user.
     * @param obsoleteTypeDefGUID - String unique identifier for the AttributeTypeDef.
     * @param obsoleteTypeDefName - String unique name for the AttributeTypeDef.
     * @throws InvalidParameterException  - the one of AttributeTypeDef identifiers is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the requested AttributeTypeDef is not found in the metadata collection.
     * @throws TypeDefInUseException      - the AttributeTypeDef can not be deleted because there are instances of this type in the
     *                                    the metadata collection.  These instances need to be purged before the
     *                                    AttributeTypeDef can be deleted.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void deleteAttributeTypeDef(String userId,
                                       String obsoleteTypeDefGUID,
                                       String obsoleteTypeDefName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            TypeDefInUseException,
            UserNotAuthorizedException {
        final String methodName = "deleteAttributeTypeDef";
        final String guidParameterName = "obsoleteTypeDefGUID";
        final String nameParameterName = "obsoleteTypeDefName";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                obsoleteTypeDefGUID,
                obsoleteTypeDefName,
                methodName);

        /*
         * Perform operation
         */
        AtlasBaseTypeDef abtd;

        /*
         * Look in the Atlas type def store for the obsolete attribute type def so that we know its category
         */
        try {

            if (!useRegistry) {
                // Look in the Atlas type def store
                abtd = typeDefStore.getByGuid(obsoleteTypeDefGUID);
            } else {
                // Using registry
                abtd = typeRegistry.getTypeDefByGuid(obsoleteTypeDefGUID);
            }

        } catch (AtlasBaseException e) {

            if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_NAME_NOT_FOUND) {
                LOG.debug("deleteAttributeTypeDef: Atlas does not have the type with GUID {} ", obsoleteTypeDefGUID);
                // The TypeDef was not found - ensure atlasPreTypeDef is null, exception to be handled below
                abtd = null;

            } else {

                LOG.error("deleteAttributeTypeDef: Caught exception trying to retrieve Atlas type with GUID {} ", obsoleteTypeDefGUID, e);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "deleteAttributeTypeDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        // Separate null check ensures we have covered both cases (registry and store)
        if (abtd == null) {

            LOG.debug("deleteAttributeTypeDef: received null return from Atlas getByName using GUID {}", obsoleteTypeDefGUID);
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(obsoleteTypeDefGUID, "unknown", "obsoleteTypeDefGUID", "deleteAttributeTypeDef", metadataCollectionId);

            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "deleteAttributeTypeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // From here on we know that atlasPreTypeDef is not null


        // We have an AtlasBaseTypeDef
        TypeCategory atlasTypeCategory = abtd.getCategory();

        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();

        try {
            switch (atlasTypeCategory) {

                case ENUM:
                    AtlasEnumDef atlasEnumDef = (AtlasEnumDef) abtd;
                    List<AtlasEnumDef> atlasEnumDefs = atlasTypesDef.getEnumDefs();
                    atlasEnumDefs.add(atlasEnumDef);
                    break;

                case PRIMITIVE:
                case ARRAY:
                case MAP:
                    LOG.debug("There is nothing to do when the AttributeTypeDef is a primitive or collection");
                    return;

                default:
                    LOG.error("deleteAttributeTypeDef: unsupported attribute type def category {}", atlasTypeCategory);
                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, abtd.toString());

                    throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
            }

            /*
             * Ask Atlas to perform the delete operation
             */
            typeDefStore.deleteTypesDef(atlasTypesDef);

        }
        catch (AtlasBaseException  e) {
            LOG.error("The Atlas repository could not delete the AttributeTypeDef", e);
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(obsoleteTypeDefName, obsoleteTypeDefGUID, guidParameterName, methodName, repositoryName, abtd.toString());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Change the guid or name of an existing TypeDef to a new value.  This is used if two different
     * TypeDefs are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId              - unique identifier for requesting user.
     * @param originalTypeDefGUID - the original guid of the TypeDef.
     * @param originalTypeDefName - the original name of the TypeDef.
     * @param newTypeDefGUID      - the new identifier for the TypeDef.
     * @param newTypeDefName      - new name for this TypeDef.
     * @return typeDef - new values for this TypeDef, including the new guid/name.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the TypeDef identified by the original guid/name is not found
     *                                    in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public TypeDef reIdentifyTypeDef(String userId,
                                     String originalTypeDefGUID,
                                     String originalTypeDefName,
                                     String newTypeDefGUID,
                                     String newTypeDefName)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {
        final String    methodName                = "reIdentifyTypeDef";
        final String    originalGUIDParameterName = "originalTypeDefGUID";
        final String    originalNameParameterName = "originalTypeDefName";
        final String    newGUIDParameterName      = "newTypeDefGUID";
        final String    newNameParameterName      = "newTypeDefName";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                originalGUIDParameterName,
                originalNameParameterName,
                originalTypeDefGUID,
                originalTypeDefName,
                methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                newGUIDParameterName,
                newNameParameterName,
                newTypeDefGUID,
                newTypeDefName,
                methodName);

        /*
         * Perform operation
         */
        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }


    /**
     * Change the guid or name of an existing TypeDef to a new value.  This is used if two different
     * TypeDefs are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param originalAttributeTypeDefGUID - the original guid of the AttributeTypeDef.
     * @param originalAttributeTypeDefName - the original name of the AttributeTypeDef.
     * @param newAttributeTypeDefGUID      - the new identifier for the AttributeTypeDef.
     * @param newAttributeTypeDefName      - new name for this AttributeTypeDef.
     * @return attributeTypeDef - new values for this AttributeTypeDef, including the new guid/name.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeDefNotKnownException   - the AttributeTypeDef identified by the original guid/name is not
     *                                    found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public AttributeTypeDef reIdentifyAttributeTypeDef(String userId,
                                                       String originalAttributeTypeDefGUID,
                                                       String originalAttributeTypeDefName,
                                                       String newAttributeTypeDefGUID,
                                                       String newAttributeTypeDefName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException
    {

        final String    methodName                = "reIdentifyAttributeTypeDef";
        final String    originalGUIDParameterName = "originalAttributeTypeDefGUID";
        final String    originalNameParameterName = "originalAttributeTypeDefName";
        final String    newGUIDParameterName      = "newAttributeTypeDefGUID";
        final String    newNameParameterName      = "newAttributeTypeDefName";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                originalGUIDParameterName,
                originalNameParameterName,
                originalAttributeTypeDefGUID,
                originalAttributeTypeDefName,
                methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                newGUIDParameterName,
                newNameParameterName,
                newAttributeTypeDefGUID,
                newAttributeTypeDefName,
                methodName);

        /*
         * Perform operation
         */
        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }


    // ==========================================================================================================================

    // Group 3 methods

    /**
     * Returns a boolean indicating if the entity is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the entity.
     * @return entity details if the entity is found in the metadata collection; otherwise return null.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail isEntityKnown(String userId,
                                      String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> isEntityKnown(userId={}, guid={})", userId, guid);
        }

        final String  methodName = "isEntityKnown";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */

        EntityDetail entityDetail;
        try {
            entityDetail = getEntityDetail(userId, guid);
        } catch (EntityNotKnownException e) {
            LOG.error("isEntityKnown: caught EntityNotKnownException exception from getEntityDetail - exception swallowed, returning null", e);
            return null;
        } catch (RepositoryErrorException e) {
            LOG.error("isEntityKnown: caught RepositoryErrorException exception from getEntityDetail - rethrowing", e);
            throw e;
        } catch (UserNotAuthorizedException e) {
            LOG.error("isEntityKnown: caught UserNotAuthorizedException exception from getEntityDetail - rethrowing", e);
            throw e;
        }

        LOG.debug("isEntityKnown: retrieved EntityDetail {}", entityDetail);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== isEntityKnown(userId={}, guid={}: entityDetail={})", userId, guid, entityDetail);
        }
        return entityDetail;
    }

    public boolean isEntityProxy(String guid) throws EntityNotKnownException {
        // Using the supplied guid look up the entity.
        final String methodName = "isEntityProxy";
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            atlasEntityWithExt = entityStore.getById(guid);
        } catch (AtlasBaseException e) {
            LOG.error("isEntityProxy: caught exception from to get entity from Atlas repository, guid {}", guid, e);
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();
        LOG.debug("isEntityProxy: AtlasEntity proxy {}",atlasEntity.isProxy());
        return atlasEntity.isProxy();
    }

    public boolean isEntityLocal(String guid) throws EntityNotKnownException {
        // Using the supplied guid look up the entity.
        final String methodName = "isEntityLocal";
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            atlasEntityWithExt = entityStore.getById(guid);
        } catch (AtlasBaseException e) {
            LOG.error("isEntityLocal: caught exception from to get entity from Atlas repository, guid {}", guid, e);
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();
        LOG.debug("isEntityLocal: AtlasEntity local {}", atlasEntity.getHomeId() == null);
        return atlasEntity.getHomeId() == null;
    }

    /**
     * Return the header and classifications for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the entity.
     * @return EntitySummary structure
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntitySummary getEntitySummary(String userId,
                                          String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getEntitySummary(userId={}, guid={})", userId, guid);
        }

        final String  methodName        = "getEntitySummary";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */

        // Using the supplied guid look up the entity.
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            atlasEntityWithExt = entityStore.getById(guid);
        } catch (AtlasBaseException e) {
            LOG.error("getEntitySummary: caught exception from to get entity from Atlas repository, guid {}", guid, e);
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();


        // Project the AtlasEntity as an EntitySummary

        try {
            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
            EntitySummary omEntitySummary = atlasEntityMapper.toEntitySummary();
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== getEntitySummary(userId={}, guid={}: entitySummary={})", userId, guid, omEntitySummary);
            }
            return omEntitySummary;
        }
        catch (Exception e) {
            LOG.error("getEntitySummary: caught exception {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
    }


    /**
     * Return the header, classifications and properties of a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the entity.
     * @return EntityDetail structure.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail getEntityDetail(String userId,
                                        String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getEntityDetail(userId={}, guid={})", userId, guid);
        }

        final String  methodName        = "getEntityDetail";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */


        // Using the supplied guid look up the entity.

        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            atlasEntityWithExt = entityStore.getById(guid);
        } catch (AtlasBaseException e) {

            LOG.error("getEntityDetail: caught exception from get entity by guid {}, {}",guid, e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        LOG.debug("getEntityDetail: atlasEntityWithExt is {}", atlasEntityWithExt);
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();


        // Project the AtlasEntity as an EntityDetail

        try {

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
            EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== getEntityDetail(userId={}, guid={}: entityDetail={})", userId, guid, omEntityDetail);
            }
            return omEntityDetail;

        } catch (TypeErrorException | InvalidEntityException e) {
            LOG.error("getEntityDetail: caught exception from attempt to convert Atlas entity to OM {}, {}", atlasEntity, e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
    }


    /**
     * Return a historical versionName of an entity - includes the header, classifications and properties of the entity.
     *
     * @param userId     - unique identifier for requesting user.
     * @param guid       - String unique identifier for the entity.
     * @param asOfTime   - the time used to determine which versionName of the entity that is desired.
     * @return EntityDetail structure.
     * @throws InvalidParameterException     - the guid or date is null or date is for future time
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws EntityNotKnownException       - the requested entity instance is not known in the metadata collection
     *                                         at the time requested.
     * @throws EntityProxyOnlyException      - the requested entity instance is only a proxy in the metadata collection.
     * @throws FunctionNotSupportedException - the repository does not support satOfTime parameter.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public EntityDetail getEntityDetail(String  userId,
                                        String  guid,
                                        Date    asOfTime)
        throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            EntityProxyOnlyException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String  methodName        = "getEntityDetail";
        final String  guidParameterName = "guid";
        final String  asOfTimeParameter = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        /*
         * Perform operation
         */

        // This method requires a historic query which is not supported
        LOG.debug("getEntityDetail: Does not support asOfTime historic retrieval");

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        String errorMessage = errorCode.getErrorMessageId()
                + errorCode.getFormattedErrorMessage(guid, methodName, metadataCollectionId);

        throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());


    }




    /**
     * Return the relationships for a specific entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityGUID - String unique identifier for the entity.
     * @param relationshipTypeGUID - String GUID of the the type of relationship required (null for all).
     * @param fromRelationshipElement - the starting element number of the relationships to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize -- the maximum number of result classifications that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return Relationships list.  Null means no relationships associated with the entity.
     * @throws InvalidParameterException a parameter is invalid or null.
     * @throws TypeErrorException the type guid passed on the request is not known by the metadata collection.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException the requested entity instance is not known in the metadata collection.
     * @throws PropertyErrorException the sequencing property is not valid for the attached classifications.
     * @throws PagingErrorException the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    public List<Relationship> getRelationshipsForEntity(String                     userId,
                                                        String                     entityGUID,
                                                        String                     relationshipTypeGUID,
                                                        int                        fromRelationshipElement,
                                                        List<InstanceStatus>       limitResultsByStatus,
                                                        Date                       asOfTime,
                                                        String                     sequencingProperty,
                                                        SequencingOrder            sequencingOrder,
                                                        int                        pageSize)
        throws
            InvalidParameterException,
            TypeErrorException,
            RepositoryErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException

    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getRelationshipsForEntity(userId={}, entityGUID={}, relationshipTypeGUID={}, fromRelationshipElement={}, limitResultsByStatus={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})", userId, entityGUID, relationshipTypeGUID, fromRelationshipElement, limitResultsByStatus, asOfTime, sequencingProperty, sequencingOrder, pageSize);
        }
        final String methodName = "getRelationshipsForEntity";
        final String guidParameterName = "entityGUID";
        final String typeGUIDParameterName = "relationshipTypeGUID";
        final String asOfTimeParameter = "asOfTime";
        final String pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, entityGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        try {
            this.validateTypeGUID(userId, repositoryName, typeGUIDParameterName, relationshipTypeGUID, methodName);
        }
        catch (TypeErrorException e) {
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage( "unknown", relationshipTypeGUID, typeGUIDParameterName, methodName, repositoryName, "unknown");

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        /*
         * Perform operation
         */

        // Historical queries are not yet supported.
        if (asOfTime != null) {
            LOG.debug("getRelationshipsForEntity: Request to find relationships as they were at time {} is not supported", asOfTime);
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Sequenced queries are not yet supported. TODO commented block to be removed when tested
        //if (!StringUtil.isBlank(sequencingProperty)) {
        //    LOG.debug("getRelationshipsForEntity: Request to find relationships sequenced by {} is not supported", sequencingProperty);
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Paged queries are not yet supported. TODO commented block to be removed when tested
        //if (pageSize != 0) {
        //    LOG.debug("getRelationshipsForEntity: Request to page relationships using pageSize {}", pageSize);
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Result offset is not yet supported. TODO commented block to be removed when tested
        //if (fromRelationshipElement != 0) {
        //    LOG.debug("getRelationshipsForEntity: Request to offset relationships is not supported");
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {

            atlasEntityWithExt = entityStore.getById(entityGUID);

        } catch (AtlasBaseException e) {
            LOG.error("getRelationshipsForEntity: caught exception from attempt to get Atlas entity by GUID {}", entityGUID, e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

        List<Relationship> matchingRelationships = null;
        try {
            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
            EntityUniverse entityUniverse = atlasEntityMapper.toEntityUniverse();
            List<Relationship> allRelationships = entityUniverse.getEntityRelationships();
            if (allRelationships != null && !allRelationships.isEmpty() ) {
                for (Relationship r : allRelationships) {
                    /*  If there is no type filtering, or if the relationship type matches then
                     *  consider including it in the result.
                     */
                    if (relationshipTypeGUID == null || r.getType().getTypeDefGUID().equals(relationshipTypeGUID)) {

                        /*  If there is no status filter specified, or if the relationship satisfies the status filter,
                         *  include the relationship in the result, otherwise skip the relationship
                         */
                        if (limitResultsByStatus != null) {
                            // Need to check that the relationship status is in the list of allowed status values
                            InstanceStatus relStatus = r.getStatus();
                            boolean match = false;
                            for (InstanceStatus allowedStatus : limitResultsByStatus) {
                                if (relStatus == allowedStatus) {
                                    match = true;
                                    break;
                                }
                            }
                            if (!match) {
                                continue;  // skip this relationship and process the next, if any remain
                            }
                        }

                        // Need to add the relationship to the result
                        if (matchingRelationships == null)
                            matchingRelationships = new ArrayList<>();
                        matchingRelationships.add(r);
                    }
                }
            }
        }
        catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {
            LOG.error("getRelationshipsForEntity: Caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getRelationshipsForEntity(userId={}, entityGUID={}, relationshipTypeGUID={}, fromRelationshipElement={}, limitResultsByStatus={}, " +
                            "asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}: matchingRelationships={})",
                    userId, entityGUID, relationshipTypeGUID, fromRelationshipElement, limitResultsByStatus,
                    asOfTime, sequencingProperty, sequencingOrder, pageSize, matchingRelationships);
        }


        return formatRelationshipResults(matchingRelationships, fromRelationshipElement, sequencingProperty, sequencingOrder, pageSize);

    }

    /**
     * Return a list of entities that match the supplied properties according to the match criteria.  The results
     * can be returned over many pages.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - String unique identifier for the entity type of interest (null means any entity type).
     * @param matchProperties - List of entity properties to match to (null means match on entityTypeGUID only).
     * @param matchCriteria - Enum defining how the properties should be matched to the entities in the repository.
     * @param fromEntityElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty - String name of the entity property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException - a parameter is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws PropertyErrorException - the properties specified are not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public  List<EntityDetail> findEntitiesByProperty(String                    userId,
                                                      String                    entityTypeGUID,
                                                      InstanceProperties        matchProperties,
                                                      MatchCriteria             matchCriteria,
                                                      int                       fromEntityElement,
                                                      List<InstanceStatus>      limitResultsByStatus,
                                                      List<String>              limitResultsByClassification,
                                                      Date                      asOfTime,
                                                      String                    sequencingProperty,
                                                      SequencingOrder           sequencingOrder,
                                                      int                       pageSize)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByProperty(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})",
                    userId,entityTypeGUID,matchProperties,matchCriteria,fromEntityElement,limitResultsByStatus,limitResultsByClassification,asOfTime,sequencingProperty,sequencingOrder,pageSize );
        }

        final String  methodName                   = "findEntitiesByProperty";
        final String  matchCriteriaParameterName   = "matchCriteria";
        final String  matchPropertiesParameterName = "matchProperties";
        final String  guidParameterName            = "entityTypeGUID";
        final String  asOfTimeParameter            = "asOfTime";
        final String  pageSizeParameter            = "pageSize";
        final String  offsetParameter              = "fromEntityElement";
        final String  sequencingOrderParameter     = "sequencingOrder";
        final String  sequencingPropertyParameter  = "sequencingProperty";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);
        repositoryValidator.validateMatchCriteria(repositoryName,
                matchCriteriaParameterName,
                matchPropertiesParameterName,
                matchCriteria,
                matchProperties,
                methodName);

        try {
            this.validateTypeGUID(userId, repositoryName, guidParameterName, entityTypeGUID, methodName);

        } catch (TypeErrorException e) {
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage( "unknown", entityTypeGUID, guidParameterName, methodName, repositoryName, "unknown");

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Perform operation
         */


        /*
         *  This method behaves as follows:
         *  * The optional entityTypeGuid restricts the find to entities of the given type - null will span all types.
         *
         *  * The optional InstanceProperties object contains a list of property filters - each property filter specifies
         *    the type, name and value of a property that a matching entity must possess. Only the named properties are
         *    compared; properties that are not named in the InstanceProperties object are ignored. Where a named property
         *    is of type String, the property will match if the specified value is contained in the value of the named
         *    property - i.e. it does not have be an exact match. For all other types of property - i.e. non-String
         *    properties - the value comparison is an exact match.
         *
         *  * The property filters can be combined using the matchCriteria of ALL, ANY or NONE. If null, this will default to ALL
         *
         *  * The results can be narrowed by optionally specifying one or more instance status values - the results
         *    will only contain entities that have one of the specified status values
         *
         *  * The results can be narrowed by optionally specifying one or more classifications - the results will
         *    only contain entities that have all of the specified classifications
         *
         *  All of the above filters are optional - if none of them are supplied - i.e. no entityTypeGUID, matchProperties,
         *  status values or classifications - then ALL entities will be returned. This will perform very badly.
         *
         *
         *  IMPORTANT NOTE
         *  --------------
         *  It is difficult to see how to implement the combination of NONE and string searchCriteria - there is no
         *  NOT operator in DSL nor in SearchWithParameters, and the only solution I can currently think of is a
         *  negated regex which seems unduly complex. Short of performing two searches and subtracting one from the
         *  other - which would not perform well - I am currently stuck for a solution. This might require direct manipulation
         *  of the gremlin - which would make for an untidy solution alongside Atlas's other query methods; or we would
         *  need to introduce negation operators into Atlas, which will take longer to implement but is probably the nicest
         *  option. So for now this method is not supporting matchCriteria NONE.
         *
         */

        if (matchCriteria == MatchCriteria.NONE) {
            LOG.debug("findEntitiesByProperty: Request to find entities using matchCriteria NONE is not supported");
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(matchCriteriaParameterName, methodName, metadataCollectionId);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Historical queries are not yet supported.
        if (asOfTime != null) {
            LOG.debug("findEntitiesByProperty: Request to find entities as they were at time {} is not supported", asOfTime);
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Sequenced queries are not yet supported. TODO commented block to be removed when tested
        //if (!StringUtil.isBlank(sequencingProperty)) {
        //    LOG.debug("findEntitiesByProperty: Request to find entities sequenced by {} is not supported", sequencingProperty);
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(sequencingOrderParameter+" or "+sequencingPropertyParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Paged queries are not yet supported. TODO commented block to be removed when tested
        //if (pageSize != 0) {
        //    LOG.debug("findEntitiesByProperty: Request to page entities using pageSize {}", pageSize);
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(pageSizeParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Result offset is not yet supported. TODO commented block to be removed when tested
        //if (fromEntityElement != 0) {
        //    LOG.debug("findEntitiesByProperty: Request to offset entities is not supported");
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(offsetParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Because there is not post-filtering by this method, pass the offset and pageSize through to the underlying methods

        List<EntityDetail> returnList;
        if (entityTypeGUID != null) {   // Use DSL
            try {
                returnList = findEntitiesByPropertyUsingDSL(userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification, fromEntityElement, pageSize);
            }
            catch (TypeErrorException | RepositoryErrorException | PropertyErrorException e) {
                // Log and re-throw
                LOG.debug("findEntitiesByProperty: Caught exception from findEntitiesByPropertyUsingDSL");
                throw e;
            }

        } else {

            // entityTypeGUID == null means type is not specified, use searchWithParameters
            returnList = findEntitiesByPropertyUsingSearchParameters(userId, matchProperties, matchCriteria,  limitResultsByStatus, limitResultsByClassification, fromEntityElement, pageSize);

        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByProperty(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}): returnList={}",
                    userId,entityTypeGUID,matchProperties,matchCriteria,fromEntityElement,limitResultsByStatus,limitResultsByClassification,asOfTime,sequencingProperty,sequencingOrder,pageSize,returnList );
        }

        // Have already applied offset and pageSize - do not apply offset again, hence fromELement set to 0
        return formatEntityResults(returnList, 0, sequencingProperty, sequencingOrder, pageSize);

    }



    /**
     * Return a list of entities that have the requested type of classification attached.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - unique identifier for the type of entity requested.  Null mans any type of entity.
     * @param classificationName - name of the classification - a null is not valid.
     * @param matchClassificationProperties - list of classification properties used to narrow the search.
     * @param matchCriteria - Enum defining how the properties should be matched to the classifications in the repository.
     * @param fromEntityElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty - String name of the entity property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException - a parameter is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws ClassificationErrorException - the classification request is not known to the metadata collection.
     * @throws PropertyErrorException - the properties specified are not valid for the requested type of
     *                                  classification.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> findEntitiesByClassification(String                    userId,
                                                           String                    entityTypeGUID,
                                                           String                    classificationName,
                                                           InstanceProperties        matchClassificationProperties,
                                                           MatchCriteria             matchCriteria,
                                                           int                       fromEntityElement,
                                                           List<InstanceStatus>      limitResultsByStatus,
                                                           Date                      asOfTime,
                                                           String                    sequencingProperty,
                                                           SequencingOrder           sequencingOrder,
                                                           int                       pageSize)

            throws
                InvalidParameterException,
                RepositoryErrorException,
                TypeErrorException,
                ClassificationErrorException,
                PropertyErrorException,
                PagingErrorException,
                FunctionNotSupportedException,
                UserNotAuthorizedException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByClassification(userId={}, entityTypeGUID={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})",
                    userId,entityTypeGUID,classificationName,matchClassificationProperties,matchCriteria,fromEntityElement,limitResultsByStatus,asOfTime,sequencingProperty,sequencingOrder,pageSize );
        }

        final String  methodName                   = "findEntitiesByClassification";
        final String  classificationParameterName  = "classificationName";
        final String  entityTypeGUIDParameterName  = "entityTypeGUID";

        final String  matchCriteriaParameterName   = "matchCriteria";
        final String  matchPropertiesParameterName = "matchClassificationProperties";
        final String  asOfTimeParameter            = "asOfTime";
        final String  pageSizeParameter            = "pageSize";
        final String  sequencingParameter          = "sequencingOrder";
        final String  offsetParameter              = "fromEntityElement";



        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        try {
            this.validateTypeGUID(userId, repositoryName, entityTypeGUIDParameterName, entityTypeGUID, methodName);
        } catch (TypeErrorException e) {
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, entityTypeGUIDParameterName, methodName, repositoryName, "unknown");

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Validate TypeDef
         */
        if (entityTypeGUID != null)
        {
            TypeDef entityTypeDef;
            try {
                entityTypeDef = _getTypeDefByGUID(userId, entityTypeGUID);
            }
            catch (TypeDefNotKnownException e) {
                // handle below
                LOG.error("findEntitiesByClassification: caught exception from _getTypeDefByGUID {}", e);
                entityTypeDef = null;
            }
            if (entityTypeDef == null || entityTypeDef.getCategory() != ENTITY_DEF) {

                OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage( "unknown", entityTypeGUID, entityTypeGUIDParameterName, methodName, repositoryName, "unknown");

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            repositoryValidator.validateTypeDefForInstance(repositoryName,
                    entityTypeGUIDParameterName,
                    entityTypeDef,
                    methodName);

            repositoryValidator.validateClassification(repositoryName,
                    classificationParameterName,
                    classificationName,
                    entityTypeDef.getName(),
                    methodName);
        }
        else
        {
            repositoryValidator.validateClassification(repositoryName,
                    classificationParameterName,
                    classificationName,
                    null,
                    methodName);
        }

        repositoryValidator.validateMatchCriteria(repositoryName,
                matchCriteriaParameterName,
                matchPropertiesParameterName,
                matchCriteria,
                matchClassificationProperties,
                methodName);

        /*
         * Perform operation
         */

        // Historical queries are not yet supported.
        if (asOfTime != null) {
            LOG.debug("findEntitiesByClassification: Request to find entities as they were at time {} is not supported", asOfTime);
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Sequencing of results is not yet supported. TODO commented block to be removed when tested
        //if (!StringUtil.isBlank(sequencingProperty)) {
        //    LOG.debug("findEntitiesByClassification: Request to sequence entities is not supported");
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(sequencingParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Paging of results is not yet supported. TODO commented block to be removed when tested
        //if (pageSize != 0) {
        //    LOG.debug("findEntitiesByClassification: Request to page entities is not supported");
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(pageSizeParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Result offset is not yet supported. TODO commented block to be removed when tested
        //if (fromEntityElement != 0) {
        //    LOG.debug("findEntitiesByClassification: Request to offset entities is not supported");
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(offsetParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}


        if (classificationName == null) {
            LOG.error("findEntitiesByClassification: Classification name must not be null; please specify a classification name");
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CLASSIFICATION_NAME;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(offsetParameter, methodName, metadataCollectionId);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // There is no post-filtering by this method, so pass offset and pageSize through to underlying methods

        List<EntityDetail> returnList;
        // If we have a guid use DSL
        if (entityTypeGUID != null) {
            // Use DSL
            returnList = findEntitiesByClassificationUsingDSL(userId, entityTypeGUID, classificationName, matchClassificationProperties, matchCriteria,  limitResultsByStatus, fromEntityElement, pageSize);

        } else {
            // Type is not specified so use searchWithParameters
            returnList = findEntitiesByClassificationUsingSearchParameters(userId, classificationName, matchClassificationProperties, matchCriteria,  limitResultsByStatus, fromEntityElement, pageSize);
        }

        // Offset and pageSize are handled in the queries above - more efficient at the server. So do not apply them again here.
        // Hence fromElement is set to 0; pageSize can be left as specified by caller.
        return formatEntityResults(returnList, 0, sequencingProperty, sequencingOrder, pageSize);


    }



    /**
     * Return a list of entities whose string based property values match the search criteria.  The
     * search criteria may include regex style wild cards.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - GUID of the type of entity to search for. Null means all types will
     *                       be searched (could be slow so not recommended).
     * @param searchCriteria - String expression contained in any of the property values within the entities
     *                       of the supplied type.
     * @param fromEntityElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus - By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime - Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria - null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException - a parameter is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> findEntitiesByPropertyValue(String                userId,
                                                          String                entityTypeGUID,
                                                          String                searchCriteria,
                                                          int                   fromEntityElement,
                                                          List<InstanceStatus>  limitResultsByStatus,
                                                          List<String>          limitResultsByClassification,
                                                          Date                  asOfTime,
                                                          String                sequencingProperty,
                                                          SequencingOrder       sequencingOrder,
                                                          int                   pageSize)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException

    {


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByPropertyValue(userId={}, entityTypeGUID={}, searchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={})",
                    userId,entityTypeGUID,searchCriteria,fromEntityElement,limitResultsByStatus,limitResultsByClassification, asOfTime,sequencingProperty,sequencingOrder,pageSize );
        }

        final String  methodName = "findEntitiesByPropertyValue";
        final String  searchCriteriaParameterName = "searchCriteria";
        final String  asOfTimeParameter = "asOfTime";
        final String  typeGUIDParameter = "entityTypeGUID";
        final String  pageSizeParameter = "pageSize";
        final String  offsetParameter   = "fromEntityElement";
        final String  sequencingParameter = "sequencingOrder";


        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateSearchCriteria(repositoryName, searchCriteriaParameterName, searchCriteria, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        try {
            this.validateTypeGUID(userId, repositoryName, typeGUIDParameter, entityTypeGUID, methodName);
        } catch (TypeErrorException e) {
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, typeGUIDParameter, methodName, repositoryName, "unknown");

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        /*
         * Process operation
         */

        // The nature of this find/search is that it will match an entity that has ANY matching property
        // i.e. any string property containing the searchCriteria string. It only operates on string
        // properties (other property types are ignored) and it implicitly uses matchCriteria.ANY.

        // Historical queries are not yet supported.
        if (!StringUtil.isBlank(sequencingProperty)) {
            LOG.debug("findEntitiesByPropertyValue: Request to find sequenced entities using {} is not supported", sequencingProperty);
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(sequencingParameter, methodName, metadataCollectionId);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Sequenced queries are not yet supported. TODO commented block to be removed when tested
        //if (asOfTime != null) {
        //    LOG.debug("findEntitiesByPropertyValue: Request to find entities as they were at time {} is not supported", asOfTime);
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}

        // Paging of results not yet supported. TODO commented block to be removed when tested
        //if (pageSize != 0) {
        //    LOG.debug("findEntitiesByPropertyValue: Request to page entities, page size {} is not supported", pageSize);
        //    OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        //    String errorMessage = errorCode.getErrorMessageId()
        //            + errorCode.getFormattedErrorMessage(pageSizeParameter, methodName, metadataCollectionId);
        //
        //    throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
        //            this.getClass().getName(),
        //            methodName,
        //            errorMessage,
        //            errorCode.getSystemAction(),
        //            errorCode.getUserAction());
        //}


        // There is no post-processing performed by this method, so it is safe to delegate offset to the underlying methods...

        List<EntityDetail> returnList;
        if (entityTypeGUID != null) {
            // Type specified so use DSL
            try {
                returnList = findEntitiesByPropertyUsingDSL(userId, entityTypeGUID, searchCriteria, limitResultsByStatus, limitResultsByClassification, fromEntityElement, pageSize);
            }
            catch (TypeErrorException | RepositoryErrorException | PropertyErrorException e) {
                LOG.error("findEntitiesByPropertyValue: Caught exception from findEntitiesByPropertyUsingDSL", e);
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("findEntitiesByProperty", methodName, metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());            }
        } else {
            // Since entityTypeGUID is null it means type is not specified, so use searchWithParameters
            // Prepare the matchProperties as we should be matching string properties only...
            returnList = findEntitiesByPropertyUsingSearchParameters(userId, searchCriteria, limitResultsByStatus, limitResultsByClassification, fromEntityElement, pageSize);

        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByPropertyValue(userId={}, entityTypeGUID={}, searchCriteria={}, fromEntityElement={}, limitResultsByStatus={}, limitResultsByClassification={}, asOfTime={}, sequencingProperty={}, sequencingOrder={}, pageSize={}): returnList={}",
                    userId,entityTypeGUID,searchCriteria,fromEntityElement,limitResultsByStatus,limitResultsByClassification, asOfTime,sequencingProperty,sequencingOrder,pageSize,returnList );
        }

        // Already applied offset and pageSize so do not offset again - set fomrElement to 0
        return formatEntityResults(returnList, 0, sequencingProperty, sequencingOrder, pageSize);

    }


    /**
     * Returns a boolean indicating if the relationship is stored in the metadata collection.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the relationship.
     * @return relationship details if the relationship is found in the metadata collection; otherwise return null.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public Relationship isRelationshipKnown(String userId,
                                            String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String methodName = "isRelationshipKnown";
        final String guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Process operation
         */

        try {
            return getRelationship(userId, guid);
        }
        catch (RelationshipNotKnownException e) {
            return null;
        }

    }


    /**
     * Return a requested relationship.
     *
     * @param userId - unique identifier for requesting user.
     * @param guid   - String unique identifier for the relationship.
     * @return a relationship structure.
     * @throws InvalidParameterException     - the guid is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the metadata collection does not have a relationship with
     *                                       the requested GUID stored.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship getRelationship(String userId,
                                        String guid)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {
        final String  methodName = "getRelationship";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Process operation
         */
        AtlasRelationship atlasRelationship;
        try {
            atlasRelationship = relationshipStore.getById(guid);

        } catch (AtlasBaseException e) {
            LOG.error("getRelationship: Caught exception from Atlas {}", e);
            OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("findEntitiesByProperty", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        LOG.debug("getRelationship: Read from atlas relationship store; relationship {}", atlasRelationship);
        Relationship omRelationship;

        try {
            AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                    this,
                    userId,
                    atlasRelationship,
                    entityStore);

            omRelationship = atlasRelationshipMapper.toOMRelationship();
            LOG.debug("getRelationship: om relationship {}", omRelationship);

        }
        catch (Exception e) {
            LOG.debug("getRelationship: caught exception from mapper "+e.getMessage());
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guidParameterName,
                    methodName,
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        LOG.debug("getRelationship: returning relationship {}", omRelationship);
        return omRelationship;
    }


    /**
     * Return a historical versionName of a relationship.
     *
     * @param userId   - unique identifier for requesting user.
     * @param guid     - String unique identifier for the relationship.
     * @param asOfTime - the time used to determine which versionName of the entity that is desired.
     * @return EntityDetail structure.
     * @throws InvalidParameterException     - the guid or date is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested entity instance is not known in the metadata collection
     *                                         at the time requested
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship getRelationship(String userId,
                                        String guid,
                                        Date asOfTime)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String  methodName = "getRelationship";
        final String  guidParameterName = "guid";
        final String  asOfTimeParameter = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        /*
         * Perform operation
         */

        // This method requires a historic query which is not supported
        LOG.debug("getRelationship: Does not support asOfTime historic retrieval");
        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        String errorMessage = errorCode.getErrorMessageId()
                + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

        throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    /**
     * Return a list of relationships that match the requested properties by the matching criteria.   The results
     * can be broken into pages.
     *
     * @param userId                  - unique identifier for requesting user
     * @param relationshipTypeGUID    - unique identifier (guid) for the new relationship's type.
     * @param matchProperties         - list of  properties used to narrow the search.
     * @param matchCriteria           - Enum defining how the properties should be matched to the relationships in the repository.
     * @param fromEntityDetailElement - the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus    - By default, relationships in all statuses are returned.  However, it is possible
     *                                to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                status values.
     * @param asOfTime                - Requests a historical query of the relationships for the entity.  Null means return the
     *                                present values.
     * @param sequencingProperty      - String name of the property that is to be used to sequence the results.
     *                                Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder         - Enum defining how the results should be ordered.
     * @param pageSize                - the maximum number of result relationships that can be returned on this request.  Zero means
     *                                unrestricted return results size.
     * @return a list of relationships.  Null means no matching relationships.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException         - the type guid passed on the request is not known by the
     *                                    metadata collection.
     * @throws PropertyErrorException     - the properties specified are not valid for any of the requested types of
     *                                    relationships.
     * @throws PagingErrorException       - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<Relationship> findRelationshipsByProperty(String userId,
                                                          String relationshipTypeGUID,
                                                          InstanceProperties matchProperties,
                                                          MatchCriteria matchCriteria,
                                                          int fromEntityDetailElement,
                                                          List<InstanceStatus> limitResultsByStatus,
                                                          Date asOfTime,
                                                          String sequencingProperty,
                                                          SequencingOrder sequencingOrder,
                                                          int pageSize)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            PagingErrorException,
            UserNotAuthorizedException

    {
        final String  methodName = "findRelationshipsByProperty";
        final String  matchCriteriaParameterName = "matchCriteria";
        final String  matchPropertiesParameterName = "matchProperties";
        final String  guidParameterName = "relationshipTypeGUID";
        final String  asOfTimeParameter = "asOfTime";
        final String  pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);
        repositoryValidator.validateMatchCriteria(repositoryName,
                matchCriteriaParameterName,
                matchPropertiesParameterName,
                matchCriteria,
                matchProperties,
                methodName);

        try {
            this.validateTypeGUID(userId, repositoryName, guidParameterName, relationshipTypeGUID, methodName);
        } catch (TypeErrorException e) {
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("unknown", relationshipTypeGUID, guidParameterName, methodName, repositoryName, "unknown");

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Perform operation
         */
        // This method requires a historic query which is not supported
        LOG.debug("findRelationshipsByProperty: Does not support asOfTime historic retrieval");
        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        String errorMessage = errorCode.getErrorMessageId()
                + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

        throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }

    /**
     * Return a list of relationships whose string based property values match the search criteria.  The
     * search criteria may include regex style wild cards.
     *
     * @param userId - unique identifier for requesting user.
     * @param relationshipTypeGUID - GUID of the type of entity to search for. Null means all types will
     *                       be searched (could be slow so not recommended).
     * @param searchCriteria - String expression contained in any of the property values within the entities
     *                       of the supplied type.
     * @param fromRelationshipElement - Element number of the results to skip to when building the results list
     *                                to return.  Zero means begin at the start of the results.  This is used
     *                                to retrieve the results over a number of pages.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime - Requests a historical query of the relationships for the entity.  Null means return the
     *                 present values.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result relationships that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of relationships.  Null means no matching relationships.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws PropertyErrorException - there is a problem with one of the other parameters.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException - the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<Relationship> findRelationshipsByPropertyValue(String                    userId,
                                                               String                    relationshipTypeGUID,
                                                               String                    searchCriteria,
                                                               int                       fromRelationshipElement,
                                                               List<InstanceStatus>      limitResultsByStatus,
                                                               Date                      asOfTime,
                                                               String                    sequencingProperty,
                                                               SequencingOrder           sequencingOrder,
                                                               int                       pageSize)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {
        final String  methodName = "findRelationshipsByPropertyValue";
        final String  asOfTimeParameter = "asOfTime";
        final String  pageSizeParameter = "pageSize";
        final String  typeGUIDParameter = "relationshipTypeGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);


        try {
            this.validateTypeGUID(userId, repositoryName, typeGUIDParameter, relationshipTypeGUID, methodName);
        } catch (TypeErrorException e) {
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage( "unknown", relationshipTypeGUID, typeGUIDParameter, methodName, repositoryName, "unknown");

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Perform operation
         */
        // This method requires a historic query which is not supported
        LOG.debug("findRelationshipsByPropertyValue: Does not support asOfTime historic retrieval");
        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
        String errorMessage = errorCode.getErrorMessageId()
                + errorCode.getFormattedErrorMessage(asOfTimeParameter, methodName, metadataCollectionId);

        throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    /**
     * Return all of the relationships and intermediate entities that connect the startEntity with the endEntity.
     *
     * @param userId               - unique identifier for requesting user.
     * @param startEntityGUID      - The entity that is used to anchor the query.
     * @param endEntityGUID        - the other entity that defines the scope of the query.
     * @param limitResultsByStatus - By default, relationships in all statuses are returned.  However, it is possible
     *                               to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                               status values.
     * @param asOfTime             - Requests a historical query of the relationships for the entity.  Null means return the
     *                               present values.
     * @return InstanceGraph - the sub-graph that represents the returned linked entities and their relationships.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by either the startEntityGUID or the endEntityGUID
     *                                      is not found in the metadata collection.
     * @throws PropertyErrorException     - there is a problem with one of the other parameters.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public InstanceGraph getLinkingEntities(String               userId,
                                            String               startEntityGUID,
                                            String               endEntityGUID,
                                            List<InstanceStatus> limitResultsByStatus,
                                            Date                 asOfTime)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String methodName                   = "getLinkingEntities";
        final String startEntityGUIDParameterName = "startEntityGUID";
        final String endEntityGUIDParameterName   = "entityGUID";
        final String asOfTimeParameter            = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, startEntityGUIDParameterName, startEntityGUID, methodName);
        repositoryValidator.validateGUID(repositoryName, endEntityGUIDParameterName, endEntityGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        /*
         * Perform operation
         */

        // TODO!! Need to investigate how to implement this method in Atlas
        // To do this efficiently requires a gremlin traversal - it may be possible to do this in Atlas (internally).
        // To achieve this by other means would be inefficient.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    /**
     * Return the entities and relationships that radiate out from the supplied entity GUID.
     * The results are scoped both the instance type guids and the level.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param entityGUID                   - the starting point of the query.
     * @param entityTypeGUIDs              - list of entity types to include in the query results.  Null means include
     *                                     all entities found, irrespective of their type.
     * @param relationshipTypeGUIDs        - list of relationship types to include in the query results.  Null means include
     *                                     all relationships found, irrespective of their type.
     * @param limitResultsByStatus         - By default, relationships in all statuses are returned.  However, it is possible
     *                                     to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                     status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime                     - Requests a historical query of the relationships for the entity.  Null means return the
     *                                     present values.
     * @param level                        - the number of the relationships out from the starting entity that the query will traverse to
     *                                     gather results.
     * @return InstanceGraph - the sub-graph that represents the returned linked entities and their relationships.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException         - one or more of the type guids passed on the request is not known by the
     *                                    metadata collection.
     * @throws EntityNotKnownException    - the entity identified by the entityGUID is not found in the metadata collection.
     * @throws PropertyErrorException     - there is a problem with one of the other parameters.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public InstanceGraph getEntityNeighborhood(String               userId,
                                               String               entityGUID,
                                               List<String>         entityTypeGUIDs,
                                               List<String>         relationshipTypeGUIDs,
                                               List<InstanceStatus> limitResultsByStatus,
                                               List<String>         limitResultsByClassification,
                                               Date                 asOfTime,
                                               int                  level)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String methodName                                  = "getEntityNeighborhood";
        final String entityGUIDParameterName                     = "entityGUID";
        final String entityTypeGUIDParameterName                 = "entityTypeGUIDs";
        final String relationshipTypeGUIDParameterName           = "relationshipTypeGUIDs";
        final String limitedResultsByClassificationParameterName = "limitResultsByClassification";
        final String asOfTimeParameter                           = "asOfTime";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);

        if (entityTypeGUIDs != null)
        {
            for (String guid : entityTypeGUIDs)
            {
                this.validateTypeGUID(userId, repositoryName, entityTypeGUIDParameterName, guid, methodName);
            }
        }

        if (relationshipTypeGUIDs != null)
        {
            for (String guid : relationshipTypeGUIDs)
            {
                this.validateTypeGUID(userId, repositoryName, relationshipTypeGUIDParameterName, guid, methodName);
            }
        }

        if (limitResultsByClassification != null)
        {
            for (String classificationName : limitResultsByClassification)
            {
                repositoryValidator.validateClassificationName(repositoryName,
                        limitedResultsByClassificationParameterName,
                        classificationName,
                        methodName);
            }
        }

        /*
         * Perform operation
         */
        // TODO - Need to investigate how to implement this method in Atlas
        // This should be implemented by a gremlin traversal internally within Atlas.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    /**
     * Return the list of entities that are of the types listed in instanceTypes and are connected, either directly or
     * indirectly to the entity identified by startEntityGUID.
     *
     * @param userId                       - unique identifier for requesting user.
     * @param startEntityGUID              - unique identifier of the starting entity.
     * @param instanceTypes                - list of types to search for.  Null means an type.
     * @param fromEntityElement            - starting element for results list.  Used in paging.  Zero means first element.
     * @param limitResultsByStatus         - By default, relationships in all statuses are returned.  However, it is possible
     *                                     to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                                     status values.
     * @param limitResultsByClassification - List of classifications that must be present on all returned entities.
     * @param asOfTime                     - Requests a historical query of the relationships for the entity.  Null means return the
     *                                     present values.
     * @param sequencingProperty           - String name of the property that is to be used to sequence the results.
     *                                     Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder              - Enum defining how the results should be ordered.
     * @param pageSize                     - the maximum number of result entities that can be returned on this request.  Zero means
     *                                     unrestricted return results size.
     * @return list of entities either directly or indirectly connected to the start entity
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException         - the requested type is not known, or not supported in the metadata repository
     *                                    hosting the metadata collection.
     * @throws EntityNotKnownException    - the entity identified by the startEntityGUID
     *                                    is not found in the metadata collection.
     * @throws PropertyErrorException     - the sequencing property specified is not valid for any of the requested types of
     *                                    entity.
     * @throws PagingErrorException       - the paging/sequencing parameters are set up incorrectly.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public List<EntityDetail> getRelatedEntities(String               userId,
                                                 String               startEntityGUID,
                                                 List<String>         instanceTypes,
                                                 int                  fromEntityElement,
                                                 List<InstanceStatus> limitResultsByStatus,
                                                 List<String>         limitResultsByClassification,
                                                 Date                 asOfTime,
                                                 String               sequencingProperty,
                                                 SequencingOrder      sequencingOrder,
                                                 int                  pageSize)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            PagingErrorException,
            UserNotAuthorizedException
    {

        final String  methodName = "getRelatedEntities";
        final String  entityGUIDParameterName  = "startEntityGUID";
        final String  instanceTypesParameter = "instanceTypes";
        final String  asOfTimeParameter = "asOfTime";
        final String  pageSizeParameter = "pageSize";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, startEntityGUID, methodName);
        repositoryValidator.validateAsOfTime(repositoryName, asOfTimeParameter, asOfTime, methodName);
        repositoryValidator.validatePageSize(repositoryName, pageSizeParameter, pageSize, methodName);

        if (instanceTypes != null)
        {
            for (String guid : instanceTypes)
            {
                this.validateTypeGUID(userId, repositoryName, instanceTypesParameter, guid, methodName);
            }
        }

        /*
         * Perform operation
         */
        // TODO - Need to investigate how to implement this method in Atlas
        // This should be implemented by a gremlin traversal within Atlas

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName, this.getClass().getName(), repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    // ==========================================================================================================================

    // Group 4 methods

    /**
     * Create a new entity and put it in the requested state.  The new entity is returned.
     *
     * @param userId - unique identifier for requesting user.
     * @param entityTypeGUID - unique identifier (guid) for the new entity's type.
     * @param initialProperties - initial list of properties for the new entity - null means no properties.
     * @param initialClassifications - initial list of classifications for the new entity - null means no classifications.
     * @param initialStatus - initial status - typically DRAFT, PREPARED or ACTIVE.
     * @return EntityDetail showing the new header plus the requested properties and classifications.  The entity will
     * not have any relationships at this stage.
     * @throws InvalidParameterException - one of the parameters is invalid or null.
     * @throws RepositoryErrorException - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException - the requested type is not known, or not supported in the metadata repository
     *                              hosting the metadata collection.
     * @throws PropertyErrorException - one or more of the requested properties are not defined, or have different
     *                                  characteristics in the TypeDef for this entity's type.
     * @throws ClassificationErrorException - one or more of the requested classifications are either not known or
     *                                           not defined for this entity type.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                       the requested status.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail addEntity(String                     userId,
                                  String                     entityTypeGUID,
                                  InstanceProperties         initialProperties,
                                  List<Classification>       initialClassifications,
                                  InstanceStatus             initialStatus)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            ClassificationErrorException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {



        final String methodName = "addEntity";
        final String sourceName = metadataCollectionId;
        final String entityGUIDParameterName = "entityTypeGUID";
        final String propertiesParameterName = "initialProperties";
        final String classificationsParameterName = "initialClassifications";
        final String initialStatusParameterName = "initialStatus";

        if (LOG.isDebugEnabled())
            LOG.debug("==> {}: userId {} entityTypeGUID {} initialProperties {} initialClassifications {} initialStatus {} ",
                    methodName,userId,entityTypeGUID , initialProperties , initialClassifications, initialStatus );

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeGUID(repositoryName, entityGUIDParameterName, entityTypeGUID, methodName);

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, entityTypeGUID);
            if (typeDef != null) {
                repositoryValidator.validateTypeDefForInstance(repositoryName, entityGUIDParameterName, typeDef, methodName);
                repositoryValidator.validateClassificationList(repositoryName,
                        classificationsParameterName,
                        initialClassifications,
                        typeDef.getName(),
                        methodName);

                repositoryValidator.validatePropertiesForType(repositoryName,
                        propertiesParameterName,
                        typeDef,
                        initialProperties,
                        methodName);

                repositoryValidator.validateInstanceStatus(repositoryName,
                        initialStatusParameterName,
                        initialStatus,
                        typeDef,
                        methodName);
            }
        } catch (TypeDefNotKnownException e) {
            // Swallow and throw TypeErrorException below
            typeDef = null;
        }

        if (typeDef == null) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        /*
         * Validation complete - ok to create new instance
         */

        // Perform validation checks on type specified by guid.
        // Create an instance of the Entity, setting the createdBy to userId, createdTime to now, etc.
        // Then set the properties and initialStatus.
        // Create the entity in the Atlas repository.
        // Set the classifications

        // Note: There are two obvious ways to implement this method -
        // i) retrieve the typedef from Atlas into OM form, perform logic, construct Atlas objects and send to Atlas
        // ii) retrieve typedef from Atlas and use it directly to instantiate the entity.
        // This method uses the former approach, as it may result in common code with other methods.


        if (typeDef.getCategory() != ENTITY_DEF) {
            LOG.error("addEntity: Found typedef with guid {} but it is not an EntityDef", entityTypeGUID);
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, entityGUIDParameterName, methodName, repositoryName, "unknown");

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        EntityDef entityDef = (EntityDef) typeDef;

        // Retrieve the classification def for each of the initial classifications, and check they list the entity type as valid.
        if (initialClassifications != null) {
            for (Classification classification :  initialClassifications ) {
                // Retrieve the classification def and check it contains the entity type
                ClassificationDef classificationDef;
                try {
                    classificationDef = (ClassificationDef)_getTypeDefByName(userId, classification.getName());
                }
                catch (Exception e) {
                    LOG.error("addEntity: Could not find classification def with name {}", classification.getName(), e);

                    OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
                    String        errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(classification.getName(), "unknown", classificationsParameterName, methodName, repositoryName, "unknown");

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                List<TypeDefLink> validEntityDefs = classificationDef.getValidEntityDefs();
                if (validEntityDefs != null) {
                    // This assumes that if validEntityDefs is null then the classification can be attached (to anything).
                    boolean match = false;
                    for (TypeDefLink tDL : validEntityDefs) {
                        // compare the tDL with the entityTDL
                        if (entityDef.getGUID().equals(tDL.getGUID()) && entityDef.getName().equals(tDL.getName())) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        LOG.error("addEntity: Cannot classify entity type with classification {}", classification.getName());

                        OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                        String        errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(classification.getName(), classificationsParameterName, methodName, repositoryName);

                        throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                    }
                }
            }
        }


        // Collate the valid instance properties
        EntityDefMapper entityDefMapper = new EntityDefMapper(this, userId, entityDef);
        ArrayList<String> validInstanceProperties = entityDefMapper.getValidPropertyNames();

        // Create an instance type
        // An OM EntityDef only has one superType - so retrieve it and wrap into a list of length one...
        ArrayList<TypeDefLink> listSuperTypes = new ArrayList<>();
        listSuperTypes.add(entityDef.getSuperType());
        InstanceType instanceType = new InstanceType(entityDef.getCategory(),
                                                     entityTypeGUID,
                                                     entityDef.getName(),
                                                     entityDef.getVersion(),
                                                     entityDef.getDescription(),
                                                     entityDef.getDescriptionGUID(),
                                                     listSuperTypes,
                                                     entityDef.getValidInstanceStatusList(),
                                                     validInstanceProperties);

        // Construct an EntityDetail object
        Date now = new Date();
        EntityDetail entityDetail = new EntityDetail();
        // Set fields from InstanceAuditHeader
        entityDetail.setType(instanceType);
        entityDetail.setCreatedBy(userId);
        entityDetail.setCreateTime(now);
        entityDetail.setUpdatedBy(userId);
        entityDetail.setUpdateTime(now);
        entityDetail.setVersion(1L);
        entityDetail.setStatus(InstanceStatus.ACTIVE);
        // Set fields from InstanceHeader
        entityDetail.setMetadataCollectionId(metadataCollectionId);
        // GUID is not set till after the create by Atlas
        entityDetail.setGUID(null);
        entityDetail.setInstanceURL(null);
        // Set fields from EntitySummary
        // Set the classifications
        entityDetail.setClassifications(initialClassifications);
        // Set fields from EntityDetail
        // Set the properties
        entityDetail.setProperties(initialProperties);

        // Add the Entity to the AtlasEntityStore...

        /*
         * Construct an AtlasEntity
         * Let the AtlasEntity constructor set the GUID - it will be initialized
         * to nextInternalId and Atlas will generate
         */
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setTypeName(typeDef.getName());

        atlasEntity.setStatus(AtlasEntity.Status.ACTIVE);
        atlasEntity.setCreatedBy(entityDetail.getCreatedBy());
        atlasEntity.setUpdatedBy(entityDetail.getUpdatedBy());
        atlasEntity.setCreateTime(entityDetail.getCreateTime());
        atlasEntity.setUpdateTime(entityDetail.getUpdateTime());
        atlasEntity.setVersion(entityDetail.getVersion());

        // Cannot set classifications yet - need to do that post-create to get the entity GUID

        // Map attributes from OM EntityDetail to AtlasEntity
        InstanceProperties instanceProperties = entityDetail.getProperties();
        Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
        atlasEntity.setAttributes(atlasAttrs);

        // AtlasEntity should have been fully constructed by this point
        LOG.debug("addEntity: atlasEntity to create is {}", atlasEntity);

        String newEntityGuid;
        // Construct an AtlasEntityStream and call the repository
        //
        // Because we want to retain the value of the isProxy  flag and other system attributes, we need to
        // use an EntityImportStream.
        // The CTOR for this takes an AtlasEntityWithExtInfo entityWithExtInfo & an EntityStream entityStream
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWEI = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
        AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityWEI, null);
        EntityMutationResponse emr;
        try {
            emr = entityStore.createOrUpdateForImport(eis);
        }
        catch (AtlasBaseException e) {

            LOG.error("addEntity: Caught exception from Atlas {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("entity", "entity parameters", methodName, sourceName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // Interpret the EMR (EntityMutationResponse) and fill in any extra detail in EntityDetail - e.g. sys attrs
        // The EMR provides the following:
        //  Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities;
        //  Map<String, String>                           guidAssignments;
        // Check that our Entity is in the createdEntities
        if (emr != null) {
            List<AtlasEntityHeader> atlasEntityHeaders = emr.getCreatedEntities();
            if (atlasEntityHeaders == null || atlasEntityHeaders.size() != 1) {
                int numReturned = 0;
                if (atlasEntityHeaders == null) {
                    LOG.error("addEntity: Expected Atlas to return exactly one created entity, but returned null");
                } else {
                    numReturned = atlasEntityHeaders.size();
                    LOG.error("addEntity: Expected Atlas to return exactly one created entity, but returned {}", numReturned);
                }
                // We have more or less entities than we expected
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("entity", "entity createOrUpdate", methodName, sourceName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            } else {
                // Verify that the AEH contains what we would expect..
                // String                    guid                => should be set to allocated GUID - we will copy this to EntityDetail
                // AtlasEntity.Status        status              => check set to AtlasEntity.Status.ACTIVE;
                // String                    displayText         => ignored
                // List<String>              classificationNames => ignored - classifications are set below
                // List<AtlasClassification> classifications     => ignored - classifications are set below
                AtlasEntityHeader aeh = atlasEntityHeaders.get(0);
                if (aeh.getStatus() != AtlasEntity.Status.ACTIVE) {
                    LOG.error("addEntity: Atlas created entity, but status set to {}", aeh.getStatus());

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage("entity", "entity createOrUpdate", methodName, sourceName);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                } else {
                    // Note the newly allocated entity GUID - we will need it in multiple places below
                    newEntityGuid = aeh.getGuid();
                    LOG.debug("newEntityGuid = {}", newEntityGuid);
                    entityDetail.setGUID(newEntityGuid);
                }
            }
        } else {
            LOG.error("addEntity: Atlas create of entity returned null");

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("null", "entityMutationResponse", methodName, sourceName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }


        /*
         * Call Atlas to set classifications..
         *
         *  You can't set the classifications on the AtlasEntity until after it has been
         *  created, as each AtlasClassification needs the entity GUID. So add the classifications post-create.
         *  AtlasEntity needs a List<AtlasClassification> so we need to do some translation
         *
         *  OM Classification has:
         *  String               classificationName
         *  InstanceProperties   classificationProperties
         *  ClassificationOrigin classificationOrigin
         *  String               classificationOriginGUID
         *
         *  AtlasClassification has:
         *  String              entityGuid
         *  boolean             propagate
         *  List<TimeBoundary>  validityPeriods
         *  String              typeName
         *  Map<String, Object> attributes
         */


        if (entityDetail.getClassifications() != null && entityDetail.getClassifications().size() > 0) {
            ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();
            for (Classification omClassification : entityDetail.getClassifications()) {
                AtlasClassification atlasClassification = new AtlasClassification(omClassification.getName());
                /*
                 * For this OM classification build an Atlas equivalent...
                 * For now we are always setting propagatable to true and AtlasClassification has propagate=true by default.
                 * Instead this could traverse to the Classification.InstanceType.typeDefGUID and retrieve the ClassificationDef
                 * to find the value of propagatable on the def.
                 */
                atlasClassification.setTypeName(omClassification.getType().getTypeDefName());
                atlasClassification.setEntityGuid(newEntityGuid);

                /* OM Classification has InstanceProperties of form Map<String, InstancePropertyValue>  instanceProperties
                 */
                InstanceProperties classificationProperties = omClassification.getProperties();
                Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
                atlasClassification.setAttributes(atlasClassificationAttrs);

                atlasClassifications.add(atlasClassification);
            }
            /* We do not need to augment the AtlasEntity we created earlier - we can just use the
             * atlasClassifications directly with the following repository call...
             */
            try {

                entityStore.addClassifications(newEntityGuid, atlasClassifications);

            } catch (AtlasBaseException e) {

                // Failed to add classifications to the entity
                LOG.error("addEntity: Atlas created entity, but we could not add classifications to it, guid {}", newEntityGuid);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        if (LOG.isDebugEnabled())
            LOG.debug("<== {}: entityDetail {} ", methodName, entityDetail);

        return entityDetail;
    }


    /**
     * Create an entity proxy in the metadata collection.  This is used to store relationships that span metadata
     * repositories.
     *
     * @param userId                         - unique identifier for requesting user.
     * @param entityProxy                    - details of entity to add.
     * @throws InvalidParameterException     - the entity proxy is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws FunctionNotSupportedException - the repository does not support entity proxies as first class elements.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public void addEntityProxy(String      userId,
                               EntityProxy entityProxy)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {
        final String  methodName         = "addEntityProxy";
        final String  proxyParameterName = "entityProxy";


        if (LOG.isDebugEnabled())
            LOG.debug("==> {}: userId {} entityProxy {} ", methodName, userId, entityProxy );

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);

        repositoryValidator.validateEntityProxy(repositoryName,
                metadataCollectionId,
                proxyParameterName,
                entityProxy,
                methodName);

        /*
         * Also validate that the proxy contains a metadataCollectionId (i.e. it is not null)
         */
        if (entityProxy.getMetadataCollectionId() == null) {
            LOG.error("addEntityProxy: Must contain a non-null metadataCollectionId identifying the home repository");

            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_ENTITY_PROXY;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                    metadataCollectionId,
                    proxyParameterName,
                    methodName);

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Validation complete
         */

        /*
         * An EntityProxy is stored in Atlas as a normal AtlasEntity but with a reduced
         * level of detail (the other fields of EntityDetail are not available). In order
         * to identify that the created AtlasEntity is a proxy, the isProxy system attribute
         * is set.
         */

        /*
         * Check that the EntityProxy contains everything we expect...
         */
        Date now = new Date();
        entityProxy.setCreatedBy(userId);
        entityProxy.setCreateTime(now);
        entityProxy.setUpdatedBy(userId);
        entityProxy.setUpdateTime(now);
        entityProxy.setVersion(1L);
        entityProxy.setStatus(InstanceStatus.ACTIVE);


        // Add the EntityProxy to the AtlasEntityStore...


        /*
         * Construct an AtlasEntity
         * The AtlasEntity constructor will set the GUID - but overwrite it with the GUID from the proxy
         * Also set the homeId and the isProxy flag.
         */
        AtlasEntity atlasEntity = new AtlasEntity();
        atlasEntity.setIsProxy(true);
        atlasEntity.setGuid(entityProxy.getGUID());
        atlasEntity.setHomeId(entityProxy.getMetadataCollectionId());

        atlasEntity.setTypeName(entityProxy.getType().getTypeDefName());

        atlasEntity.setStatus(AtlasEntity.Status.ACTIVE);
        atlasEntity.setCreatedBy(entityProxy.getCreatedBy());
        atlasEntity.setUpdatedBy(entityProxy.getUpdatedBy());
        atlasEntity.setCreateTime(entityProxy.getCreateTime());
        atlasEntity.setUpdateTime(entityProxy.getUpdateTime());
        atlasEntity.setVersion(entityProxy.getVersion());

        // Cannot set classifications yet - need to do that post-create to get the entity GUID

        // Map attributes from OM EntityProxy to AtlasEntity
        InstanceProperties instanceProperties = entityProxy.getUniqueProperties();
        Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
        atlasEntity.setAttributes(atlasAttrs);

        // AtlasEntity should have been fully constructed by this point
        LOG.debug("addEntityProxy: atlasEntity to create is {}", atlasEntity);

        // Construct an AtlasEntityStream and call the repository
        // Because we want to impose the GUID (e.g. RID) that has been supplied in the EntityProxy, we
        // need to ask Atlas to accept the entity with existing GUID. Therefore we must use an EntityImportStream.
        // The CTOR for this takes an AtlasEntityWithExtInfo entityWithExtInfo & an EntityStream entityStream
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWEI = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
        AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityWEI, null);
        EntityMutationResponse emr;
        try {
            emr = entityStore.createOrUpdateForImport(eis);

        } catch (AtlasBaseException e) {
            LOG.error("addEntityProxy: Caught exception trying to create entity proxy, {} ", e);
            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                    atlasEntity.toString(), methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        LOG.debug("addEntityProxy: response from entityStore {}", emr);

        /*
         * Call Atlas to set classifications..
         *
         *  You can't set the classifications on the AtlasEntity until after it has been
         *  created, as each AtlasClassification needs the entity GUID. So add the classifications post-create.
         *  AtlasEntity needs a List<AtlasClassification> so we need to do some translation
         *
         *  OM Classification has:
         *  String               classificationName
         *  InstanceProperties   classificationProperties
         *  ClassificationOrigin classificationOrigin
         *  String               classificationOriginGUID
         *
         *  AtlasClassification has:
         *  String              entityGuid
         *  boolean             propagate
         *  List<TimeBoundary>  validityPeriods
         *  String              typeName
         *  Map<String, Object> attributes
         */


        if (entityProxy.getClassifications() != null && entityProxy.getClassifications().size() > 0) {
            ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();
            for (Classification omClassification : entityProxy.getClassifications()) {
                AtlasClassification atlasClassification = new AtlasClassification(omClassification.getName());
                /*
                 * For this OM classification build an Atlas equivalent...
                 * For now we are always setting propagatable to true and AtlasClassification has propagate=true by default.
                 * Instead this could traverse to the Classification.InstanceType.typeDefGUID and retrieve the ClassificationDef
                 * to find the value of propagatable on the def.
                 */
                atlasClassification.setTypeName(omClassification.getType().getTypeDefName());
                atlasClassification.setEntityGuid(entityProxy.getGUID());

                /* OM Classification has InstanceProperties of form Map<String, InstancePropertyValue>  instanceProperties
                 */
                InstanceProperties classificationProperties = omClassification.getProperties();
                Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
                atlasClassification.setAttributes(atlasClassificationAttrs);

                atlasClassifications.add(atlasClassification);
            }
            /* We do not need to augment the AtlasEntity we created earlier - we can just use the
             * atlasClassifications directly with the following repository call...
             */
            try {

                entityStore.addClassifications(entityProxy.getGUID(), atlasClassifications);

            } catch (AtlasBaseException e) {

                // Failed to add classifications to the entity
                LOG.error("addEntityProxy: Atlas created entity, but we could not add classifications to it, guid {}", entityProxy.getGUID(), e);

                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }


        if (LOG.isDebugEnabled())
            LOG.debug("<== {}: entityProxy {} ", methodName, entityProxy);

    }


    /**
     * Update the status for a specific entity.
     *
     * @param userId     - unique identifier for requesting user.
     * @param entityGUID - unique identifier (guid) for the requested entity.
     * @param newStatus  - new InstanceStatus for the entity.
     * @return EntityDetail showing the current entity header, properties and classifications.
     * @throws InvalidParameterException   - one of the parameters is invalid or null.
     * @throws RepositoryErrorException    - there is a problem communicating with the metadata repository where
     *                                     the metadata collection is stored.
     * @throws EntityNotKnownException     - the entity identified by the guid is not found in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status.
     * @throws UserNotAuthorizedException  - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityStatus(String         userId,
                                           String         entityGUID,
                                           InstanceStatus newStatus)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {

        final String  methodName               = "updateEntityStatus";
        final String  entityGUIDParameterName  = "entityGUID";
        final String  statusParameterName      = "newStatus";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */
        EntityDetail entity  = getEntityDetail(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        repositoryValidator.validateInstanceType(repositoryName, entity);

        String entityTypeGUID = entity.getType().getTypeDefGUID();

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, entityTypeGUID);

            if (typeDef != null) {
                repositoryValidator.validateNewStatus(repositoryName, statusParameterName, newStatus, typeDef, methodName);
            }
        }
        catch (TypeDefNotKnownException e) {
            typeDef = null;
        }

        if (typeDef == null) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Validation complete - ok to make changes
         */

        /*
         * The above validation has already retrieved the entity from Atlas, by GUID and
         * converted it to an EntityDetail.
         */

        /*
         * Set the status to the requested value.
         */
        entity.setStatus(newStatus);

        /*
         * Convert to Atlas and store, retrieve and convert to returnable EntityDetail
         */
        try {
            AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
            LOG.debug("updateEntityStatus: atlasEntity to update is {}", atlasEntity);

            // Construct an AtlasEntityWithExtInfo and call the repository
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);

            entityStore.createOrUpdate(new AtlasEntityStream(atlasEntityToUpdate),true);

            // Retrieve the AtlasEntity - rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

            atlasEntWithExt = entityStore.getById(entityGUID);

            if (atlasEntWithExt == null) {
                LOG.error("updateEntityStatus: Could not find entity with guid {} ", entityGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();

            return returnEntityDetail;

        }
        catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

            LOG.error("updateEntityStatus: Caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        catch (AtlasBaseException e) {

            LOG.error("updateEntityStatus: Caught exception from Atlas {}", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Update selected properties in an entity.
     *
     * @param userId     - unique identifier for requesting user.
     * @param entityGUID - String unique identifier (guid) for the entity.
     * @param properties - a list of properties to change.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection
     * @throws PropertyErrorException     - one or more of the requested properties are not defined, or have different
     *                                      characteristics in the TypeDef for this entity's type
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityProperties(String             userId,
                                               String             entityGUID,
                                               InstanceProperties properties)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String  methodName = "updateEntityProperties";
        final String  entityGUIDParameterName  = "entityGUID";
        final String  propertiesParameterName  = "properties";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */
        EntityDetail entity  = getEntityDetail(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        repositoryValidator.validateInstanceType(repositoryName, entity);

        String entityTypeGUID = entity.getType().getTypeDefGUID();

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, entityTypeGUID);

            if (typeDef != null) {
                repositoryValidator.validateNewPropertiesForType(repositoryName,
                        propertiesParameterName,
                        typeDef,
                        properties,
                        methodName);
            }
        }
        catch (TypeDefNotKnownException e) {
            typeDef = null;
        }

        if (typeDef == null) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }



        /*
         * Validation complete - ok to make changes
         */

        /*
         * The above validation has already retrieved the entity from Atlas, by GUID and
         * converted it to an EntityDetail.
         */

        /*
         * Update the properties that need changing.
         */
        InstanceProperties currentProps = entity.getProperties();
        Iterator<String> newPropNames = properties == null ? null : properties.getPropertyNames();
        if (newPropNames != null) {
            while (newPropNames.hasNext()) {
                String name = newPropNames.next();
                InstancePropertyValue value = properties.getPropertyValue(name);
                currentProps.setProperty(name,value);
            }
        }
        entity.setProperties(currentProps);

        /*
         * Convert to Atlas and store, retrieve and convert to returnable EntityDetail
         */
        try {
            AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
            LOG.debug("updateEntityStatus: atlasEntity to update is {}", atlasEntity);

            // Construct an AtlasEntityWithExtInfo and call the repository
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
            entityStore.createOrUpdate(new AtlasEntityStream(atlasEntityToUpdate),true);

            // Retrieve the AtlasEntity. Rather than parsing the EMR returned by the store, just get the entity directly
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

            atlasEntWithExt = entityStore.getById(entityGUID);

            if (atlasEntWithExt == null) {
                LOG.error("updateEntityStatus: Could not find entity with guid {} ", entityGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();

            return returnEntityDetail;

        }
        catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

            LOG.error("updateEntityStatus: Caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        catch (AtlasBaseException e) {

            LOG.error("updateEntityStatus: Caught exception from Atlas {}", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Undo the last update to an entity and return the previous content.
     *
     * @param userId                      - unique identifier for requesting user.
     * @param entityGUID                  - String unique identifier (guid) for the entity.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail undoEntityUpdate(String userId,
                                         String entityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        final String  methodName = "undoEntityUpdate";
        final String  entityGUIDParameterName  = "entityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Validation complete - ok to restore entity
         */

        // In the Atlas connector the previous update is not persisted.

        // I do not known of a way in Atlas to retrieve an earlier previous version.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    /**
     * Delete an entity.  The entity is soft deleted.  This means it is still in the graph but it is no longer returned
     * on queries.  All relationships to the entity are also soft-deleted and will no longer be usable.
     * To completely eliminate the entity from the graph requires a call to the purgeEntity() method after the delete call.
     * The restoreEntity() method will switch an entity back to Active status to restore the entity to normal use.
     *
     * @param userId                         - unique identifier for requesting user.
     * @param typeDefGUID                    - unique identifier of the type of the entity to delete.
     * @param typeDefName                    - unique name of the type of the entity to delete.
     * @param obsoleteEntityGUID             - String unique identifier (guid) for the entity.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws EntityNotKnownException       - the entity identified by the guid is not found in the metadata collection.
     * @throws FunctionNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                         soft-deletes - use purgeEntity() to remove the entity permanently.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public EntityDetail deleteEntity(String userId,
                                     String typeDefGUID,
                                     String typeDefName,
                                     String obsoleteEntityGUID)

            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        // TODO - need to find a way to determine whether soft deletes are supported.

        final String  methodName               = "deleteEntity";
        final String  typeDefGUIDParameterName = "typeDefGUID";
        final String  typeDefNameParameterName = "typeDefName";
        final String  entityGUIDParameterName  = "obsoleteEntityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                typeDefGUIDParameterName,
                typeDefNameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, obsoleteEntityGUID, methodName);

        /*
         * Locate Entity
         */
        EntityDetail entity  = getEntityDetail(userId, obsoleteEntityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, obsoleteEntityGUID, entity, methodName);

        repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                typeDefGUID,
                typeDefName,
                entity,
                methodName);

        repositoryValidator.validateInstanceStatusForDelete(repositoryName, entity, methodName);

        /*
         * Complete the request
         */

        // One final piece of validation - if deletes are configured as HARD then throw not supported exception
        if (atlasDeleteConfiguration == AtlasDeleteOption.HARD) {
            LOG.error("deleteEntity: Repository is configured for hard deletes, cannot soft delete entity with guid {}",
                    obsoleteEntityGUID);
            LocalAtlasOMRSErrorCode errorCode    = LocalAtlasOMRSErrorCode.ATLAS_CONFIGURATION_HARD;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(obsoleteEntityGUID, methodName, repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /* From here on, this method is performing a SOFT delete.
         * The deleted entity is read back to read the updated the system attributes
         * so they can be set in the returned EntityDetail
         */

        try {
            entityStore.deleteById(obsoleteEntityGUID);

            /*
             * Read the entity back from the store - this should work if it was a soft delete.
             * By reading back, the status should reflect that it's been deleted and it should
             * have updated system attributes.
             *
             * Retrieve the AtlasEntity - rather than parsing the EMR just get the entity directly
             */
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

            atlasEntWithExt = entityStore.getById(obsoleteEntityGUID);

            if (atlasEntWithExt == null) {
                LOG.error("deleteEntity: Could not find entity with guid {} ", obsoleteEntityGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(obsoleteEntityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();

            return returnEntityDetail;

        }
        catch (TypeErrorException | InvalidEntityException e) {

            LOG.error("updateEntityStatus: Caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        catch (AtlasBaseException e) {
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Permanently removes a deleted entity from the metadata collection.  This request can not be undone.
     *
     * @param userId            - unique identifier for requesting user.
     * @param typeDefGUID       - unique identifier of the type of the entity to purge.
     * @param typeDefName       - unique name of the type of the entity to purge.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection
     * @throws EntityNotDeletedException  - the entity is not in DELETED status and so can not be purged
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeEntity(String userId,
                            String typeDefGUID,
                            String typeDefName,
                            String deletedEntityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            EntityNotDeletedException,
            UserNotAuthorizedException
    {

        final String  methodName               = "purgeEntity";
        final String  typeDefGUIDParameterName = "typeDefGUID";
        final String  typeDefNameParameterName = "typeDefName";
        final String  entityGUIDParameterName  = "deletedEntityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                typeDefGUIDParameterName,
                typeDefNameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, deletedEntityGUID, methodName);

        /*
         * Locate entity
         */
        EntityDetail entity  = getEntityDetail(userId, deletedEntityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, deletedEntityGUID, entity, methodName);

        repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                typeDefGUID,
                typeDefName,
                entity,
                methodName);


        /* Atlas has two configuration settings for its DeleteHandler - soft deletes or hard deletes.
         * If Atlas is configured for hard deletes, this purge method will remove the entity.
         * If Atlas is configured for soft deletes, the entity must have been (soft-)deleted prior to
         * this purgeEntity call. In this case, check that the entity is in DELETED state.
         *
         * This method does not use the repository's deleteById method - instead it uses the purgeById
         * method - which guarantees a permanent removal independent of how the repository is configured.
         */

        /*
         * If soft delete is enabled in the repository, check that the entity has been deleted.
         */
        if (atlasDeleteConfiguration == AtlasDeleteOption.SOFT) {
            LOG.debug("purgeEntity: Soft-delete is configured, check that entity to be purged is in DELETED state");
            repositoryValidator.validateEntityIsDeleted(repositoryName, entity, methodName);
        }


        /*
         * Validation is complete - ok to remove the entity
         */

        /*
         * From here on, this method performs a HARD delete.
         */

        try {

            LOG.debug("TODO!! add purgeById to entity store!!");
            //entityStore.purgeById(deletedEntityGUID); // TODO - waiting for ATLAS-2774

        }
        //catch (AtlasBaseException e) { // TODO - catch appropriate exceptions when ATLAS-2774 is available
        catch (Exception e) {
            LOG.error("purgeEntity: Caught exception from Atlas entityStore trying to purge entity {}", deletedEntityGUID, e);

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_DELETED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(deletedEntityGUID, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Restore the requested entity to the state it was before it was deleted.
     *
     * @param userId            - unique identifier for requesting user.
     * @param deletedEntityGUID - String unique identifier (guid) for the entity.
     * @return EntityDetail showing the restored entity header, properties and classifications.
     * @throws InvalidParameterException  - the guid is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection
     * @throws EntityNotDeletedException  - the entity is currently not in DELETED status and so it can not be restored
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail restoreEntity(String userId,
                                      String deletedEntityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            EntityNotDeletedException,
            UserNotAuthorizedException
    {

        final String  methodName              = "restoreEntity";
        final String  entityGUIDParameterName = "deletedEntityGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, deletedEntityGUID, methodName);

        /*
         * Locate entity
         */
        EntityDetail entity  = getEntityDetail(userId, deletedEntityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, deletedEntityGUID, entity, methodName);

        repositoryValidator.validateEntityIsDeleted(repositoryName, entity, methodName);

        /*
         * Validation is complete.  It is ok to restore the entity.
         */
        // Atlas has two configuration settings for its DeleteHandler - all soft deletes or all hard deletes.
        // We would require that soft deletes are in force and even then it is not clear how to implement
        // a restore from an earlier soft delete. There is no entity store operation for this.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }


    /**
     * Add the requested classification to a specific entity.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param entityGUID               - String unique identifier (guid) for the entity.
     * @param classificationName       - String name for the classification.
     * @param classificationProperties - list of properties to set in the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException    - one of the parameters is invalid or null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException      - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is either not known or not valid
     *                                      for the entity.
     * @throws PropertyErrorException       - one or more of the requested properties are not defined, or have different
     *                                      characteristics in the TypeDef for this classification type
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public EntityDetail classifyEntity(String             userId,
                                       String             entityGUID,
                                       String             classificationName,
                                       InstanceProperties classificationProperties)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            ClassificationErrorException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String  methodName                  = "classifyEntity";
        final String  entityGUIDParameterName     = "entityGUID";
        final String  classificationParameterName = "classificationName";
        final String  propertiesParameterName     = "classificationProperties";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */
        EntityDetail entity  = getEntityDetail(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        repositoryValidator.validateInstanceType(repositoryName, entity);

        InstanceType entityType = entity.getType();

        repositoryValidator.validateClassification(repositoryName,
                classificationParameterName,
                classificationName,
                entityType.getTypeDefName(),
                methodName);

        Classification newClassification;
        try
        {
            repositoryValidator.validateClassificationProperties(repositoryName,
                    classificationName,
                    propertiesParameterName,
                    classificationProperties,
                    methodName);

            /*
             * Validation complete - build the new classification
             */
            newClassification = repositoryHelper.getNewClassification(repositoryName,
                    userId,
                    classificationName,
                    entityType.getTypeDefName(),
                    ClassificationOrigin.ASSIGNED,
                    null,
                    classificationProperties);
        }
        catch (Throwable   error)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;

            throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    error.getMessage(),
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Validation complete - ok to update entity
         */


        /*
         * The classificationName parameter is actually the name of the ClassificationDef for the type of classification.
         * Locate the ClassificationDef (by name). Fail if not found.
         * Locate the AtlasEntity (by GUID).  Fail if not found.
         * Project the AtlasEntity to an OM Entity - classification eligibility is tested in terms of OM defs.
         * Check that the ClassificationDef lists the EntityDef as a validEntityDef. Fail if not.
         * Create an AtlasClassification.
         * Update the AtlasEntity with the AtlasClassification.
         * Store in the repo.
         * Retrieve the classified entity from the repo and convert to/return as an EntityDetail showing the new classification.
         */

        LOG.debug("classifyEntity: newClassification is {}", newClassification);

        // Use the classificationName to locate the ClassificationDef.
        // The parameter validation above performs some of this already, but we need to classify the Atlas entity
        // with an Atlas classification
        TypeDef typeDef;
        try {

            typeDef = _getTypeDefByName(userId, classificationName);

        } catch (TypeDefNotKnownException e) {
            // Handle below
            typeDef = null;
        }
        if (typeDef == null || typeDef.getCategory() != CLASSIFICATION_DEF) {
            LOG.error("classifyEntity: Could not find a classification def with name {}", classificationName);
            OMRSErrorCode errorCode    = OMRSErrorCode.NO_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(classificationName, "classificationName", methodName, repositoryName);

            throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        ClassificationDef classificationDef = (ClassificationDef) typeDef;


        // Retrieve the AtlasEntity
        AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;
        try {

            atlasEntWithExt = entityStore.getById(entityGUID);

        } catch (AtlasBaseException e) {
            // handle below
            atlasEntWithExt = null;
        }
        if (atlasEntWithExt == null) {
            LOG.error("classifyEntity: Could not find entity with guid {} ", entityGUID);
            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, "entityGUID", methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
        // Extract the entity
        AtlasEntity atlasEntity = atlasEntWithExt.getEntity();

        LOG.debug("classifyEntity: atlasEntity retrieved with classifications {}", atlasEntity.getClassifications());

        // Project AtlasEntity as an EntityDetail.
        EntityDetail entityDetail;
        try {
            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
            entityDetail = atlasEntityMapper.toEntityDetail();
        }
        catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

            LOG.error("classifyEntity: Caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, "entityGUID", methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Check that the entity type is listed in the VEDs for the classificationDef
        entityType = entityDetail.getType();
        String entityTypeName = entityType.getTypeDefName();
        List<TypeDefLink> validEntityDefs = classificationDef.getValidEntityDefs();
        LOG.debug("classifyEntity: classification def has validEntityDefs {}", validEntityDefs);
        if (validEntityDefs != null) {
            boolean match = false;
            for (TypeDefLink vED : validEntityDefs) {
                // check whether this validEntityDef matches the entity type or any of its supertypes...
                if ( vED.getName().equals(entityTypeName) ) {
                    match = true;
                    break;
                }
                // If not an immediate match check the supertypes
                for ( TypeDefLink superType : entityType.getTypeDefSuperTypes() ) {
                    if ( vED.getName().equals(superType.getName()) ) {
                        match = true;
                        break;
                    }
                }
            }
            if (!match) {
                // Entity is not of a type that is eligible for the classification...reject
                LOG.error("classifyEntity: Classification cannot be applied to entity of type {}", entityTypeName);
                OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityTypeName, "entityTypeName", methodName, repositoryName);

                throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        // Create an AtlasClassification.
        AtlasClassification atlasClassification = new AtlasClassification();
        atlasClassification.setEntityGuid(entityGUID);
        atlasClassification.setTypeName(classificationName);

        // Map classification properties to classification attributes
        if (classificationProperties != null) {
            Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
            atlasClassification.setAttributes(atlasClassificationAttrs);
        }

        // Add the AtlasClassification to the AtlasEntity

        try {
            // method accepts a list of entity GUIDS
            List<String> entityGUIDList = new ArrayList<>();
            entityGUIDList.add(entityGUID);
            entityStore.addClassification(entityGUIDList, atlasClassification);

            /* Retrieve the AtlasEntity back from store to pick up any Atlas modifications.
             * Rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
             */
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityExtRetrieved;

            atlasEntityExtRetrieved = entityStore.getById(entityGUID);

            if (atlasEntityExtRetrieved == null) {
                LOG.error("classifyEntity: Could not find entity with guid {} ", entityGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntityRetrieved contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntityExtRetrieved.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();

            // Return OM EntityDetail which should now have the new classification.
            LOG.debug("classifyEntity: om entity {}", returnEntityDetail);
            return returnEntityDetail;

        } catch (Exception e) {
            LOG.error("classifyEntity: caught exception {}", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Remove a specific classification from an entity.
     *
     * @param userId             - unique identifier for requesting user.
     * @param entityGUID         - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException    - one of the parameters is invalid or null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException      - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is not set on the entity.
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public EntityDetail declassifyEntity(String userId,
                                         String entityGUID,
                                         String classificationName)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            ClassificationErrorException,
            UserNotAuthorizedException
    {

        final String  methodName                  = "declassifyEntity";
        final String  entityGUIDParameterName     = "entityGUID";
        final String  classificationParameterName = "classificationName";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
        repositoryValidator.validateClassificationName(repositoryName,
                classificationParameterName,
                classificationName,
                methodName);

        /*
         * Locate entity
         */
        EntityDetail entity = getEntityDetail(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Process the request
         */


        /*
         * The classificationName parameter is actually the name of the ClassificationDef for the type of classification.
         * Locate the ClassificationDef (by name). Fail if not found.
         * Retrieve the AtlasEntity (by GUID).  Fail if not found.
         * Update the AtlasEntity by removing the AtlasClassification.
         * Store in the repo.
         * Retrieve the de-classified entity from the repository.
         * Convert to/return an EntityDetail showing the revised classifications.
         */

        // Retrieve the AtlasEntity
        AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;
        try {

            atlasEntWithExt = entityStore.getById(entityGUID);

        }
        catch (AtlasBaseException e) {

            LOG.error("declassifyEntity: Caught exception from Atlas {}", e);
            // handle below
            atlasEntWithExt = null;
        }

        if (atlasEntWithExt == null) {

            LOG.error("declassifyEntity: Could not find entity with guid {} ", entityGUID);
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // atlasEntWithExt contains an AtlasEntity (entity)
        // Extract the entity
        AtlasEntity atlasEntity = atlasEntWithExt.getEntity();

        LOG.debug("declassifyEntity: atlasEntity retrieved with classifications {}", atlasEntity.getClassifications());

        // Remove the AtlasClassification from the AtlasEntity


        AtlasEntity.AtlasEntityWithExtInfo atlasEntityExtRetrieved;

        try {

            // entityStore.createOrUpdate(new AtlasEntityStream(atlasEntityWEI), false, true);

            entityStore.deleteClassification(entityGUID, classificationName);

            /* Retrieve the AtlasEntity back from store to pick up any Atlas modifications.
             * Rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
             */

            atlasEntityExtRetrieved = entityStore.getById(entityGUID);

        }
        catch (AtlasBaseException e) {
            LOG.error("declassifyEntity: Caught exception from Atlas {}", e);
            // handle below
            atlasEntityExtRetrieved = null;

        }

        if (atlasEntityExtRetrieved == null) {

            LOG.error("declassifyEntity: Could not update or find entity with guid {} ", entityGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // atlasEntityRetrieved contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
        // Extract the entity
        AtlasEntity atlasEntityRetrieved = atlasEntityExtRetrieved.getEntity();

        // Convert AtlasEntity to OM EntityDetail.
        EntityDetail returnEntityDetail;

        try {
            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();
        }
        catch (TypeErrorException | InvalidEntityException e) {

            LOG.error("declassifyEntity: Could not convert Atlas entity with guid {} ", entityGUID, e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Return OM EntityDetail which should now have the new classification.
        LOG.debug("declassifyEntity: om entity {}", returnEntityDetail);
        return returnEntityDetail;



    }


    /**
     * Update one or more properties in one of an entity's classifications.
     *
     * @param userId             - unique identifier for requesting user.
     * @param entityGUID         - String unique identifier (guid) for the entity.
     * @param classificationName - String name for the classification.
     * @param properties         - list of properties for the classification.
     * @return EntityDetail showing the resulting entity header, properties and classifications.
     * @throws InvalidParameterException    - one of the parameters is invalid or null.
     * @throws RepositoryErrorException     - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException      - the entity identified by the guid is not found in the metadata collection
     * @throws ClassificationErrorException - the requested classification is not attached to the classification.
     * @throws PropertyErrorException       - one or more of the requested properties are not defined, or have different
     *                                      characteristics in the TypeDef for this classification type
     * @throws UserNotAuthorizedException   - the userId is not permitted to perform this operation.
     */
    public EntityDetail updateEntityClassification(String             userId,
                                                   String             entityGUID,
                                                   String             classificationName,
                                                   InstanceProperties properties)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            ClassificationErrorException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String  methodName = "updateEntityClassification";
        final String  sourceName = metadataCollectionId;
        final String  entityGUIDParameterName     = "entityGUID";
        final String  classificationParameterName = "classificationName";
        final String  propertiesParameterName = "properties";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityGUIDParameterName, entityGUID, methodName);
        repositoryValidator.validateClassificationName(repositoryName, classificationParameterName, classificationName, methodName);


        try
        {
            repositoryValidator.validateClassificationProperties(repositoryName,
                    classificationName,
                    propertiesParameterName,
                    properties,
                    methodName);
        }
        catch (PropertyErrorException  error)
        {
            throw error;
        }
        catch (Throwable   error)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.UNKNOWN_CLASSIFICATION;

            throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    error.getMessage(),
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        /*
         * Locate entity
         */
        EntityDetail entity = getEntityDetail(userId,entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);


        /*
         * Process the request
         */

        /*
         * The classificationName parameter is the name of the ClassificationDef for the type of classification.
         * This has already been validated above.
         * Retrieve the AtlasEntity (by GUID).  Fail if not found.
         * Retrieve the classifications, locate the one to update and update its properties
         * Store in the repo.
         * Retrieve the de-classified entity from the repository.
         * Convert to/return an EntityDetail showing the revised classifications.
         */


        // Retrieve the AtlasEntity
        AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;
        try {

            atlasEntWithExt = entityStore.getById(entityGUID);

        } catch (AtlasBaseException e) {

            LOG.error("updateEntityClassification: Caught exception from Atlas {}", e);
            // handle below
            atlasEntWithExt = null;
        }

        if (atlasEntWithExt == null) {

            LOG.error("updateEntityClassification: Could not find entity with guid {} ", entityGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // atlasEntWithExt contains an AtlasEntity (entity). Extract the entity
        AtlasEntity atlasEntity = atlasEntWithExt.getEntity();

        LOG.debug("updateEntityClassification: atlasEntity retrieved with classifications {}", atlasEntity.getClassifications());

        // Project AtlasEntity as an EntityDetail.
        EntityDetail entityDetail;
        try {
            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
            entityDetail = atlasEntityMapper.toEntityDetail();
        }
        catch (TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

            LOG.error("updateEntityClassification: Caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, sourceName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }


        // Update the entityDetail's classifications

        // Locate the Classification to update
        List<Classification> preClassifications = entityDetail.getClassifications();
        // Find the classification to remove...
        if (preClassifications == null) {

            LOG.error("updateEntityClassification: Entity with guid {} has no classifications", entityGUID);

            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_CLASSIFIED;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new ClassificationErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Allow loop to match multiple entries, just in case duplicates exist
        for (Classification preClassification : preClassifications){
            if (preClassification.getName().equals(classificationName)) {
                // Found the classification to update
                LOG.debug("updateEntityClassification: classification {} previously had properties {}", classificationName, preClassification.getProperties());
                // Replace all of the classification's properties with the new InstanceProperties
                preClassification.setProperties(properties);
                LOG.debug("updateEntityClassification: classification {} now has properties {}", classificationName, preClassification.getProperties());
            }
        }

        entityDetail.setClassifications(preClassifications);


        // Construct an atlasClassifications list - based on the revised OM classifications above - that we can pass to the Atlas EntityStore.
        // There is no public Atlas method to update the props of one classification, so this code replaces them all.
        ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();

        // For each classification in the entity detail create an Atlas classification and add it to the list...
        for (Classification c : entityDetail.getClassifications() ) {
            // Create an AtlasClassification.
            AtlasClassification atlasClassification = new AtlasClassification();
            atlasClassification.setEntityGuid(entityGUID);
            atlasClassification.setTypeName(c.getName());
            // Map classification properties to classification attributes
            if (c.getProperties() != null) {
                Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(c.getProperties());
                atlasClassification.setAttributes(atlasClassificationAttrs);
            }
            atlasClassifications.add(atlasClassification);
        }


        LOG.debug("updateEntityClassification: atlasEntity to be updated with classifications {}", atlasEntity.getClassifications());


        AtlasEntity.AtlasEntityWithExtInfo atlasEntityExtRetrieved;
        try {

            // entityStore.createOrUpdate(new AtlasEntityStream(atlasEntityWEI), false, true);
            entityStore.updateClassifications(entityGUID, atlasClassifications);

            /* Retrieve the AtlasEntity back from store to pick up any Atlas modifications.
             * Rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
             */

            atlasEntityExtRetrieved = entityStore.getById(entityGUID);
        }
        catch (AtlasBaseException e) {
            LOG.error("updateEntityClassification: Caught exception from Atlas {}", e);
            // handle below
            atlasEntityExtRetrieved = null;
        }

        if (atlasEntityExtRetrieved == null) {

            LOG.error("updateEntityClassification: Could not find entity with guid {} ", entityGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // atlasEntityRetrieved contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
        // Extract the entity
        AtlasEntity atlasEntityRetrieved = atlasEntityExtRetrieved.getEntity();

        // Convert AtlasEntity to OM EntityDetail.
        EntityDetail returnEntityDetail;

        try {
            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();
        }
        catch (TypeErrorException | InvalidEntityException e) {

            LOG.error("updateEntityClassification: Caught exception from AtlasEntityMapper, entity guid {}, {} ", entityGUID, e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityGUIDParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Return OM EntityDetail which should now have the new classification.
        LOG.debug("updateEntityClassification: om entity {}", returnEntityDetail);
        return returnEntityDetail;

    }


    /**
     * Add a new relationship between two entities to the metadata collection.
     *
     * @param userId               - unique identifier for requesting user.
     * @param relationshipTypeGUID - unique identifier (guid) for the new relationship's type.
     * @param initialProperties    - initial list of properties for the new entity - null means no properties.
     * @param entityOneGUID        - the unique identifier of one of the entities that the relationship is connecting together.
     * @param entityTwoGUID        - the unique identifier of the other entity that the relationship is connecting together.
     * @param initialStatus        - initial status - typically DRAFT, PREPARED or ACTIVE.
     * @return Relationship structure with the new header, requested entities and properties.
     * @throws InvalidParameterException   - one of the parameters is invalid or null.
     * @throws RepositoryErrorException    - there is a problem communicating with the metadata repository where
     *                                     the metadata collection is stored.
     * @throws TypeErrorException          - the requested type is not known, or not supported in the metadata repository
     *                                     hosting the metadata collection.
     * @throws PropertyErrorException      - one or more of the requested properties are not defined, or have different
     *                                     characteristics in the TypeDef for this relationship's type.
     * @throws EntityNotKnownException     - one of the requested entities is not known in the metadata collection.
     * @throws StatusNotSupportedException - the metadata repository hosting the metadata collection does not support
     *                                     the requested status.
     * @throws UserNotAuthorizedException  - the userId is not permitted to perform this operation.
     */
    public Relationship addRelationship(String             userId,
                                        String             relationshipTypeGUID,
                                        InstanceProperties initialProperties,
                                        String             entityOneGUID,
                                        String             entityTwoGUID,
                                        InstanceStatus     initialStatus)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            EntityNotKnownException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {

        final String  methodName = "addRelationship";
        final String  guidParameterName = "relationshipTypeGUID";
        final String  propertiesParameterName       = "initialProperties";
        final String  initialStatusParameterName    = "initialStatus";
        final String  entityOneGUIDParameterName    = "entityOneGUID";
        final String  entityTwoGUIDParameterName    = "entityTwoGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeGUID(repositoryName, guidParameterName, relationshipTypeGUID, methodName);

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);
            if (typeDef != null) {
                repositoryValidator.validateTypeDefForInstance(repositoryName, guidParameterName, typeDef, methodName);


                repositoryValidator.validatePropertiesForType(repositoryName,
                        propertiesParameterName,
                        typeDef,
                        initialProperties,
                        methodName);

                repositoryValidator.validateInstanceStatus(repositoryName,
                        initialStatusParameterName,
                        initialStatus,
                        typeDef,
                        methodName);

            }
        } catch (TypeDefNotKnownException e) {
            typeDef = null;
        }

        if (typeDef == null) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Validation complete - ok to create new instance
         */

        //=================================================================================================================
        // Perform validation checks on type specified by guid.
        // Retrieve both entities by guid from the repository
        // Create an instance of the Relationship, setting the createdBy to userId, createdTime to now, etc.
        // Then set the properties and initialStatus.
        // Create the relationship in the Atlas repository.
        //

        RelationshipDef relationshipDef = (RelationshipDef) typeDef;

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


        /* Create a Relationship object
         * InstanceAuditHeader fields
         * InstanceType              type = null;
         * String                    createdBy
         * String                    updatedBy
         * Date                      createTime
         * Date                      updateTime
         * Long                      version
         * InstanceStatus            currentStatus
         * InstanceStatus            statusOnDelete
         * Instance Header fields
         * InstanceProvenanceType    instanceProvenanceType
         * String                    metadataCollectionId
         * String                    guid
         * String                    instanceURL
         * Relationship fields
         *   InstanceProperties    relationshipProperties
         *   String                entityOnePropertyName    // Retrieve this from the RelDef.RelEndDef for end1
         *   EntityProxy           entityOneProxy
         *   String                entityTwoPropertyName    // Retrieve this from the RelDef.RelEndDef for end2
         *   EntityProxy           entityTwoProxy
         */

        // An OM RelationshipDef has no superType - so we just set that to null
        InstanceType instanceType = new InstanceType(
                relationshipDef.getCategory(),
                relationshipTypeGUID,
                relationshipDef.getName(),
                relationshipDef.getVersion(),
                relationshipDef.getDescription(),
                relationshipDef.getDescriptionGUID(),
                null,
                relationshipDef.getValidInstanceStatusList(),
                validInstanceProperties);

        // Construct a Relationship object
        Date now = new Date();
        Relationship omRelationship = new Relationship();
        // Set fields from InstanceAuditHeader
        omRelationship.setType(instanceType);
        omRelationship.setCreatedBy(userId);
        omRelationship.setCreateTime(now);
        omRelationship.setUpdatedBy(userId);
        omRelationship.setUpdateTime(now);
        omRelationship.setVersion(1L);
        omRelationship.setStatus(InstanceStatus.ACTIVE);
        // Set fields from InstanceHeader
        omRelationship.setMetadataCollectionId(metadataCollectionId);
        omRelationship.setGUID(null);                                    // GUID will not be set until after the create in Atlas
        omRelationship.setInstanceURL(null);
        // Set fields from Relationship
        omRelationship.setProperties(initialProperties);
        //   String                entityOnePropertyName
        //   EntityProxy           entityOneProxy
        //   String                entityTwoPropertyName
        //   EntityProxy           entityTwoProxy

        RelationshipEndDef relEndDef1 = relationshipDef.getEndDef1();
        if (relEndDef1 != null) {
            String endDef1AttributeName = relEndDef1.getAttributeName();
            omRelationship.setEntityOnePropertyName(endDef1AttributeName);
        } else {
            LOG.error("addRelationship: Missing relationship end def in relationship def {} ", relationshipDef.getName());
            return null;
        }
        RelationshipEndDef relEndDef2 = relationshipDef.getEndDef2();
        if (relEndDef2 != null) {
            String endDef2AttributeName = relEndDef2.getAttributeName();
            omRelationship.setEntityTwoPropertyName(endDef2AttributeName);
        } else {
            LOG.error("addRelationship: Missing relationship end def in relationship def {} ", relationshipDef.getName());
            return null;
        }

        // Need to retrieve the entities from the repository and create an EntityProxy object for each...
        // ENT1 : We have the parameter entityOneGUID
        AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt1;
        try {
            atlasEntWithExt1 = entityStore.getById(entityOneGUID);
        } catch (AtlasBaseException e) {
            LOG.error("addRelationship: Caught exception from Atlas {}", e);
            // Handle below
            atlasEntWithExt1 = null;
        }
        if (atlasEntWithExt1 == null) {
            LOG.error("addRelationship: Could not find entity with guid {} ", entityOneGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityOneGUID, entityOneGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // Extract the entity
        AtlasEntity atlasEnt1 = atlasEntWithExt1.getEntity();

        // Convert the AtlasEntity into an OM EntityProxy
        try {

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEnt1);
            EntityProxy end1Proxy = atlasEntityMapper.toEntityProxy();
            LOG.debug("addRelationship: om entity {}", end1Proxy);
            omRelationship.setEntityOneProxy(end1Proxy);

        } catch (TypeErrorException | InvalidEntityException e) {

            LOG.error("addRelationship: caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityOneGUID, entityOneGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // Repeat the above for ent2

        // ENT2 : We have the parameter entityTwoGUID
        AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt2;
        try {
            atlasEntWithExt2 = entityStore.getById(entityTwoGUID);
        } catch (AtlasBaseException e) {
            LOG.error("addRelationship: Caught exception from Atlas {}", e);
            // Handle below
            atlasEntWithExt2 = null;
        }
        if (atlasEntWithExt2 == null) {
            LOG.error("addRelationship: Could not find entity with guid {} ", entityTwoGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTwoGUID, entityTwoGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // Extract the entity
        AtlasEntity atlasEnt2 = atlasEntWithExt2.getEntity();

        // Convert the AtlasEntity into an OM EntityProxy
        try {

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEnt2);
            EntityProxy end2Proxy = atlasEntityMapper.toEntityProxy();
            LOG.debug("addRelationship: om entity {}", end2Proxy);
            omRelationship.setEntityTwoProxy(end2Proxy);

        } catch (Exception e) {

            LOG.error("addRelationship: caught exception {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTwoGUID, entityTwoGUIDParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // omRelationship should be complete apart from GUID - which is to be assigned by repo.

        // Convert the OM Relationship to an AtlasRelationship. Because this is a new Relationship we want
        // Atlas to generate the GUID, so useExistingGUID must be set to false.
        AtlasRelationship atlasRelationship = convertOMRelationshipToAtlasRelationship(omRelationship, false, relationshipDef);

        // Add the Relationship to the AtlasRelationshipStore...
        AtlasRelationship returnedAtlasRelationship;
        try {

            returnedAtlasRelationship = relationshipStore.create(atlasRelationship);

        } catch (AtlasBaseException e) {

            LOG.error("addRelationship: caught exception from relationship store create method {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }


        // Verify the returnedAtlasRelationship and fill in any extra detail in omRelationship - e.g. sys attrs

        if (returnedAtlasRelationship.getStatus() != ACTIVE) {

            LOG.error("addRelationship: Atlas created relationship, but status set to {}", returnedAtlasRelationship.getStatus());

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Note the newly allocated relationship GUID
        String newRelationshipGuid = returnedAtlasRelationship.getGuid();
        omRelationship.setGUID(newRelationshipGuid);

        return omRelationship;

    }

    /*
     * Utility method to convert a Relationship to an AtlasRelationship.
     * Needs the RelationshipDef to find the propagation rule
     */
    private AtlasRelationship convertOMRelationshipToAtlasRelationship(Relationship omRelationship,
                                                                       boolean useExistingGUID,
                                                                       RelationshipDef relationshipDef)
        throws
            StatusNotSupportedException,
            TypeErrorException

    {


        final String methodName = "convertOMRelationshipToAtlasRelationship";

        /* Construct an AtlasRelationship
         * An AtlasRelationship has:
         * String              typeName
         * Map<String, Object> attributes
         * String              guid
         * AtlasObjectID       end1
         * AtlasObjectID       end2
         * String              label
         * PropagateTags       propagateTags
         * Status              status
         * String              createdBy
         * String              updatedBy
         * Date                createTime
         * Date                updateTime
         * Long                version
         */

        AtlasRelationship atlasRelationship = new AtlasRelationship();
        atlasRelationship.setTypeName(omRelationship.getType().getTypeDefName());
        /* GUID is set by Atlas to nextInternalID - you must leave it for a new AtlasRelationship,
         * unless you really want to reuse a particular GUID in which case the useGUID parameter will
         * have been set to true and the GUID to use will have been set in the OM Relationship...
         */
        if (useExistingGUID)
           atlasRelationship.setGuid(omRelationship.getGUID());

        InstanceStatus omStatus = omRelationship.getStatus();
        AtlasRelationship.Status atlasRelationshipStatus;
        switch (omStatus) {
            // AtlasEntity.Status can only be either { ACTIVE | DELETED }
            case DELETED:
                atlasRelationshipStatus = AtlasRelationship.Status.DELETED;
                break;
            case ACTIVE:
                atlasRelationshipStatus = AtlasRelationship.Status.ACTIVE;
                break;
            default:
                // unsupportable status
                LOG.error("convertOMRelationshipToAtlasRelationship: Atlas does not support relationship status {}", omStatus);
                OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new StatusNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }
        atlasRelationship.setStatus(atlasRelationshipStatus);

        atlasRelationship.setCreatedBy(omRelationship.getCreatedBy());
        atlasRelationship.setUpdatedBy(omRelationship.getUpdatedBy());
        atlasRelationship.setCreateTime(omRelationship.getCreateTime());
        atlasRelationship.setUpdateTime(omRelationship.getUpdateTime());
        atlasRelationship.setVersion(omRelationship.getVersion());
        // Set end1
        AtlasObjectId end1 = new AtlasObjectId();
        end1.setGuid(omRelationship.getEntityOneProxy().getGUID());                       // entityOneGUID
        end1.setTypeName(omRelationship.getEntityOneProxy().getType().getTypeDefName());  //relationshipDef.getEndDef1().getEntityType().getName());
        atlasRelationship.setEnd1(end1);
        // Set end2
        AtlasObjectId end2 = new AtlasObjectId();
        end2.setGuid(omRelationship.getEntityTwoProxy().getGUID());                      // entityTwoGUID
        end2.setTypeName(omRelationship.getEntityTwoProxy().getType().getTypeDefName()); // relationshipDef.getEndDef2().getEntityType().getName());
        atlasRelationship.setEnd2(end2);
        atlasRelationship.setLabel(omRelationship.getType().getTypeDefName());           // Set the label to the type name of the relationship.
        // Set propagateTags
        ClassificationPropagationRule omPropRule = relationshipDef.getPropagationRule();
        AtlasRelationshipDef.PropagateTags atlasPropTags = convertOMPropagationRuleToAtlasPropagateTags(omPropRule);
        atlasRelationship.setPropagateTags(atlasPropTags);

        // Set attributes on AtlasRelationship
        // Map attributes from OM Relationship to AtlasRelationship
        InstanceProperties instanceProperties = omRelationship.getProperties();
        Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
        atlasRelationship.setAttributes(atlasAttrs);

        // AtlasRelationship should have been fully constructed by this point

        return atlasRelationship;
    }

    /**
     * Update the status of a specific relationship.
     *
     * @param userId           - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param newStatus        - new InstanceStatus for the relationship.
     * @return Resulting relationship structure with the new status set.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws StatusNotSupportedException   - the metadata repository hosting the metadata collection does not support
     *                                       the requested status.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship updateRelationshipStatus(String         userId,
                                                 String         relationshipGUID,
                                                 InstanceStatus newStatus)
        throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            StatusNotSupportedException,
            UserNotAuthorizedException
    {

        final String  methodName          = "updateRelationshipStatus";
        final String  guidParameterName   = "relationshipGUID";
        final String  statusParameterName = "newStatus";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship = this.getRelationship(userId, relationshipGUID);

        repositoryValidator.validateInstanceType(repositoryName, relationship);

        String relationshipTypeGUID = relationship.getType().getTypeDefGUID();

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);
            if (typeDef != null) {
                repositoryValidator.validateNewStatus(repositoryName,
                        statusParameterName,
                        newStatus,
                        typeDef,
                        methodName);
            }
        }
        catch (TypeDefNotKnownException e) {
            typeDef = null;
        }

        if (typeDef == null) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        /*
         * Validation complete - ok to make changes
         */

        Relationship   updatedRelationship = new Relationship(relationship);

        updatedRelationship.setStatus(newStatus);

        updatedRelationship = repositoryHelper.incrementVersion(userId, relationship, updatedRelationship);

        /*
         * Store the updatedRelationship into Atlas
         *
         * To do this, retrieve the AtlasRelationship but do not map it (as we did above) to an OM Relationship.
         * Update the status of the Atlas relationship then write it back to the store
         */
        AtlasRelationship atlasRelationship;
        try {
            atlasRelationship = relationshipStore.getById(relationshipGUID);

        } catch (AtlasBaseException e) {

            LOG.error("updateRelationshipStatus: Caught exception from Atlas {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipGUID, guidParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        LOG.debug("updateRelationshipStatus: Read from atlas relationship store; relationship {}", atlasRelationship);

        // Atlas status field has type Status
        AtlasRelationship.Status atlasStatus;
        switch (newStatus) {
            case ACTIVE:
                atlasStatus = ACTIVE;
                break;
            case DELETED:
                atlasStatus = DELETED;
                break;
            default:
                // Atlas can only accept ACTIVE | DELETED
                LOG.error("updateRelationshipStatus: Atlas cannot accept status value of {}", newStatus);

                OMRSErrorCode errorCode    = OMRSErrorCode.BAD_INSTANCE_STATUS;

                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID,
                        methodName,
                        repositoryName);

                throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }

        atlasRelationship.setStatus(atlasStatus);

        // Save the relationship to Atlas
        AtlasRelationship returnedAtlasRelationship;
        try {

            returnedAtlasRelationship = relationshipStore.update(atlasRelationship);

        } catch (AtlasBaseException e) {

            LOG.error("updateRelationshipStatus: Caught exception from Atlas relationship store update method {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipGUID,
                    methodName,
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Refresh the system attributes of the returned OM Relationship
        updatedRelationship.setUpdatedBy(returnedAtlasRelationship.getUpdatedBy());
        updatedRelationship.setUpdateTime(returnedAtlasRelationship.getUpdateTime());

        return updatedRelationship;

    }


    /**
     * Update the properties of a specific relationship.
     *
     * @param userId           - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @param properties       - list of the properties to update.
     * @return Resulting relationship structure with the new properties set.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws PropertyErrorException        - one or more of the requested properties are not defined, or have different
     *                                       characteristics in the TypeDef for this relationship's type.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship updateRelationshipProperties(String             userId,
                                                     String             relationshipGUID,
                                                     InstanceProperties properties)
        throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            PropertyErrorException,
            UserNotAuthorizedException
    {

        final String  methodName = "updateRelationshipProperties";
        final String  guidParameterName = "relationshipGUID";
        final String  propertiesParameterName = "properties";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship = this.getRelationship(userId, relationshipGUID);

        repositoryValidator.validateInstanceType(repositoryName, relationship);

        String relationshipTypeGUID = relationship.getType().getTypeDefGUID();

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, relationshipTypeGUID);
            if (typeDef != null) {
                repositoryValidator.validateNewPropertiesForType(repositoryName,
                        propertiesParameterName,
                        typeDef,
                        properties,
                        methodName);
            }
        }
        catch (TypeDefNotKnownException e) {
            typeDef = null;
        }

        if (typeDef == null) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipTypeGUID, guidParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /*
         * Validation complete - ok to make changes
         */

        Relationship   updatedRelationship = new Relationship(relationship);

        updatedRelationship.setProperties(repositoryHelper.mergeInstanceProperties(repositoryName,
                relationship.getProperties(),
                properties));
        updatedRelationship = repositoryHelper.incrementVersion(userId, relationship, updatedRelationship);

        /*
         * Store the updatedRelationship into Atlas
         *
         * To do this, retrieve the AtlasRelationship but do not map it (as we did above) to an OM Relationship.
         * Update the properties of the Atlas relationship then write it back to the store
         */
        AtlasRelationship atlasRelationship;
        try {

            atlasRelationship = relationshipStore.getById(relationshipGUID);
            LOG.debug("updateRelationshipProperties: Read from atlas relationship store; relationship {}", atlasRelationship);

        } catch (AtlasBaseException e) {

            LOG.error("updateRelationshipProperties: Caught exception from Atlas {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipGUID, guidParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // The properties were merged above, retrieve the merged set from the updatedRelationship
        // and use it to replace the Atlas properties
        InstanceProperties mergedProperties = updatedRelationship.getProperties();
        Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(mergedProperties);
        atlasRelationship.setAttributes(atlasAttrs);

        // Save the relationship to Atlas
        AtlasRelationship returnedAtlasRelationship;
        try {

            returnedAtlasRelationship = relationshipStore.create(atlasRelationship);

        } catch (AtlasBaseException e) {
            LOG.error("updateRelationshipProperties: Caught exception from Atlas relationship store create method, {}", e);
            // handle below
            returnedAtlasRelationship = null;
        }

        if (returnedAtlasRelationship == null) {

            LOG.error("updateRelationshipProperties: Atlas relationship store create method did not return a relationship");

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // Convert the returnedAtlasRelationship to OM for return, instead of updatedRelationship

        Relationship returnRelationship;

        try {
            AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                    this,
                    userId,
                    returnedAtlasRelationship,
                    entityStore);

            returnRelationship = atlasRelationshipMapper.toOMRelationship();
            LOG.debug("updateRelationshipProperties: om relationship {}", returnRelationship);

        }
        catch (Exception e) {
            LOG.debug("updateRelationshipProperties: caught exception from mapper "+e.getMessage());
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(guidParameterName,
                    methodName,
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        return returnRelationship;
    }


    /**
     * Undo the latest change to a relationship (either a change of properties or status).
     *
     * @param userId           - unique identifier for requesting user.
     * @param relationshipGUID - String unique identifier (guid) for the relationship.
     * @return Relationship structure with the new current header, requested entities and properties.
     * @throws InvalidParameterException     - the guid is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the requested relationship is not known in the metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship undoRelationshipUpdate(String userId,
                                               String relationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String  methodName = "undoRelationshipUpdate";
        final String  guidParameterName = "relationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, relationshipGUID, methodName);

        // I do not known of a way in Atlas to retrieve an earlier previous version.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    /**
     * Delete a specific relationship.  This is a soft-delete which means the relationship's status is updated to
     * DELETED and it is no longer available for queries.  To remove the relationship permanently from the
     * metadata collection, use purgeRelationship().
     *
     * @param userId                          - unique identifier for requesting user.
     * @param typeDefGUID                     - unique identifier of the type of the relationship to delete.
     * @param typeDefName                     - unique name of the type of the relationship to delete.
     * @param obsoleteRelationshipGUID        - String unique identifier (guid) for the relationship.
     * @throws InvalidParameterException      - one of the parameters is null.
     * @throws RepositoryErrorException       - there is a problem communicating with the metadata repository where
     *                                          the metadata collection is stored.
     * @throws RelationshipNotKnownException  - the requested relationship is not known in the metadata collection.
     * @throws FunctionNotSupportedException  - the metadata repository hosting the metadata collection does not support
     *                                          soft-deletes.
     * @throws UserNotAuthorizedException     - the userId is not permitted to perform this operation.
     */
    public Relationship deleteRelationship(String userId,
                                           String typeDefGUID,
                                           String typeDefName,
                                           String obsoleteRelationshipGUID)
        throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            FunctionNotSupportedException,
            UserNotAuthorizedException
    {

        final String  methodName = "deleteRelationship";
        final String  guidParameterName = "typeDefGUID";
        final String  nameParameterName = "typeDefName";
        final String  relationshipParameterName = "obsoleteRelationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, obsoleteRelationshipGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, obsoleteRelationshipGUID);

        repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                typeDefGUID,
                typeDefName,
                relationship,
                methodName);


        /*
         * Process the request
         */

        // One final piece of validation - if deletes are configured as HARD then throw not supported exception
        if (atlasDeleteConfiguration == AtlasDeleteOption.HARD) {
            LOG.error("deleteRelationship: Repository is configured for hard deletes, cannot soft delete relationship with guid {}",
                    obsoleteRelationshipGUID);
            LocalAtlasOMRSErrorCode errorCode    = LocalAtlasOMRSErrorCode.ATLAS_CONFIGURATION_HARD;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(obsoleteRelationshipGUID, methodName, repositoryName);

            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /* From here on, this method is performing a SOFT delete.
         * The method returns the deleted Relationship, which should reflect
         * the change in state in the AtlasRelationship's system attributes.
         */

        try {
            relationshipStore.deleteById(obsoleteRelationshipGUID);

            /*
             * Read the relationship back from the store - this should work if it was a soft delete.
             * By reading back, the status should reflect that it's been deleted and it should
             * have updated system attributes.
             *
             * Retrieve the AtlasRelationship
             */
            AtlasRelationship atlasRelationship;

            atlasRelationship = relationshipStore.getById(obsoleteRelationshipGUID);

            if (atlasRelationship == null) {
                LOG.error("deleteRelationship: Could not find relationship with guid {} ", obsoleteRelationshipGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(obsoleteRelationshipGUID, relationshipParameterName, methodName, repositoryName);

                throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            AtlasRelationshipMapper atlasRelationshipMapper =
                    new AtlasRelationshipMapper(this,
                                                 userId,
                                                 atlasRelationship,
                                                 entityStore);

            Relationship returnRelationship = atlasRelationshipMapper.toOMRelationship();

            LOG.debug("deleteRelationship: deleted relationship {}", returnRelationship);

            return returnRelationship;

        }
        catch (TypeErrorException | AtlasBaseException | InvalidEntityException | InvalidRelationshipException | EntityNotKnownException e) {
            OMRSErrorCode errorCode = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
    }


    /**
     * Permanently delete the relationship from the repository.  There is no means to undo this request.
     *
     * @param userId                  - unique identifier for requesting user.
     * @param typeDefGUID             - unique identifier of the type of the relationship to purge.
     * @param typeDefName             - unique name of the type of the relationship to purge.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @throws InvalidParameterException       - one of the parameters is null.
     * @throws RepositoryErrorException        - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws RelationshipNotKnownException   - the requested relationship is not known in the metadata collection.
     * @throws RelationshipNotDeletedException - the requested relationship is not in DELETED status.
     * @throws UserNotAuthorizedException      - the userId is not permitted to perform this operation.
     */
    public void purgeRelationship(String userId,
                                  String typeDefGUID,
                                  String typeDefName,
                                  String deletedRelationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            RelationshipNotDeletedException,
            UserNotAuthorizedException
    {
        final String  methodName = "purgeRelationship";
        final String  guidParameterName = "typeDefGUID";
        final String  nameParameterName = "typeDefName";
        final String  relationshipParameterName = "deletedRelationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, deletedRelationshipGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, deletedRelationshipGUID);

        repositoryValidator.validateTypeForInstanceDelete(repositoryName,
                typeDefGUID,
                typeDefName,
                relationship,
                methodName);


        /* Atlas has two configuration settings for its DeleteHandler - soft deletes or hard deletes.
         * If Atlas is configured for hard deletes, this purge method will remove the relationship.
         * If Atlas is configured for soft deletes, the relationship must have been (soft-)deleted prior to
         * this purgeEntity call. In this case, check that the relationship is in DELETED state.
         *
         * This method does not use the repository's deleteById method - instead it uses the purgeById
         * method - which guarantees a permanent removal independent of how the repository is configured.
         */

        /*
         * If soft delete is enabled in the repository, check that the relationship has been deleted.
         */
        if (atlasDeleteConfiguration == AtlasDeleteOption.SOFT) {
            LOG.debug("purgeRelationship: Soft-delete is configured, check that relationship to be purged is in DELETED state");
            repositoryValidator.validateRelationshipIsDeleted(repositoryName, relationship, methodName);
        }


        /*
         * Validation is complete - ok to remove the entity
         */

        /*
         * From here on, this method performs a HARD delete.
         */

        try {

            LOG.debug("purgeRelationship: TODO!! add purgeById to relationship store!!");
            //entityStore.purgeById(deletedEntityGUID); // TODO - waiting for ATLAS-2774

        }
        //catch (AtlasBaseException e) { // TODO - catch appropriate exceptions when ATLAS-2774 is available
        catch (Exception e) {
            LOG.error("purgeRelationship: Caught exception from Atlas relationshipStore trying to purge relationship {}", deletedRelationshipGUID, e);

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.RELATIONSHIP_NOT_DELETED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(deletedRelationshipGUID, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Restore a deleted relationship into the metadata collection.  The new status will be ACTIVE and the
     * restored details of the relationship are returned to the caller.
     *
     * @param userId                  - unique identifier for requesting user.
     * @param deletedRelationshipGUID - String unique identifier (guid) for the relationship.
     * @return Relationship structure with the restored header, requested entities and properties.
     * @throws InvalidParameterException       - the guid is null.
     * @throws RepositoryErrorException        - there is a problem communicating with the metadata repository where
     *                                         the metadata collection is stored.
     * @throws RelationshipNotKnownException   - the requested relationship is not known in the metadata collection.
     * @throws RelationshipNotDeletedException - the requested relationship is not in DELETED status.
     * @throws UserNotAuthorizedException      - the userId is not permitted to perform this operation.
     */
    public Relationship restoreRelationship(String userId,
                                            String deletedRelationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            RelationshipNotDeletedException,
            UserNotAuthorizedException
    {

        final String  methodName = "restoreRelationship";
        final String  guidParameterName = "deletedRelationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, deletedRelationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, deletedRelationshipGUID);

        repositoryValidator.validateRelationshipIsDeleted(repositoryName, relationship, methodName);

        /*
         * Validation is complete.  It is ok to restore the relationship.
         */
        // Atlas has two configuration settings for its DeleteHandler - all soft deletes or all hard deletes.
        // We would require that soft deletes are in force and even then it is not clear how to implement
        // a restore from an earlier soft delete. There is no entity store operation for this.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }


    // =========================================================================================================

    // Group 5 methods

    /**
     * Change the guid of an existing entity to a new value.  This is used if two different
     * entities are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId        - unique identifier for requesting user.
     * @param typeDefGUID   - the guid of the TypeDef for the entity - used to verify the entity identity.
     * @param typeDefName   - the name of the TypeDef for the entity - used to verify the entity identity.
     * @param entityGUID    - the existing identifier for the entity.
     * @param newEntityGUID - new unique identifier for the entity.
     * @return entity - new values for this entity, including the new guid.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reIdentifyEntity(String userId,
                                         String typeDefGUID,
                                         String typeDefName,
                                         String entityGUID,
                                         String newEntityGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        final String  methodName = "reIdentifyEntity";
        final String  guidParameterName = "typeDefGUID";
        final String  nameParameterName = "typeDefName";
        final String  instanceParameterName = "deletedRelationshipGUID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        parentConnector.validateRepositoryIsActive(methodName);
        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, instanceParameterName, newEntityGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);

        /*
         * Locate entity
         */
        EntityDetail  entity  = getEntityDetail(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        /*
         * Validation complete - ok to make changes
         */

        /* In Atlas the GUID is the id of the vertex, so it is not possible to change it in situ.
         * It may be possible to implement this method by deleting the existing Atlas entity
         * and creating a new one. If soft deletes are in force then the previous entity would
         * still exist, in soft-deleted state. This may lead to complexity compared to the
         * more desirable situation of having exactly one GUID that relates to the entity.
         */

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }


    /**
     * Change the type of an existing entity.  Typically this action is taken to move an entity's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type and the properties adjusted.
     *
     * @param userId                - unique identifier for requesting user.
     * @param entityGUID            - the unique identifier for the entity to change.
     * @param currentTypeDefSummary - the current details of the TypeDef for the entity - used to verify the entity identity
     * @param newTypeDefSummary     - details of this entity's new TypeDef.
     * @return entity - new values for this entity, including the new type information.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException         - the requested type is not known, or not supported in the metadata repository
     *                                    hosting the metadata collection.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.     *
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reTypeEntity(String         userId,
                                     String         entityGUID,
                                     TypeDefSummary currentTypeDefSummary,
                                     TypeDefSummary newTypeDefSummary)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            ClassificationErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException

    {
        final String  methodName = "reTypeEntity";
        final String  entityParameterName = "entityGUID";
        final String  currentTypeDefParameterName = "currentTypeDefSummary";
        final String  newTypeDefParameterName = "newTypeDefSummary";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityParameterName, entityGUID, methodName);

        /*
         * Locate entity
         */
        EntityDetail  entity  = getEntityDetail(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);

        repositoryValidator.validateInstanceType(repositoryName,
                entity,
                currentTypeDefParameterName,
                currentTypeDefParameterName,
                currentTypeDefSummary.getGUID(),
                currentTypeDefSummary.getName());

        repositoryValidator.validatePropertiesForType(repositoryName,
                newTypeDefParameterName,
                newTypeDefSummary,
                entity.getProperties(),
                methodName);

        repositoryValidator.validateClassificationList(repositoryName,
                entityParameterName,
                entity.getClassifications(),
                newTypeDefSummary.getName(),
                methodName);

        /*
         * Validation complete - ok to make changes
         */


        /* In Atlas the entity type is stored in the AtlasEntity typeName field.
         * If you want to update an entity then you essentially pass the modified AtlasEntity
         * to the entity store's updateEntity method - this accepts an AtlasEntity.
         * That in turn will call the entity store's createOrUpdate.
         * This calls the store's preCreateOrUpdate...
         * preCreateOrUpdate calls validateAndNormalizeForUpdate
         *  ... that gets the type using the typeName in the updated entity
         *  ... and then calls validateValueForUpdate
         *  ... and getNormalizedValueForUpdate
         *  ... which validates that the attributes are valid values
         * createOrUpdate then...
         * ... compares the attributes & classifications of the updated entity
         * ... and then asks the EntityGraphMapper to alter attributes and classifications
         * ... this will get the type by referring to the typename in the (updated) entity....
         *
         * All in all I think it is possible to change the entity type
         *
         */

        /*
         * Alter the retrieved EntityDetail so that it's InstanceType reflects the new type
         * Then convert it to an AtlasEntity and call updateEntity.
         */


        // Retrieve the TypeDef for the new type
        String newTypeGUID = newTypeDefSummary.getGUID();
        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, newTypeGUID);
        }
        catch (TypeDefNotKnownException e) {
            typeDef = null;
        }

        if (typeDef == null || typeDef.getCategory() != ENTITY_DEF) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(newTypeGUID, newTypeDefParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Create a new InstanceType
        InstanceType newInstanceType = new InstanceType();
        newInstanceType.setTypeDefName(typeDef.getName());
        newInstanceType.setTypeDefCategory(typeDef.getCategory());
        List<TypeDefLink> superTypes = new ArrayList<>();
        superTypes.add(typeDef.getSuperType());
        newInstanceType.setTypeDefSuperTypes(superTypes);
        List<TypeDefAttribute> typeDefAttributes = typeDef.getPropertiesDefinition();
        List<String> validPropertyNames = null;
        if (typeDefAttributes != null) {
            validPropertyNames = new ArrayList<>();
            for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                validPropertyNames.add(typeDefAttribute.getAttributeName());
            }
        }
        newInstanceType.setValidInstanceProperties(validPropertyNames);
        newInstanceType.setTypeDefGUID(newTypeGUID);
        newInstanceType.setTypeDefVersion(typeDef.getVersion());
        newInstanceType.setValidStatusList(typeDef.getValidInstanceStatusList());
        newInstanceType.setTypeDefDescription(typeDef.getDescription());
        newInstanceType.setTypeDefDescriptionGUID(typeDef.getDescriptionGUID());

        // Set the new instance type into the entity
        entity.setType(newInstanceType);

        // Convert and store the re-typed entity to Atlas
        try {
            AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
            LOG.debug("updateEntityStatus: atlasEntity to update is {}", atlasEntity);

            // Construct an AtlasEntityWithExtInfo and call the repository
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
            AtlasObjectId atlasObjectId = AtlasTypeUtil.getAtlasObjectId(atlasEntity);

            entityStore.updateEntity(atlasObjectId, atlasEntityToUpdate,true);

            // Retrieve the AtlasEntity - rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

            atlasEntWithExt = entityStore.getById(entityGUID);

            if (atlasEntWithExt == null) {
                LOG.error("updateEntityStatus: Could not find entity with guid {} ", entityGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();

            return returnEntityDetail;

        }
        catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

            LOG.error("retypeEntity: Caught OMRS exception {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        catch (AtlasBaseException e) {

            LOG.error("retypeEntity: Caught exception from Atlas {}", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


    }


    /**
     * Change the home of an existing entity.  This action is taken for example, if the original home repository
     * becomes permanently unavailable, or if the user community updating this entity move to working
     * from a different repository in the open metadata repository cohort.
     *
     * @param userId                      - unique identifier for requesting user.
     * @param entityGUID                  - the unique identifier for the entity to change.
     * @param typeDefGUID                 - the guid of the TypeDef for the entity - used to verify the entity identity.
     * @param typeDefName                 - the name of the TypeDef for the entity - used to verify the entity identity.
     * @param homeMetadataCollectionId    - the existing identifier for this entity's home.
     * @param newHomeMetadataCollectionId - unique identifier for the new home metadata collection/repository.
     * @return entity - new values for this entity, including the new home information.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public EntityDetail reHomeEntity(String userId,
                                     String entityGUID,
                                     String typeDefGUID,
                                     String typeDefName,
                                     String homeMetadataCollectionId,
                                     String newHomeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName                = "reHomeEntity";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String entityParameterName       = "entityGUID";
        final String homeParameterName         = "homeMetadataCollectionId";
        final String newHomeParameterName      = "newHomeMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, entityParameterName, entityGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);
        repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);
        repositoryValidator.validateHomeMetadataGUID(repositoryName, newHomeParameterName, newHomeMetadataCollectionId, methodName);

        /*
         * Locate entity
         */
        EntityDetail  entity  =  getEntityDetail(userId, entityGUID);

        repositoryValidator.validateEntityFromStore(repositoryName, entityGUID, entity, methodName);


        /*
         * Validation complete - ok to make changes
         */
        // Set the new instance type into the entity
        entity.setMetadataCollectionId(newHomeMetadataCollectionId);

        // Convert and store the re-typed entity to Atlas
        try {
            AtlasEntity atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
            LOG.debug("updateEntityStatus: atlasEntity to update is {}", atlasEntity);

            // Construct an AtlasEntityWithExtInfo and call the repository
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityToUpdate = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
            AtlasObjectId atlasObjectId = AtlasTypeUtil.getAtlasObjectId(atlasEntity);
            entityStore.updateEntity(atlasObjectId, atlasEntityToUpdate,true);

            // Retrieve the AtlasEntity - rather than parsing the EMR since it only has AtlasEntityHeaders. So get the entity directly
            AtlasEntity.AtlasEntityWithExtInfo atlasEntWithExt;

            atlasEntWithExt = entityStore.getById(entityGUID);

            if (atlasEntWithExt == null) {
                LOG.error("updateEntityStatus: Could not find entity with guid {} ", entityGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }

            // atlasEntWithExt contains an AtlasEntity (entity) plus a Map<String, AtlasEntity> (referredEntities)
            // Extract the entity
            AtlasEntity atlasEntityRetrieved = atlasEntWithExt.getEntity();

            // Convert AtlasEntity to OM EntityDetail.
            EntityDetail returnEntityDetail;

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntityRetrieved);
            returnEntityDetail = atlasEntityMapper.toEntityDetail();

            return returnEntityDetail;

        }
        catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidEntityException e) {

            LOG.error("retypeEntity: Caught exception from AtlasEntityMapper {}", e);

            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        catch (AtlasBaseException e) {

            LOG.error("retypeEntity: Caught exception from Atlas {}", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, entityParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Change the guid of an existing relationship.  This is used if two different
     * relationships are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param userId              - unique identifier for requesting user.
     * @param typeDefGUID         - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName         - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param relationshipGUID    - the existing identifier for the relationship.
     * @param newRelationshipGUID - the new unique identifier for the relationship.
     * @return relationship - new values for this relationship, including the new guid.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                       metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship reIdentifyRelationship(String userId,
                                               String typeDefGUID,
                                               String typeDefName,
                                               String relationshipGUID,
                                               String newRelationshipGUID)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String methodName                   = "reIdentifyRelationship";
        final String guidParameterName            = "typeDefGUID";
        final String nameParameterName            = "typeDefName";
        final String relationshipParameterName    = "relationshipGUID";
        final String newRelationshipParameterName = "newHomeMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);
        repositoryValidator.validateGUID(repositoryName, newRelationshipParameterName, newRelationshipGUID, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, relationshipGUID);

        /*
         * Validation complete - ok to make changes
         */

        LOG.debug("reIdentifyRelationship: relationship is {}", relationship);

        /* In Atlas the GUID is the id of the vertex, so it is not possible to change it in situ.
         * It may be possible to implement this method by deleting the existing Atlas entity
         * and creating a new one. If soft deletes are in force then the previous entity would
         * still exist, in soft-deleted state. This may lead to complexity compared to the
         * more desirable situation of having exactly one GUID that relates to the entity.
         */

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }


    /**
     * Change the type of an existing relationship.  Typically this action is taken to move a relationship's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param userId                - unique identifier for requesting user.
     * @param relationshipGUID      - the unique identifier for the relationship.
     * @param currentTypeDefSummary - the details of the TypeDef for the relationship - used to verify the relationship identity.
     * @param newTypeDefSummary     - details of this relationship's new TypeDef.
     * @return relationship - new values for this relationship, including the new type information.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws TypeErrorException            - the requested type is not known, or not supported in the metadata repository
     *                                       hosting the metadata collection.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                       metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship reTypeRelationship(String         userId,
                                           String         relationshipGUID,
                                           TypeDefSummary currentTypeDefSummary,
                                           TypeDefSummary newTypeDefSummary)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException

    {

        final String methodName = "reTypeRelationship";
        final String relationshipParameterName = "relationshipGUID";
        final String currentTypeDefParameterName = "currentTypeDefSummary";
        final String newTypeDefParameterName = "newTypeDefSummary";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
        repositoryValidator.validateType(repositoryName, currentTypeDefParameterName, currentTypeDefSummary, TypeDefCategory.RELATIONSHIP_DEF, methodName);
        repositoryValidator.validateType(repositoryName, currentTypeDefParameterName, newTypeDefSummary, TypeDefCategory.RELATIONSHIP_DEF, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, relationshipGUID);

        repositoryValidator.validateInstanceType(repositoryName,
                relationship,
                currentTypeDefParameterName,
                currentTypeDefParameterName,
                currentTypeDefSummary.getGUID(),
                currentTypeDefSummary.getName());


        repositoryValidator.validatePropertiesForType(repositoryName,
                newTypeDefParameterName,
                newTypeDefSummary,
                relationship.getProperties(),
                methodName);

        /*
         * Validation complete - ok to make changes
         */

        /* In Atlas the relationship type is stored in the AtlasRelationship typeName field.
         * If you want to update an relationship then you essentially pass the modified AtlasRelationship
         * to the relationship store's update() method - this accepts an AtlasRelationship.
         */

        /*
         * Alter the retrieved Relationship so that it's InstanceType reflects the new type
         * Then convert it to an AtlasRelationship and call the update method.
         */


        // Retrieve the TypeDef for the new type
        String newTypeGUID = newTypeDefSummary.getGUID();
        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, newTypeGUID);
        }
        catch (TypeDefNotKnownException e) {
            typeDef = null;
        }

        if (typeDef == null || typeDef.getCategory() != RELATIONSHIP_DEF) {
            OMRSErrorCode errorCode    = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(newTypeGUID, newTypeDefParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        RelationshipDef relationshipDef = (RelationshipDef)typeDef;

        // Create a new InstanceType
        InstanceType newInstanceType = new InstanceType();
        newInstanceType.setTypeDefName(typeDef.getName());
        newInstanceType.setTypeDefCategory(typeDef.getCategory());
        // supertypes not relevant to Relationship
        newInstanceType.setTypeDefSuperTypes(null);
        List<TypeDefAttribute> typeDefAttributes = typeDef.getPropertiesDefinition();
        List<String> validPropertyNames = null;
        if (typeDefAttributes != null) {
            validPropertyNames = new ArrayList<>();
            for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                validPropertyNames.add(typeDefAttribute.getAttributeName());
            }
        }
        newInstanceType.setValidInstanceProperties(validPropertyNames);
        newInstanceType.setTypeDefGUID(newTypeGUID);
        newInstanceType.setTypeDefVersion(typeDef.getVersion());
        newInstanceType.setValidStatusList(typeDef.getValidInstanceStatusList());
        newInstanceType.setTypeDefDescription(typeDef.getDescription());
        newInstanceType.setTypeDefDescriptionGUID(typeDef.getDescriptionGUID());

        // Set the new instance type into the relationship
        relationship.setType(newInstanceType);


        try {

            // Convert and store the re-typed relationship to Atlas. Because this is a retype of an existing
            // Relationship, reuse the same GUID, so useExistingGUID must be set to true.
            AtlasRelationship atlasRelationship = convertOMRelationshipToAtlasRelationship(relationship, true, relationshipDef);

            LOG.debug("reTypeRelationship: atlasRelationship to update is {}", atlasRelationship);

            // Save the retyped AtlasRelationship to the repository
            AtlasRelationship atlasRelationshipRetrieved = relationshipStore.update(atlasRelationship);

            // Convert and return the retrieved AtlasRelationship
            if (atlasRelationshipRetrieved == null) {
                LOG.error("reTypeRelationship: Could not update relationship with guid {} ", relationshipGUID);
                OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

                throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }


            // Convert AtlasRelationship to OM Relationship.
            Relationship returnRelationship;

            AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                    this,
                    userId,
                    atlasRelationshipRetrieved,
                    entityStore);

            returnRelationship = atlasRelationshipMapper.toOMRelationship();
            LOG.debug("reTypeRelationship: retrieved Relationship {}", returnRelationship);

            return returnRelationship;

        }
        catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | EntityNotKnownException | InvalidRelationshipException | InvalidEntityException e) {
            LOG.error("reTypeRelationship: Caught OMRS exception", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

            throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        catch (AtlasBaseException e) {

            LOG.error("reTypeRelationship: Caught exception from Atlas", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    /**
     * Change the home of an existing relationship.  This action is taken for example, if the original home repository
     * becomes permanently unavailable, or if the user community updating this relationship move to working
     * from a different repository in the open metadata repository cohort.
     *
     * @param userId                      - unique identifier for requesting user.
     * @param relationshipGUID            - the unique identifier for the relationship.
     * @param typeDefGUID                 - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName                 - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId    - the existing identifier for this relationship's home.
     * @param newHomeMetadataCollectionId - unique identifier for the new home metadata collection/repository.
     * @return relationship - new values for this relationship, including the new home information.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identified by the guid is not found in the
     *                                       metadata collection.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public Relationship reHomeRelationship(String userId,
                                           String relationshipGUID,
                                           String typeDefGUID,
                                           String typeDefName,
                                           String homeMetadataCollectionId,
                                           String newHomeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            UserNotAuthorizedException
    {

        final String  methodName               = "reHomeRelationship";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String relationshipParameterName = "relationshipGUID";
        final String homeParameterName         = "homeMetadataCollectionId";
        final String newHomeParameterName      = "newHomeMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);
        repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);
        repositoryValidator.validateHomeMetadataGUID(repositoryName, newHomeParameterName, newHomeMetadataCollectionId, methodName);

        /*
         * Locate relationship
         */
        Relationship  relationship  = this.getRelationship(userId, relationshipGUID);

        /*
         * Validation complete - ok to make changes
         */

        // Will need the RelationshipDef
        RelationshipDef relationshipDef;
        TypeDef relTypeDef;
        try {
            relTypeDef = _getTypeDefByName(userId, typeDefName);
        }
        catch (TypeDefNotKnownException e) {
            // Handle below
            relTypeDef = null;
        }
        if (relTypeDef == null || relTypeDef.getCategory() != RELATIONSHIP_DEF) {
            LOG.debug("reHomeRelationship: no existing relationship_def with name {}", typeDefName);
            OMRSErrorCode errorCode    = OMRSErrorCode.INVALID_TYPEDEF;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(typeDefName, typeDefGUID, nameParameterName, methodName, repositoryName, "unknown");

            throw new InvalidParameterException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        relationshipDef = (RelationshipDef) relTypeDef;
        LOG.debug("reHomeRelationship: existing RelationshipDef: {}", relationshipDef);


        // Set the new metadataCollectionId in the relationship
        relationship.setMetadataCollectionId(newHomeMetadataCollectionId);

        // Convert and store the re-typed entity to Atlas
        try {
            // Construct an AtlasRelationship and call the repository
            AtlasRelationship atlasRelationship = convertOMRelationshipToAtlasRelationship(relationship, true, relationshipDef);
            LOG.debug("reHomeRelationship: atlasRelationship to update is {}", atlasRelationship);

            AtlasRelationship atlasReturnedRelationship = relationshipStore.update(atlasRelationship);

            // Convert AtlasRelationship to OM Relationship.
            Relationship returnRelationship;

            AtlasRelationshipMapper atlasRelationshipMapper = new AtlasRelationshipMapper(
                    this,
                    userId,
                    atlasReturnedRelationship,
                    entityStore);

            returnRelationship = atlasRelationshipMapper.toOMRelationship();
            LOG.debug("reHomeRelationship: returning Relationship {}", returnRelationship);

            return returnRelationship;

        }
        catch (StatusNotSupportedException | TypeErrorException | RepositoryErrorException | InvalidRelationshipException | EntityNotKnownException | InvalidEntityException e) {
            LOG.error("reTypeRelationship: Caught OMRS exception", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

            throw new RelationshipNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        catch (AtlasBaseException e) {

            LOG.error("reTypeRelationship: Caught exception from Atlas", e);
            OMRSErrorCode errorCode    = OMRSErrorCode.RELATIONSHIP_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(relationshipGUID, relationshipParameterName, methodName, repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

    }


    // =========================================================================================================

    // Group 6 methods

    /**
     * Save the entity as a reference copy.  The id of the home metadata collection is already set up in the
     * entity.
     *
     * @param userId - unique identifier for requesting user.
     * @param entity - details of the entity to save.
     * @throws InvalidParameterException  - the entity is null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws TypeErrorException         - the requested type is not known, or not supported in the metadata repository
     *                                    hosting the metadata collection.
     * @throws PropertyErrorException     - one or more of the requested properties are not defined, or have different
     *                                    characteristics in the TypeDef for this entity's type.
     * @throws HomeEntityException        - the entity belongs to the local repository so creating a reference
     *                                    copy would be invalid.
     * @throws EntityConflictException    - the new entity conflicts with an existing entity.
     * @throws InvalidEntityException     - the new entity has invalid contents.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void saveEntityReferenceCopy(String          userId,
                                        EntityDetail    entity)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            PropertyErrorException,
            HomeEntityException,
            EntityConflictException,
            InvalidEntityException,
            UserNotAuthorizedException
    {

        final String  methodName = "saveEntityReferenceCopy";
        final String  instanceParameterName = "entity";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);
        repositoryValidator.validateReferenceInstanceHeader(repositoryName,
                metadataCollectionId,
                instanceParameterName,
                entity,
                methodName);


        // Convert the EntityDetail to an AtlasEntity and store it.


        if (entity == null) {
            LOG.debug("saveEntityReferenceCopy: the EntityDetail entity is null");
            return;
        }

        AtlasEntity atlasEntity;
        try {
            atlasEntity = convertOMEntityDetailToAtlasEntity(userId, entity);
        }
        catch (StatusNotSupportedException e) {
            LOG.error("saveEntityReferenceCopy: Could not set entity status for entity with GUID {} ", entity.getGUID(), e);
            OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);
            throw new InvalidEntityException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        LOG.debug("saveEntityReferenceCopy: atlasEntity (minus any classifications) to create is {}", atlasEntity);

        // Store the entity

        // Construct an AtlasEntityStream and call the repository
        // Because we want to impose the GUID (e.g. RID) that has been supplied in the EntityDetail, we
        // need to ask Atlas to accept the entity with existing GUID. Therefore we must use an EntityImportStream.
        // The CTOR for this takes an AtlasEntityWithExtInfo entityWithExtInfo & an EntityStream entityStream
        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWEI = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
        AtlasEntityStreamForImport eis = new AtlasEntityStreamForImport(atlasEntityWEI, null);
        EntityMutationResponse emr;
        try {
            emr = entityStore.createOrUpdateForImport(eis);

        } catch (AtlasBaseException e) {
            LOG.error("saveEntityReferenceCopy: Caught exception trying to create entity", e);
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        LOG.debug("saveEntityReferenceCopy: emr is {}", emr);

        /* If there were classifications on the supplied EntityDetail, call Atlas to set classifications
         *
         * AtlasEntity needs a List<AtlasClassification> so we need to do some translation
         * OM Classification has:
         * String                 classificationName
         * InstanceProperties     classificationProperties
         * ClassificationOrigin   classificationOrigin
         * String                 classificationOriginGUID
         *
         * AtlasClassification has:
         * String                 entityGuid
         * boolean                propagate
         * List<TimeBoundary>     validityPeriods
         * String                 typeName;
         * Map<String, Object>    attributes;
         */
        if (entity.getClassifications() != null && entity.getClassifications().size() > 0) {
            ArrayList<AtlasClassification> atlasClassifications = new ArrayList<>();
            for (Classification omClassification : entity.getClassifications()) {
                AtlasClassification atlasClassification = new AtlasClassification(omClassification.getName());
                // For this OM classification build an Atlas equivalent...

                /* For now we are always setting propagatable to true and AtlasClassification has propagate=true by default.
                 * Instead this could traverse to the Classification.InstanceType.typeDefGUID and retrieve the ClassificationDef
                 * to find the value of propagatable on the def.
                 */
                atlasClassification.setTypeName(omClassification.getType().getTypeDefName());
                atlasClassification.setEntityGuid(entity.getGUID());

                // Map attributes from OM Classification to AtlasEntity
                InstanceProperties classificationProperties = omClassification.getProperties();
                Map<String, Object> atlasClassificationAttrs = convertOMPropertiesToAtlasAttributes(classificationProperties);
                atlasClassification.setAttributes(atlasClassificationAttrs);

                LOG.debug("saveEntityReferenceCopy: adding classification {}", atlasClassification);
                atlasClassifications.add(atlasClassification);
            }
            // We do not need to augment the AtlasEntity we created earlier - we can just use the
            // atlasClassifications directly with the following repository call...
            try {
                LOG.debug("saveEntityReferenceCopy: adding classifications {}", atlasClassifications);
                entityStore.addClassifications(entity.getGUID(), atlasClassifications);

            } catch (AtlasBaseException e) {
                // Could not add classifications to the entity
                LOG.error("saveEntityReferenceCopy: Atlas saved entity, but could not add classifications to it, guid {}", entity.getGUID(), e);
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }
        LOG.debug("saveEntityReferenceCopy: completed");


    }


    /**
     * Utility method to convert an OM EntityDetail to an AtlasEntity
     * @param userId - the security context of the operation
     * @param entityDetail - the OM EntityDetail object to convert
     * @return atlasEntity that corresponds to supplied entityDetail
     * @throws TypeErrorException          - general type error
     * @throws RepositoryErrorException    - unknown error afflicting repository
     * @throws StatusNotSupportedException - status value is not supported
     */
    private AtlasEntity convertOMEntityDetailToAtlasEntity(String       userId,
                                                           EntityDetail entityDetail)
        throws
            TypeErrorException,
            RepositoryErrorException,
            StatusNotSupportedException
    {

        final String methodName = "convertOMEntityDetailToAtlasEntity";


        // Find the entity type
        String entityTypeName = entityDetail.getType().getTypeDefName();

        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByName(userId, entityTypeName);
            if (typeDef == null || typeDef.getCategory() != ENTITY_DEF) {
                LOG.error("convertOMEntityDetailToAtlasEntity: Could not find entity def with name {} ", entityTypeName);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            AtlasEntity atlasEntity = new AtlasEntity();
            atlasEntity.setTypeName(typeDef.getName());

            /* The AtlasEntity constructor sets an unassigned GUID (initialized to nextInternalId),
             * because normally Atlas would generate a new GUID.
             *
             * However, in this particular API we want to accept a supplied identifier (could be a
             * RID from IGC for example) and use that as the GUID.
             *
             * The Atlas connector will need to later be able to identify the home repo that this
             * object came from - as well as reconstitute that repo's identifier for the object -
             * which could be an IGC rid for example. The GUID is set as supplied and the homeId
             * is set to the metadataCollectionId of the home repository.
             *
             */

            atlasEntity.setGuid(entityDetail.getGUID());
            atlasEntity.setHomeId(entityDetail.getMetadataCollectionId());


            InstanceStatus omStatus = entityDetail.getStatus();
            AtlasEntity.Status atlasEntityStatus;
            switch (omStatus) {
                // AtlasEntity.Status can only be either { ACTIVE | DELETED }
                case DELETED:
                    atlasEntityStatus = AtlasEntity.Status.DELETED;
                    break;
                case ACTIVE:
                    atlasEntityStatus = AtlasEntity.Status.ACTIVE;
                    break;
                default:
                    // unsupportable status
                    LOG.error("convertOMEntityDetailToAtlasEntity: Atlas does not support entity status {}", omStatus);
                    OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
                    String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                            this.getClass().getName(),
                            repositoryName);
                    throw new StatusNotSupportedException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

            }
            atlasEntity.setStatus(atlasEntityStatus);


            atlasEntity.setCreatedBy(entityDetail.getCreatedBy());
            atlasEntity.setUpdatedBy(entityDetail.getUpdatedBy());
            atlasEntity.setCreateTime(entityDetail.getCreateTime());
            atlasEntity.setUpdateTime(entityDetail.getUpdateTime());
            atlasEntity.setVersion(entityDetail.getVersion());
            // Cannot set classifications yet - need to do that post-create to get the entity GUID

            // Map attributes from OM EntityDetail to AtlasEntity
            InstanceProperties instanceProperties = entityDetail.getProperties();
            Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
            atlasEntity.setAttributes(atlasAttrs);

            // AtlasEntity has been fully constructed

            return atlasEntity;

        }
        catch (TypeDefNotKnownException e) {

            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

    }

    /**
     * Remove a reference copy of the the entity from the local repository.  This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or entities that have come from open metadata archives.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param entityGUID               - the unique identifier for the entity.
     * @param typeDefGUID              - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName              - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the new home repository.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws HomeEntityException        - the entity belongs to the local repository so creating a reference
     *                                    copy would be invalid.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void purgeEntityReferenceCopy(String userId,
                                         String entityGUID,
                                         String typeDefGUID,
                                         String typeDefName,
                                         String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            HomeEntityException,
            UserNotAuthorizedException
    {
        final String methodName                = "purgeEntityReferenceCopy";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String entityParameterName       = "entityGUID";
        final String homeParameterName         = "homeMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateGUID(repositoryName, entityParameterName, entityGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName, guidParameterName, nameParameterName, typeDefGUID, typeDefName, methodName);
        repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);

        // Atlas has two configuration settings for its DeleteHandler - all soft deletes or all hard deletes.
        // It is not clear how to implement this method other than when hard deletes are configured.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified entity sends out the details of this entity so the local repository can create a reference copy.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param entityGUID               - unique identifier of requested entity.
     * @param typeDefGUID              - unique identifier of requested entity's TypeDef.
     * @param typeDefName              - unique name of requested entity's TypeDef.
     * @param homeMetadataCollectionId - identifier of the metadata collection that is the home to this entity.
     * @throws InvalidParameterException  - one of the parameters is invalid or null.
     * @throws RepositoryErrorException   - there is a problem communicating with the metadata repository where
     *                                      the metadata collection is stored.
     * @throws EntityNotKnownException    - the entity identified by the guid is not found in the metadata collection.
     * @throws HomeEntityException        - the entity belongs to the local repository so creating a reference
     *                                      copy would be invalid.
     * @throws UserNotAuthorizedException - the userId is not permitted to perform this operation.
     */
    public void refreshEntityReferenceCopy(String userId,
                                           String entityGUID,
                                           String typeDefGUID,
                                           String typeDefName,
                                           String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            HomeEntityException,
            UserNotAuthorizedException
    {

        final String methodName = "refreshEntityReferenceCopy";

        /*
         * This method needs to check whether this is the metadataCollection indicated in the homeMetadataCollectionId
         * parameter. If it is not, it will ignore the request. If it is the specified metadataCollection, then it needs
         * to retrieve the entity (by GUID) from Atlas and pass it to the EventMapper which will handle it (formulate the
         * appropriate call to the repository event processor).
         */

        if (!homeMetadataCollectionId.equals(this.metadataCollectionId)) {
            LOG.debug("refreshEntityReferenceCopy: ignoring request because not intended for this metadataCollection");
            return;
        }

        if (this.eventMapper == null) {

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.EVENT_MAPPER_NOT_INITIALIZED;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /* This is the correct metadataCollection and it has a valid event mapper.
         * Retrieve the entity and call the event mapper
         */

        // Using the supplied guid look up the entity.

        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
        try {
            atlasEntityWithExt = entityStore.getById(entityGUID);
        } catch (AtlasBaseException e) {

            LOG.error("getEntityDetail: caught exception from get entity by guid {}, {}", entityGUID, e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        LOG.debug("getEntityDetail: atlasEntityWithExt is {}", atlasEntityWithExt);
        AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();


        // Project the AtlasEntity as an EntityDetail and invoke the mapper

        try {

            AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
            EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== getEntityDetail(userId={}, guid={}: entityDetail={})", userId, entityGUID, omEntityDetail);
            }
            this.eventMapper.processRefreshEvent(omEntityDetail);

        } catch (TypeErrorException | InvalidEntityException e) {
            LOG.error("getEntityDetail: caught exception from attempt to convert Atlas entity to OM {}, {}", atlasEntity, e);

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityGUID, methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


    }

    /**
     * Save the relationship as a reference copy.  The id of the home metadata collection is already set up in the
     * relationship.
     *
     * @param userId       - unique identifier for requesting user.
     * @param relationship - relationship to save.
     * @throws InvalidParameterException     - the relationship is null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws TypeErrorException            - the requested type is not known, or not supported in the metadata repository
     *                                       hosting the metadata collection.
     * @throws EntityNotKnownException       - one of the entities identified by the relationship is not found in the
     *                                       metadata collection.
     * @throws PropertyErrorException        - one or more of the requested properties are not defined, or have different
     *                                       characteristics in the TypeDef for this relationship's type.
     * @throws HomeRelationshipException     - the relationship belongs to the local repository so creating a reference
     *                                       copy would be invalid.
     * @throws RelationshipConflictException - the new relationship conflicts with an existing relationship.
     * @throws InvalidRelationshipException  - the new relationship has invalid contents.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public void saveRelationshipReferenceCopy(String       userId,
                                              Relationship relationship)
        throws
            InvalidParameterException,
            RepositoryErrorException,
            TypeErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            HomeRelationshipException,
            RelationshipConflictException,
            InvalidRelationshipException,
            UserNotAuthorizedException
    {
        final String  methodName = "saveRelationshipReferenceCopy";
        final String  instanceParameterName = "relationship";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateReferenceInstanceHeader(repositoryName, metadataCollectionId, instanceParameterName, relationship, methodName);

        // Need to access the relationship def to find the OM propagation rule.
        // Use the typeName from the relationship to locate the RelationshipDef typedef for the relationship.

        // Note that relationship end def attributeNames are reversed between OM and Atlas.... see the converter methods.


        // Find the relationship type
        String relationshipTypeName = relationship.getType().getTypeDefName();
        TypeDef typeDef;
        try {

            typeDef = _getTypeDefByName(userId, relationshipTypeName);

        }
        catch (TypeDefNotKnownException e) {
            LOG.debug("saveRelationshipReferenceCopy: Caught exception attempting to get relationship type from _getTypeDefByName {}", e.getMessage());
            // Handle below
            typeDef = null;
        }
        // Validate it
        if (typeDef == null || typeDef.getCategory() != RELATIONSHIP_DEF) {
            LOG.error("saveRelationshipReferenceCopy: Could not find a RelationshipDef with name {} ", relationshipTypeName);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        RelationshipDef relationshipDef = (RelationshipDef) typeDef;


        // Find the GUIDs and the type names of the entities
        String entityOneGUID = null;
        String entityOneTypeName = null;
        EntityProxy entityOneProxy = relationship.getEntityOneProxy();
        if (entityOneProxy != null) {
            entityOneGUID = entityOneProxy.getGUID();
            if (entityOneProxy.getType() != null) {
                entityOneTypeName = entityOneProxy.getType().getTypeDefName();
            }
        }

        // Test whether we have the entity (by GUID). If so that's cool; if not we must create a proxy
        // Using the supplied guid look up the entity
        try {
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
            atlasEntityWithExt = entityStore.getById(entityOneGUID);
            LOG.debug("saveRelationshipReferenceCopy: entity one exists in repository; {}", atlasEntityWithExt);

        } catch (AtlasBaseException e) {

            LOG.debug("saveRelationshipReferenceCopy: exception from get entity by guid {}, {}",entityOneGUID, e.getMessage());
            // The entity does not already exist, so create as proxy
            try {
                addEntityProxy(userId,entityOneProxy);
            }
            catch (InvalidParameterException | RepositoryErrorException |
                   FunctionNotSupportedException | UserNotAuthorizedException exc) {
                LOG.error("saveRelationshipReferenceCopy: caught exception from addEntityProxy", exc );
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                        entityOneProxy.toString(), methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        }


        String entityTwoGUID = null;
        String entityTwoTypeName = null;
        EntityProxy entityTwoProxy = relationship.getEntityTwoProxy();
        if (entityTwoProxy != null) {
            entityTwoGUID = entityTwoProxy.getGUID();
            if (entityTwoProxy.getType() != null) {
                entityTwoTypeName = entityTwoProxy.getType().getTypeDefName();
            }
        }

        // Test whether we have the entity (by GUID). If so that's cool; if not we must create a proxy
        // Using the supplied guid look up the entity
        try {
            AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
            atlasEntityWithExt = entityStore.getById(entityTwoGUID);
            LOG.debug("saveRelationshipReferenceCopy: entity two exists in repository; {}", atlasEntityWithExt);

        } catch (AtlasBaseException e) {

            LOG.debug("saveRelationshipReferenceCopy: exception from get entity by guid {}, {}",entityTwoGUID, e.getMessage());
            // The entity does not already exist, so create as proxy
            try {
                addEntityProxy(userId,entityTwoProxy);
            }
            catch (InvalidParameterException | RepositoryErrorException |
                    FunctionNotSupportedException | UserNotAuthorizedException exc) {
                LOG.error("saveRelationshipReferenceCopy: caught exception from addEntityProxy", exc );
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ENTITY_NOT_CREATED;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                        entityTwoProxy.toString(), methodName, repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        }

        /* Create or Update the AtlasRelationship
         * If the relationship already exists - i.e. we have a relationship with the same GUID - then
         * perform an update. Otherwise we will create the relationship.
         */

        AtlasRelationship atlasRelationship = new AtlasRelationship();

        /* GUID is set by Atlas CTOR (called above) to nextInternalID. Because we are saving a ref copy we overwrite with
         * the provided GUID if one has been supplied. In some cases, the caller may not have a GUID to use, in which case
         * leave the GUID as generated by Atlas.
         */
        String suppliedGUID = relationship.getGUID();
        if (suppliedGUID != null) {
            atlasRelationship.setGuid(suppliedGUID);
        }
        // The atlasRelationship now has either the supplied GUID or has generated its own GUID


        /* The Atlas connector will need to later be able to identify the home repo that this object came from - as well as reconstitute that
         * repo's identifier for the object - which could be an IGC rid for example. The GUID is stored as supplied and the homeId is set to the
         * metadataCollectionId of the home repository.
         */
        atlasRelationship.setHomeId(relationship.getMetadataCollectionId());

        atlasRelationship.setTypeName(relationshipTypeName);
        atlasRelationship.setStatus(ACTIVE);
        atlasRelationship.setCreatedBy(relationship.getCreatedBy());
        atlasRelationship.setUpdatedBy(relationship.getUpdatedBy());
        atlasRelationship.setCreateTime(relationship.getCreateTime());
        atlasRelationship.setUpdateTime(relationship.getUpdateTime());
        atlasRelationship.setVersion(relationship.getVersion());
        // Set end1
        AtlasObjectId end1 = new AtlasObjectId();
        end1.setGuid(entityOneGUID);
        end1.setTypeName(entityOneTypeName);
        atlasRelationship.setEnd1(end1);
        // Set end2
        AtlasObjectId end2 = new AtlasObjectId();
        end2.setGuid(entityTwoGUID);
        end2.setTypeName(entityTwoTypeName);
        atlasRelationship.setEnd2(end2);
        atlasRelationship.setLabel(relationshipTypeName);    // Set the label to the type name of the relationship def.
        // Set propagateTags
        ClassificationPropagationRule omPropRule = relationshipDef.getPropagationRule();
        AtlasRelationshipDef.PropagateTags atlasPropTags = convertOMPropagationRuleToAtlasPropagateTags(omPropRule);
        atlasRelationship.setPropagateTags(atlasPropTags);

        // Set attributes on AtlasRelationship
        // Get the relationshipProperties from the OM Relationship, and create Atlas attributes...

        // Map attributes from OM Relationship to AtlasRelationship
        InstanceProperties instanceProperties = relationship.getProperties();
        Map<String, Object> atlasAttrs = convertOMPropertiesToAtlasAttributes(instanceProperties);
        atlasRelationship.setAttributes(atlasAttrs);

        // AtlasRelationship should have been fully constructed by this point

        // Call the repository
        // If the relationship does not exist we will call create(). If already exists then we must call update().
        // So we need to find out whether it exists or not... the getById will throw an exception if the id does
        // not yet exist.

        boolean relationshipExists;
        try {
            AtlasRelationship existingRelationship = relationshipStore.getById(atlasRelationship.getGuid());
            LOG.debug("Relationship with GUID {} already exists - so will be updated", atlasRelationship.getGuid() );
            relationshipExists = true;
        }
        catch (AtlasBaseException e) {
            if (e.getAtlasErrorCode() == AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND) {
                LOG.debug("Relationship with GUID {} does not exist - so will be created", atlasRelationship.getGuid());
                relationshipExists = false;
            }
            else {
                // Trouble at mill...
                LOG.debug("saveRelationshipReferenceCopy: Caught exception from Atlas relationship store getById method {}", e.getMessage());

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;

                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }


        AtlasRelationship returnedAtlasRelationship;

        try {
            if (relationshipExists) {
                LOG.debug("Updating existing relationship wih GUID {}", atlasRelationship.getGuid());
                returnedAtlasRelationship = relationshipStore.update(atlasRelationship);
            } else {
                LOG.debug("Creating new relationship wih GUID {}", atlasRelationship.getGuid());
                returnedAtlasRelationship = relationshipStore.create(atlasRelationship);
            }
        }
        catch (AtlasBaseException e) {

            LOG.debug("saveRelationshipReferenceCopy: Caught exception from Atlas relationship store create/update method {}", e.getMessage());

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.REPOSITORY_ERROR;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        // Verify the returnedAtlasRelationship and fill in any extra detail in relationship - e.g. sys attrs

        if (returnedAtlasRelationship.getStatus() != ACTIVE) {
            LOG.error("saveRelationshipReferenceCopy: Atlas created relationship, but status set to {}", returnedAtlasRelationship.getStatus());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_FROM_STORE;

            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        LOG.debug("saveRelationshipReferenceCopy: Save to Atlas returned relationship {}", returnedAtlasRelationship);

    }


    /**
     * Remove the reference copy of the relationship from the local repository. This method can be used to
     * remove reference copies from the local cohort, repositories that have left the cohort,
     * or relationships that have come from open metadata archives.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param relationshipGUID         - the unique identifier for the relationship.
     * @param typeDefGUID              - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName              - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identifier is not recognized.
     * @throws HomeRelationshipException     - the relationship belongs to the local repository so creating a reference
     *                                       copy would be invalid.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public void purgeRelationshipReferenceCopy(String userId,
                                               String relationshipGUID,
                                               String typeDefGUID,
                                               String typeDefName,
                                               String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            HomeRelationshipException,
            UserNotAuthorizedException
    {
        final String methodName                = "purgeRelationshipReferenceCopy";
        final String guidParameterName         = "typeDefGUID";
        final String nameParameterName         = "typeDefName";
        final String relationshipParameterName = "relationshipGUID";
        final String homeParameterName         = "homeMetadataCollectionId";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateGUID(repositoryName, relationshipParameterName, relationshipGUID, methodName);
        repositoryValidator.validateTypeDefIds(repositoryName,
                guidParameterName,
                nameParameterName,
                typeDefGUID,
                typeDefName,
                methodName);
        repositoryValidator.validateHomeMetadataGUID(repositoryName, homeParameterName, homeMetadataCollectionId, methodName);

        /*
         * Process the request
         */

        // Atlas has two configuration settings for its DeleteHandler - all soft deletes or all hard deletes.
        // It is not clear how to implement this method other than when hard deletes are configured.

        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());
    }


    /**
     * The local repository has requested that the repository that hosts the home metadata collection for the
     * specified relationship sends out the details of this relationship so the local repository can create a
     * reference copy.
     *
     * @param userId                   - unique identifier for requesting user.
     * @param relationshipGUID         - unique identifier of the relationship.
     * @param typeDefGUID              - the guid of the TypeDef for the relationship - used to verify the relationship identity.
     * @param typeDefName              - the name of the TypeDef for the relationship - used to verify the relationship identity.
     * @param homeMetadataCollectionId - unique identifier for the home repository for this relationship.
     * @throws InvalidParameterException     - one of the parameters is invalid or null.
     * @throws RepositoryErrorException      - there is a problem communicating with the metadata repository where
     *                                       the metadata collection is stored.
     * @throws RelationshipNotKnownException - the relationship identifier is not recognized.
     * @throws HomeRelationshipException     - the relationship belongs to the local repository so creating a reference
     *                                       copy would be invalid.
     * @throws UserNotAuthorizedException    - the userId is not permitted to perform this operation.
     */
    public void refreshRelationshipReferenceCopy(String userId,
                                                 String relationshipGUID,
                                                 String typeDefGUID,
                                                 String typeDefName,
                                                 String homeMetadataCollectionId)
            throws
            InvalidParameterException,
            RepositoryErrorException,
            RelationshipNotKnownException,
            HomeRelationshipException,
            UserNotAuthorizedException
    {
        final String methodName                = "refreshRelationshipReferenceCopy";

        /*
         *  TODO!! Need to work out how to implement this method in conjunction with EventMapper
         */
        OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;

        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                this.getClass().getName(),
                repositoryName);

        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                this.getClass().getName(),
                methodName,
                errorMessage,
                errorCode.getSystemAction(),
                errorCode.getUserAction());

    }


    // ==============================================================================================================
    //
    // INTERNAL METHODS AND HELPER CLASSSES
    //

    /*
     * Parse the OM EntityDef and create an AtlasEntityDef
     */
    private AtlasEntityDef convertOMEntityDefToAtlasEntityDef(EntityDef omEntityDef) {

        LOG.debug("convertOMEntityDef: OM EntityDef {}", omEntityDef);

        if (omEntityDef == null) {
            return null;
        }

        String omTypeName = omEntityDef.getName();
        String atlasTypeName = omTypeName;

        // Detect whether the OM Type is one of the Famous Five
        boolean famousFive = false;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName)) {
            famousFive = true;
            // Prefix the type name
            atlasTypeName = FamousFive.getAtlasTypeName(omTypeName,omEntityDef.getGUID());
        }

        // Convert OM type into a valid Atlas type

        // Allocate AtlasEntityDef, which will set TypeCategory automatically
        AtlasEntityDef atlasEntityDef = new AtlasEntityDef();

        // Set common fields
        atlasEntityDef.setGuid(omEntityDef.getGUID());
        atlasEntityDef.setName(atlasTypeName);
        atlasEntityDef.setDescription(omEntityDef.getDescription());
        atlasEntityDef.setVersion(omEntityDef.getVersion());
        atlasEntityDef.setTypeVersion(omEntityDef.getVersionName());
        atlasEntityDef.setCreatedBy(omEntityDef.getCreatedBy());
        atlasEntityDef.setUpdatedBy(omEntityDef.getUpdatedBy());
        atlasEntityDef.setCreateTime(omEntityDef.getCreateTime());
        atlasEntityDef.setUpdateTime(omEntityDef.getUpdateTime());
        atlasEntityDef.setOptions(omEntityDef.getOptions());

        // Handle fields that require conversion - i.e. supertypes, attributeDefs
        // subtypes are deliberately ignored

        // Convert OMRS List<TypeDefLink> to an Atlas Set<String> of Atlas type names
        // If famousFive then modify the superType hierarchy
        if (!famousFive) {
            TypeDefLink omSuperType = omEntityDef.getSuperType();
            if (omSuperType == null) {
                // If there is no superType defined in OM EntityDef then set atlasEntityDef.superTypes to null
                atlasEntityDef.setSuperTypes(null);
            } else {
                // OM type has supertype...add that plus original OM type as supertypes in Atlas.
                // If the OM supertype is itself in Famous Five, convert it...
                String omSuperTypeName = omSuperType.getName();
                String omSuperTypeGUID = omSuperType.getGUID();
                if (FamousFive.omTypeRequiresSubstitution(omSuperTypeName)) {
                    // Prefix the supertype name
                    String atlasSuperTypeName = FamousFive.getAtlasTypeName(omSuperTypeName, omSuperTypeGUID);
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(atlasSuperTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                }
                else {
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(omSuperTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                }
            }
        }
        else { // famousFive
            TypeDefLink omSuperType = omEntityDef.getSuperType();
            if (omSuperType == null) {
                // If there is no superType defined in OM set atlasEntityDef.superTypes to original OM type
                Set<String> atlasSuperTypes = new HashSet<>();
                atlasSuperTypes.add(omTypeName);
                atlasEntityDef.setSuperTypes(atlasSuperTypes);
            } else {
                // OM type has supertype...add that plus original OM type as supertypes in Atlas.
                // If the OM supertype is itself in Famous Five, convert it...
                String omSuperTypeName = omSuperType.getName();
                String omSuperTypeGUID = omSuperType.getGUID();
                if (FamousFive.omTypeRequiresSubstitution(omSuperTypeName)) {
                    // Prefix the supertype name
                    String atlasSuperTypeName = FamousFive.getAtlasTypeName(omSuperTypeName, omSuperTypeGUID);
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(atlasSuperTypeName);
                    atlasSuperTypes.add(omTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                }
                else {
                    Set<String> atlasSuperTypes = new HashSet<>();
                    atlasSuperTypes.add(omSuperTypeName);
                    atlasSuperTypes.add(omTypeName);
                    atlasEntityDef.setSuperTypes(atlasSuperTypes);
                }
            }
        }

        // Set Atlas Attribute defs
        // OMRS ArrayList<TypeDefAttribute> --> Atlas List<AtlasAttributeDef>
        // Retrieve the OM EntityDef attributes:
        List<TypeDefAttribute> omAttrs = omEntityDef.getPropertiesDefinition();
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttrs = convertOMAttributeDefs(omAttrs);
        atlasEntityDef.setAttributeDefs(atlasAttrs);

        // Return the AtlasEntityDef
        return atlasEntityDef;
    }


    /*
     * Parse the OM RelationshipDef and create an AtlasRelationshipDef
     */
    private AtlasRelationshipDef convertOMRelationshipDefToAtlasRelationshipDef(RelationshipDef omRelationshipDef)
            throws
            RepositoryErrorException,
            TypeErrorException
    {


        final String methodName = "convertOMRelationshipDefToAtlasRelationshipDef";

        LOG.debug("convertOMRelationshipDef: OM RelationshipDef {}", omRelationshipDef);

        if (omRelationshipDef == null) {
            return null;
        }

        /*
         * Convert OM type into a valid Atlas type:
         *
         *  [OM RelationshipDef]                                         ->  [AtlasRelationshipDef]
         *  RelationshipCategory               relationshipCategory      ->  RelationshipCategory    relationshipCategory; REMOVED
         *  RelationshipContainerEnd           relationshipContainerEnd  ->  sets aspects of EndDef that is the ctr end    REMOVED
         *  ClassificationPropagationRule      propagationRule           ->  PropagateTags           propagateTags;
         *  RelationshipEndDef                 endDef1                   ->  AtlasRelationshipEndDef endDef1;
         *  RelationshipEndDef                 endDef2                   ->  AtlasRelationshipEndDef endDef2;
         *  TypeDefLink                        superType                 ->  IGNORED
         *  String                             description               ->  description
         *  String                             descriptionGUID           ->  IGNORED
         *  String                             origin                    ->  IGNORED
         *  String                             createdBy                 ->  createdBy
         *  String                             updatedBy                 ->  updatedBy
         *  Date                               createTime                ->  createTime
         *  Date                               updateTime                ->  updateTime
         *  Map<String, String>                options                   ->  options
         *  ArrayList<ExternalStandardMapping> externalStandardMappings  ->  IGNORED
         *  ArrayList<InstanceStatus>          validInstanceStatusList   ->  IGNORED
         *  InstanceStatus                     initialStatus             ->  IGNORED
         *  ArrayList<TypeDefAttribute>        propertiesDefinition      ->  attributeDefs
         *  Long                               version                   ->  atlas version
         *  String                             versionName               ->  typeVersion
         *  TypeDefCategory                    category                  ->  NOT NEEDED Atlas Cat set by CTOR
         *  String                             guid                      ->  atlas guid
         *  String                             name                      ->  atlas name
         */


        // Allocate AtlasRelationshipDef, which will set TypeCategory automatically
        AtlasRelationshipDef atlasRelationshipDef;
        try {
            atlasRelationshipDef = new AtlasRelationshipDef();

        } catch (AtlasBaseException e) {
            LOG.error("convertOMRelationshipDef: could not create an AtlasRelationshipDef for type {}", omRelationshipDef.getName(), e);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("RelationshipDef", "convert", metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "convertOMRelationshipDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Set common fields
        atlasRelationshipDef.setGuid(omRelationshipDef.getGUID());
        atlasRelationshipDef.setName(omRelationshipDef.getName());
        atlasRelationshipDef.setDescription(omRelationshipDef.getDescription());
        atlasRelationshipDef.setVersion(omRelationshipDef.getVersion());
        atlasRelationshipDef.setTypeVersion(omRelationshipDef.getVersionName());
        atlasRelationshipDef.setCreatedBy(omRelationshipDef.getCreatedBy());
        atlasRelationshipDef.setUpdatedBy(omRelationshipDef.getUpdatedBy());
        atlasRelationshipDef.setCreateTime(omRelationshipDef.getCreateTime());
        atlasRelationshipDef.setUpdateTime(omRelationshipDef.getUpdateTime());
        atlasRelationshipDef.setOptions(omRelationshipDef.getOptions());

        /*
         * Remaining fields require conversion - supertypes and subtypes are deliberately ignored
         */

        /* If in future OM RelationshipDef supports different categories of Relationship, convert accordingly:
         *
         * RelationshipCategory omRelCat = omRelationshipDef.getRelationshipCategory();
         * AtlasRelationshipDef.RelationshipCategory atlasRelCat = convertOMRelationshipCategoryToAtlasRelationshipCategory(omRelCat);
         * atlasRelationshipDef.setRelationshipCategory(atlasRelCat);
         *
         * For now though:
         * OM relationship defs are always ASSOCIATIONS. Atlas supports COMPOSITION and AGGREGATION too, but OM only uses ASSOCIATION
         */
        atlasRelationshipDef.setRelationshipCategory(AtlasRelationshipDef.RelationshipCategory.ASSOCIATION);

        // Convert propagationRule to propagateTags
        ClassificationPropagationRule omPropRule = omRelationshipDef.getPropagationRule();

        AtlasRelationshipDef.PropagateTags atlasPropTags = convertOMPropagationRuleToAtlasPropagateTags(omPropRule);

        atlasRelationshipDef.setPropagateTags(atlasPropTags);

        /* RelationshipEndDef needs to be converted to AtlasRelationshipEndDef
         *
         * OM RelationshipEndDef contains:
         * TypeDefLink  entityType   which contains name and guid     -> set Atlas type
         * String       attributeName                                 -> set Atlas name
         * String       attributeDescription                          -> set Atlas description
         * String       attributeDescriptionGUID                      -> IGNORED
         * AttributeCardinality attributeCardinality                  -> set Atlas cardinality
         * In Atlas RED we need the following fields - which are set as above, unless noted differently:
         *
         * Note that the attribute naming is transposed between the OM and Atlas models, so we swap the
         * names - i.e. the attrName of OM end1 will be used for the attribute name of Atlas end2 and v.v.
         * This is because Atlas stores the relationship ends as literal objects - i.e. the object will contain
         * the attributeName that will be used to refer to the other (relationship) end. - e.g. the RelationshipEndDef
         * for a Glossary that has a TermAnchor relationship to a Term will be stored as type="Glossary";attributeName="terms".
         * This is the opposite to OM which defines the RelEndDef for Glossary as type="Glossary"; attributeName="anchor" - ie.
         * the name by which the Glossary will be 'known' (referred to) from a Term. Therefore when converting between
         * OM and Atlas we must switch the attributeNames between ends 1 and 2.
         *
         * String type;
         * String name;
         * boolean isContainer;        <--will be set from OM RCE after both ends have been established
         * Cardinality cardinality;
         * boolean isLegacyAttribute;  <-- will always be set to false.
         * String description;
         *
         * The connector take the various parts of the RelationshipDef in good faith and does not perform existence
         * checking or comparison - e.g. that the entity defs exist. That is delegated to Atlas since it has that
         * checking already.
         */

        // Get both the OM end defs and validate them. We also need both their types up front - so we can swap them over.


        // END1
        //RelationshipEndDef            endDef1                   ->  AtlasRelationshipEndDef endDef1;
        RelationshipEndDef omEndDef1 = omRelationshipDef.getEndDef1();
        // An OM end def must have a type and an attributeName, otherwise it will fail to register in Atlas
        TypeDefLink omTDL1 = omEndDef1.getEntityType();
        String attributeName1 = omEndDef1.getAttributeName();
        String attributeDescription1 = omEndDef1.getAttributeDescription();
        if (attributeName1 == null || omTDL1.getName() == null || omTDL1.getGUID() == null) {
            // There is not enough information to create the relationship end - and hence the relationship def
            LOG.error("convertOMRelationshipDef: Failed to convert OM RelationshipDef {} - end1 partially defined; name {}, type {}",
                    omRelationshipDef.getName(), attributeName1, omTDL1.getName());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "convertOMRelationshipDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // END2
        RelationshipEndDef omEndDef2 = omRelationshipDef.getEndDef2();
        TypeDefLink omTDL2 = omEndDef2.getEntityType();
        String attributeName2 = omEndDef2.getAttributeName();
        String attributeDescription2 = omEndDef2.getAttributeDescription();
        if (attributeName2 == null || omTDL2.getName() == null || omTDL2.getGUID() == null) {
            // There is not enough information to create the relationship end - and hence the relationship def
            LOG.error("convertOMRelationshipDef: Failed to convert OM RelationshipDef {} - end2 partially defined; name {}, type {}",
                    omRelationshipDef.getName(), attributeName2, omTDL2.getName());
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "convertOMRelationshipDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        /* Process END1
         * Note that hen converting from Atlas to OM or vice versa, the attribute name and cardinality needs to be switched
         * from one end to the other.
         */
        AtlasRelationshipEndDef atlasEndDef1 = new AtlasRelationshipEndDef();
        atlasEndDef1.setName(omEndDef2.getAttributeName());   // attribute names are deliberately transposed as commented above
        atlasEndDef1.setDescription(attributeDescription2);   // attribute descriptions are deliberately transposed as above
        // Ends with entity types in famous five need to be converted...
        String omTypeName1 = omTDL1.getName();
        String omTypeGUID1 = omTDL1.getGUID();
        String atlasTypeName1 = omTypeName1;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName1)) {
            atlasTypeName1 = FamousFive.getAtlasTypeName(omTypeName1, omTypeGUID1);
        }
        atlasEndDef1.setType(atlasTypeName1);
        LOG.debug("Atlas end1 is {}", atlasEndDef1);


        atlasEndDef1.setIsLegacyAttribute(false);

        /* Cardinality
         * Cardinality is mapped from OM to Atlas Cardinality { SINGLE, LIST, SET }.
         * An OM relationship end def has cardinality always interpreted in the 'optional' sense, so that
         * the relationship ends are 0..1 or 0..* - allowing us to create a relationship instance between a pair of entity
         * instances as a one-to-many, many-to-one, many-to-many.
         */
        RelationshipEndCardinality omEndDef1Card = omEndDef2.getAttributeCardinality(); // attribute cardinality deliberately transposed as commented above
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasEndDef1Card;
        switch (omEndDef1Card) {

            case AT_MOST_ONE:
                atlasEndDef1Card = SINGLE;
                break;

            case ANY_NUMBER:
                atlasEndDef1Card = SET;
                break;

            default:
                /* Any other cardinality is unexpected - all OM RelationshipDefs are associations with optional, unordered ends.
                 * There is no sensible way to proceed here.
                 */
                 LOG.error("convertOMRelationshipDef: OM cardinality {} not valid for relationship end def 1 in relationship def {}", omEndDef1Card, omRelationshipDef.getName());

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "convertOMRelationshipDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }
        atlasEndDef1.setCardinality(atlasEndDef1Card);
        atlasRelationshipDef.setEndDef1(atlasEndDef1);

        /* Process END2
         * Note that hen converting from Atlas to OM or vice versa, the attribute name and cardinality needs to be switched
         * from one end to the other.
         */

        AtlasRelationshipEndDef atlasEndDef2 = new AtlasRelationshipEndDef();
        atlasEndDef2.setName(omEndDef1.getAttributeName());     // attribute names are deliberately transposed as commented above
        atlasEndDef2.setDescription(attributeDescription1);     // attribute descriptions are deliberately transposed as above
        // Ends with entity types in famous five need to be converted...
        String omTypeName2 = omTDL2.getName();
        String omTypeGUID2 = omTDL2.getGUID();
        String atlasTypeName2 = omTypeName2;
        if (FamousFive.omTypeRequiresSubstitution(omTypeName2)) {
            atlasTypeName2 = FamousFive.getAtlasTypeName(omTypeName2, omTypeGUID2);
        }
        atlasEndDef2.setType(atlasTypeName2);
        atlasEndDef2.setDescription(omEndDef2.getAttributeDescription());
        atlasEndDef2.setIsLegacyAttribute(false);

        /* Cardinality
         * Cardinality is mapped from OM to Atlas Cardinality { SINGLE, LIST, SET }.
         * An OM relationship end def has cardinality always interpreted in the 'optional' sense, so that
         * the relationship ends are 0..1 or 0..* - allowing us to create a relationship instance between a pair of entity
         * instances as a one-to-many, many-to-one, many-to-many.
         */
        RelationshipEndCardinality omEndDef2Card = omEndDef1.getAttributeCardinality();  // attribute cardinality deliberately transposed as commented a
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasEndDef2Card;
        switch (omEndDef2Card) {
            case AT_MOST_ONE:
                atlasEndDef2Card = SINGLE;
                break;

            case ANY_NUMBER:
                atlasEndDef2Card = SET;
                break;

            default:
                /* Any other cardinality is unexpected - all OM RelationshipDefs are associations with optional, unordered ends.
                 * There is no sensible way to proceed here.
                 */
                LOG.error("convertOMRelationshipDef: OM cardinality {} not valid for relationship end def 2 in relationship def {}", omEndDef2Card, omRelationshipDef.getName());

                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage( omRelationshipDef.getName(), omRelationshipDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omRelationshipDef.toString());

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "convertOMRelationshipDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }
        atlasEndDef2.setCardinality(atlasEndDef2Card);
        atlasRelationshipDef.setEndDef2(atlasEndDef2);

        /* If in future OM RelationshipDef supports COMPOSITION and/or AGGREGATION, then set the container end.
         * Whilst only ASSOCIATION is supported, neither end is a container.
         */
         //RelationshipContainerEnd omRCE = omRelationshipDef.getRelationshipContainerEnd();
         //switch (omRCE) {
         //   case END1:
         //       atlasEndDef1.setIsContainer(true);
         //       break;
         //   case END2:
         //       atlasEndDef2.setIsContainer(true);
         //       break;
         //   default:
         //       LOG.debug("Cannot convert OM RelationshipContainerEnd {} to Atlas value", omRCE);
         //       break;
         //}
        /*
         * For now a RelationshipDef is always an ASSOCIATION, so neither end is a container
         */
        atlasEndDef1.setIsContainer(false);
        atlasEndDef2.setIsContainer(false);

        /* Set Atlas Attribute defs
         * OMRS ArrayList<TypeDefAttribute> --> Atlas List<AtlasAttributeDef>
         * Retrieve the OM RelationshipDef attributes:
         */
        List<TypeDefAttribute> omAttrs = omRelationshipDef.getPropertiesDefinition();
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttrs = convertOMAttributeDefs(omAttrs);
        atlasRelationshipDef.setAttributeDefs(atlasAttrs);

        /*
         * Return the AtlasRelationshipDef
         */
        LOG.debug("AtlasRelationshipDef is {}", atlasRelationshipDef);
        return atlasRelationshipDef;
    }


    /*
     * Parse the OM ClassificationDef and create an AtlasClassificationDef
     * @param omClassificationDef - the OM classification def to convert
     * @return - the Atlas classification def
     */
    private AtlasClassificationDef convertOMClassificationDefToAtlasClassificationDef(ClassificationDef omClassificationDef) {


        LOG.debug("convertOMClassificationDef: OM ClassificationDef {}", omClassificationDef);

        if (omClassificationDef == null) {
            return null;
        }

        /*
         * OM ClassificationDef                                             AtlasClassificationDef
         * --------------------                                             ----------------------
         * ArrayList<TypeDefLink>               validEntityDefs          -> entityTypes
         * . boolean                            propagatable             -> IGNORED
         * TypeDefLink                          superType                -> superTypes
         * . String                             description              -> description
         * . String                             descriptionGUID          -> IGNORED
         * . String                             origin                   -> IGNORED
         * . String                             createdBy                -> createdBy
         * . String                             updatedBy                -> updatedBy
         * . Date                               createTime               -> createTime
         * . Date                               updateTime               -> updateTime
         * . Map<String, String>                options                  -> options
         * . ArrayList<ExternalStandardMapping> externalStandardMappings -> IGNORED
         * . ArrayList<InstanceStatus>          validInstanceStatusList  -> IGNORED
         * . InstanceStatus                     initialStatus            -> IGNORED
         * . ArrayList<TypeDefAttribute>        propertiesDefinition     -> attributeDefs
         * . Long                               version                  -> version
         * . String                             versionName              -> typeVersion
         * . TypeDefCategory                    category                 -> NOT NEEDED Atlas Cat set by CTOR
         * . String                             uid                      -> guid
         * . String                             name                     -> name
         *
         */

        // Convert OM type into a valid Atlas type

        // Allocate AtlasClassificationDef, which will set TypeCategory automatically
        AtlasClassificationDef atlasClassificationDef = new AtlasClassificationDef();

        // Set common fields
        atlasClassificationDef.setGuid(omClassificationDef.getGUID());
        atlasClassificationDef.setName(omClassificationDef.getName());
        atlasClassificationDef.setDescription(omClassificationDef.getDescription());
        atlasClassificationDef.setVersion(omClassificationDef.getVersion());
        atlasClassificationDef.setTypeVersion(omClassificationDef.getVersionName());
        atlasClassificationDef.setCreatedBy(omClassificationDef.getCreatedBy());
        atlasClassificationDef.setUpdatedBy(omClassificationDef.getUpdatedBy());
        atlasClassificationDef.setCreateTime(omClassificationDef.getCreateTime());
        atlasClassificationDef.setUpdateTime(omClassificationDef.getUpdateTime());
        atlasClassificationDef.setOptions(omClassificationDef.getOptions());

        // Handle fields that require conversion - i.e. supertypes and validEntityDefs

        // Set the (at most one) superType
        // Note that there is no error or existence checking on this
        TypeDefLink omSuperType = omClassificationDef.getSuperType();
        atlasClassificationDef.setSuperTypes(null);
        if (omSuperType != null) {
            String atlasSuperType = omSuperType.getName();
            Set<String> atlasSuperTypes = new HashSet<>();
            atlasSuperTypes.add(atlasSuperType);
            atlasClassificationDef.setSuperTypes(atlasSuperTypes);
        }

        // Set the validEntityDefs
        // For each TypeDefLink in the OM list of VEDs, extract the name into a Set<String> for Atlas.
        // Note that there is no error or existence checking on this
        List<TypeDefLink> omVEDs = omClassificationDef.getValidEntityDefs();
        Set<String> atlasEntityTypes = null;
        if (omVEDs != null) {
            atlasEntityTypes = new HashSet<>();
            for (TypeDefLink tdl : omVEDs) {
                LOG.debug("convertOMClassificationDef: process OM VED {}", tdl.getName());
                String omEntityTypeName = tdl.getName();
                String omEntityTypeGUID = tdl.getGUID();
                String atlasEntityTypeName = omEntityTypeName;
                if (FamousFive.omTypeRequiresSubstitution(omEntityTypeName)) {
                    atlasEntityTypeName = FamousFive.getAtlasTypeName(omEntityTypeName, omEntityTypeGUID);
                    LOG.debug("convertOMClassificationDef: substituted type name {}", atlasEntityTypeName);
                }
                atlasEntityTypes.add(atlasEntityTypeName);
            }
        }
        atlasClassificationDef.setEntityTypes(atlasEntityTypes);

        // Set Atlas Attribute defs
        List<TypeDefAttribute> omAttrs = omClassificationDef.getPropertiesDefinition();
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttrs = convertOMAttributeDefs(omAttrs);
        atlasClassificationDef.setAttributeDefs(atlasAttrs);

        // Return the AtlasClassificationDef
        return atlasClassificationDef;
    }


    // Accept an AtlasTypesDef and populate the typeDefsForAPI so it contains all OM defs corresponding to
    // defs encountered in the AtlasTypesDef
    //
    private void convertAtlasTypeDefs(String        userId,
                                      AtlasTypesDef atd)
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> convertAtlasTypeDefs(userId={}, atd={}", userId, atd);
        }

        if (atd == null) {
            return;
        }

        // This method could walk the AtlasTypesDef in any order because discovered defs are marshalled
        // by the typeDefsForAPI and converted into desired output order later. In order to allow the
        // connector to discover dependencies before they are required, use the following order:
        //
        // i.e. enums -> entities -> relationships -> classifications ( structs are ignored )
        //
        // For example a classificationDef may refer to an entityDef in its list of validEntityDefs.
        //
        // Each category method will handle all the typedefs it can - i.e. all those that result in valid
        // OM typedefs. Not all Atlas type defs can be modelled - e.g. an Atlas def could have multiple
        // superTypes. Any problems with conversion of the AtlasTypeDefs are handled internally by the individual
        // parsing methods, and each method returns a list of the OM defs it could
        // generate from the given AtlasTypeDefs.

        // Process enums
        List<AtlasEnumDef> enumDefs = atd.getEnumDefs();
        processAtlasEnumDefs(enumDefs);
        LOG.debug("convertAtlasTypeDefs: process enums returned {}", typeDefsForAPI.getEnumDefs());

        // Process entities
        List<AtlasEntityDef> entityDefs = atd.getEntityDefs();
        processAtlasEntityDefs(userId, entityDefs);
        LOG.debug("convertAtlasTypeDefs: process entities returned {}", typeDefsForAPI.getEntityDefs());

        // Process relationships
        List<AtlasRelationshipDef> relationshipDefs = atd.getRelationshipDefs();
        processAtlasRelationshipDefs(userId, relationshipDefs);
        LOG.debug("convertAtlasTypeDefs: process relationships returned {}", typeDefsForAPI.getRelationshipDefs());

        // Process classifications
        List<AtlasClassificationDef> classificationDefs = atd.getClassificationDefs();
        processAtlasClassificationDefs(userId, classificationDefs);
        LOG.debug("convertAtlasTypeDefs: process classifications returned {}", typeDefsForAPI.getClassificationDefs());

        // Structs are explicitly ignored

        //LOG.debug("convertAtlasTypeDefs: complete, typeDefsForAPI {}", typeDefsForAPI);
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== convertAtlasTypeDefs(atd={}", atd);
        }

    }


    // Convert a list of AtlasEnumDef to a typeDefsForAPI; each convertible typedef is added to the typeDefsForAPI member variable.
    //
    private void processAtlasEnumDefs(List<AtlasEnumDef> atlasEnumDefs) {

        if (atlasEnumDefs != null) {
            for (AtlasEnumDef atlasEnumDef : atlasEnumDefs) {
                try {
                    processAtlasEnumDef(atlasEnumDef);
                } catch (TypeErrorException e) {
                    LOG.error("processAtlasEnumDefs: gave up on AtlasEnumDef {}", atlasEnumDef.getName(), e);
                    // swallow the exception to proceed with the next type...
                }
            }
        }
    }



    // Convert a list of AtlasEntityDef saving each into the typeDefsForAPI
    //
    private void processAtlasEntityDefs(String               userId,
                                        List<AtlasEntityDef> atlasEntityDefs)
    {

        if (atlasEntityDefs != null) {
            for (AtlasEntityDef atlasEntityDef : atlasEntityDefs) {
                try {
                    processAtlasEntityDef(userId, atlasEntityDef);
                }
                catch (TypeErrorException | RepositoryErrorException e) {
                    LOG.error("processAtlasEntityDefs: gave up on AtlasEntityDef {}",atlasEntityDef.getName(), e);
                    // swallow the exception to proceed with the next type...
                }
            }
        }
    }

    // Convert a list of AtlasClassificationDef to a typeDefsForAPI
    //
    private void processAtlasClassificationDefs(String                       userId,
                                                List<AtlasClassificationDef> atlasClassificationDefs) {

        if (atlasClassificationDefs != null) {
            for (AtlasClassificationDef atlasClassificationDef : atlasClassificationDefs) {
                try {
                    processAtlasClassificationDef(userId, atlasClassificationDef);
                }
                catch (TypeErrorException | RepositoryErrorException e) {
                    LOG.error("processAtlasClassificationDefs: gave up on AtlasClassificationDef {}",atlasClassificationDef.getName(), e);
                    // swallow the exception to proceed with the next type...
                }

            }
        }
    }



    // Convert a list of AtlasRelationshipDef to a typeDefsForAPI
    //
    private void processAtlasRelationshipDefs(String                     userId,
                                              List<AtlasRelationshipDef> atlasRelationshipDefs) {

        if (atlasRelationshipDefs != null) {
            for (AtlasRelationshipDef atlasRelationshipDef : atlasRelationshipDefs) {
                try {
                    processAtlasRelationshipDef(userId, atlasRelationshipDef);
                }
                catch (TypeErrorException | RepositoryErrorException e) {
                    LOG.error("processAtlasRelationshipDefs: gave up on AtlasRelationshipDef {}", atlasRelationshipDef.getName(), e);
                    // swallow the exception to proceed with the next type...
                }

            }
        }
    }




    // Convert an AtlasEnumDef to an OM EnumDef
    //
    // 1. Convert the AtlasEnumDef into the corresponding OM EnumDef and (implicitly) validate the content of the OM EnumDef
    // 2. Query RCM to find whether it knows of an EnumDef with the same name:
    //    a. If exists, retrieve the known typedef from RCM and deep compare the known and new EnumDefs.
    //       i.  If same use the existing type's GUID; add def to TypeDefGallery;
    //       ii. If different fail (audit log)
    //    b. If not exists (in RCM), generate a GUID and add to TypeDefGallery.
    //
    // Error handling: If at any point we decide not to proceed with the conversion of this EnumDef, we must log
    // the details of the error condition and return to the caller without having added the OM EnumDef to typeDefsForAPI.
    //
    // package private
    void processAtlasEnumDef(AtlasEnumDef atlasEnumDef)
            throws
            TypeErrorException
    {

        final String methodName = "processAtlasEnumDef";

        LOG.debug("processAtlasEnumDef: convert AtlasEnumDef {}", atlasEnumDef);

        if (atlasEnumDef == null) {
            return;
        }

        String typeName = atlasEnumDef.getName();

        // 1. Convert Atlas type into a valid OM type

        // Allocate OMRS EnumDef, which will set AttributeTypeDefCategory automatically
        EnumDef omrsEnumDef = new EnumDef();

        // Set common fields
        omrsEnumDef.setGUID(atlasEnumDef.getGuid());
        omrsEnumDef.setName(atlasEnumDef.getName());
        omrsEnumDef.setDescription(atlasEnumDef.getDescription());
        // DescriptionGUID is set below iff this is a new typedef.

        // Additional fields on an AtlasEnumDef and OM EnumDef are elementDefs and defaultValue
        // Initialize the Atlas and OMRS default values so we can test and set default value in the loop
        String atlasDefaultValue = atlasEnumDef.getDefaultValue();
        EnumElementDef omrsDefaultValue = null;

        List<AtlasEnumDef.AtlasEnumElementDef> atlasElemDefs = atlasEnumDef.getElementDefs();
        ArrayList<EnumElementDef> omrsElementDefs = null;
        if (atlasElemDefs != null) {
            omrsElementDefs = new ArrayList<>();
            for (AtlasEnumDef.AtlasEnumElementDef atlasElementDef : atlasElemDefs) {
                EnumElementDef omrsEnumElementDef = new EnumElementDef();
                omrsEnumElementDef.setValue(atlasElementDef.getValue());
                omrsEnumElementDef.setDescription(atlasElementDef.getDescription());
                omrsEnumElementDef.setOrdinal(atlasElementDef.getOrdinal());
                omrsElementDefs.add(omrsEnumElementDef);
                if (atlasElementDef.getValue().equals(atlasDefaultValue)) {
                    omrsDefaultValue = omrsEnumElementDef;
                }
            }
        }
        omrsEnumDef.setElementDefs(omrsElementDefs);
        omrsEnumDef.setDefaultValue(omrsDefaultValue);

        // 2. Query RepositoryContentManager to find whether it knows of an EnumDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.

        // Ask RepositoryContentManager whether there is an EnumDef with same name as typeName
        String source = metadataCollectionId;
        AttributeTypeDef existingAttributeTypeDef;
        try {
            existingAttributeTypeDef = repositoryHelper.getAttributeTypeDefByName(source, typeName);

        } catch (OMRSLogicErrorException e) {
            // Fail the conversion - this can be achieved by simply returning before completion
            LOG.error("processAtlasEnumDef: caught exception from RepositoryHelper", e);
            return;
        }

        if (existingAttributeTypeDef == null) {
            LOG.debug("processAtlasEnumDef: repository content manager returned name not found - proceed to publish");
            // Use the candidate attribute type def
            // Check (by name) whether we have already added one to current TDBC - e.g. if there are
            // multiple attributes of the same type - they should refer to the same ATD in TDBC.

            // No existing ATD was found in RH.
            // If it does not already exist add the new ATD to the ATDs in the TypeDefGallery and use it in TDA.
            // If there is already a TDBC copy of the ATD then use that - avoid duplication.
            EnumDef tdbcCopy = typeDefsForAPI.getEnumDef(omrsEnumDef.getName());
            if (tdbcCopy != null)
                omrsEnumDef = tdbcCopy;
            // Add the OM typedef to TDBC - this will get it into the TypeDefGallery
            typeDefsForAPI.addEnumDef(omrsEnumDef);
            // In future may want to generate a descriptionGUID here, but for now these are not used
            omrsEnumDef.setDescriptionGUID(null);

        } else {

            LOG.debug("processAtlasEnumDef: repositoryHelper has an AttributeTypeDef with name {} : {}", typeName, existingAttributeTypeDef);
            if (existingAttributeTypeDef.getCategory() == ENUM_DEF) {
                // LOG.debug("processAtlasEnumDef: existing AttributeTypeDef has category {} ", existingAttributeTypeDef.getCategory());
                // There is an EnumDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                EnumDef existingEnumDef = (EnumDef) existingAttributeTypeDef;
                boolean typematch = comp.compare(existingEnumDef, omrsEnumDef);
                // If compare matches then we can proceed to publish the def
                if (typematch) {
                    // There is exact match in the ReposHelper - we will add that to our TypeDefGallery
                    omrsEnumDef = existingEnumDef;
                    // Add the OM typedef to TDBC - this will get it into the TypeDefGallery
                    typeDefsForAPI.addEnumDef(omrsEnumDef);
                } else {
                    // If compare failed abandon processing of this EnumDef
                    LOG.debug("processAtlasEnumDef: existing AttributeTypeDef did not match");

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(typeName, omrsEnumDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omrsEnumDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "processAtlasEnumDef",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }

            } else {
                // There is a type of this name but it is not an EnumDef - fail!
                LOG.debug("processAtlasEnumDef: existing AttributeTypeDef not an EnumDef - has category {} ", existingAttributeTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(typeName, omrsEnumDef.getGUID(), "omRelationshipDef", methodName, repositoryName, omrsEnumDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "processAtlasEnumDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }


    }


    // Convert an AtlasClassificationDef to an OMRS ClassificationDef
    //
    // 1. Convert the AtlasClassificationDef into the corresponding OM ClassificationDef and (implicitly) validate the content of the OM ClassificationDef
    // 2. Query RCM to find whether it knows of an ClassificationDef with the same name:
    //    a. If exists, retrieve the known typedef from RCM and deep compare the known and new ClassificationDefs.
    //       i.  If same use the existing type's GUID; add def to TypeDefGallery;
    //       ii. If different fail (audit log)
    //    b. If not exists (in RCM), generate a GUID and add to TypeDefGallery.
    //
    // Error handling: If at any point we decide not to proceed with the conversion of this ClassificationDef, we must log
    // the details of the error condition and return to the caller without having added the omrsClassificationDef to typeDefsForAPI.
    //
    private void processAtlasClassificationDef(String                 userId,
                                               AtlasClassificationDef atlasClassificationDef)
        throws
            TypeErrorException,
            RepositoryErrorException
    {

        final String methodName = "processAtlasClassificationDef";

        LOG.debug("processAtlasClassificationDef: AtlasClassificationDef {}", atlasClassificationDef);

        String typeName = atlasClassificationDef.getName();

        // Create an AtlasClassificationDefMapper to convert to an OM ClassificationDef, then invoke the RH to verify
        // whether a type with the same name is already known, and if it compares.
        // Finally add the ClassificationDef to the TDBC typeDefsForAPI.

        ClassificationDef classificationDef;
        AtlasClassificationDefMapper atlasClassificationDefMapper;
        try {
            atlasClassificationDefMapper = new AtlasClassificationDefMapper(this, userId, atlasClassificationDef);
            classificationDef = atlasClassificationDefMapper.toOMClassificationDef();
        }
        catch (RepositoryErrorException e) {
            LOG.error("processAtlasClassificationDef: could not initialise mapper", e);
            throw e;
        }
        catch (TypeErrorException e) {
            LOG.error("processAtlasClassificationDef: could not convert AtlasEntityDef {} to OM EntityDef", typeName, e);
            throw e;
        }

        if (classificationDef == null) {
            LOG.error("processAtlasClassificationDef: could not convert AtlasClassificationDef {} to OM ClassificationDef", typeName);
            return;
        }

        LOG.debug("processAtlasClassificationDef: AtlasClassificationDef mapped to OM ClassificationDef {}", classificationDef);

        // Query RepositoryContentManager to find whether it knows of a TypeDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.

        // Error handling:
        // If at any point we decide not to proceed with the conversion of this ClassificationDef, we must log
        // the details of the error condition and return to the caller without having added the omrsClassificationDef to typeDefsForAPI.

        // Ask RepositoryContentManager whether there is a known TypeDef with same name
        String source = metadataCollectionId;
        TypeDef existingTypeDef;
        try {
            existingTypeDef = repositoryHelper.getTypeDefByName(source, typeName);
        } catch (OMRSLogicErrorException e) {
            // Fail the conversion by returning without adding the type def to the TDBC
            LOG.error("processAtlasClassificationDef: caught exception from RepositoryHelper", e);
            return;
        }

        if (existingTypeDef == null) {
            LOG.debug("processAtlasClassificationDef: repository content manager returned name not found - proceed to publish");
            // In future may want to generate a descriptionGUID, but for now these are not used
            classificationDef.setDescriptionGUID(null);
        }
        else {
            LOG.debug("processAtlasClassificationDef: there is a TypeDef with name {} : {}", typeName, existingTypeDef);

            if (existingTypeDef.getCategory() == CLASSIFICATION_DEF) {
                LOG.debug("processAtlasClassificationDef: existing TypeDef has category {} ", existingTypeDef.getCategory());
                // There is a ClassificationDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                ClassificationDef existingClassificationDef = (ClassificationDef) existingTypeDef;
                boolean typematch = comp.equivalent(existingClassificationDef, classificationDef);
                // If compare matches use the known type
                if (typematch) {
                    // We will add the typedef to the TypeDefGallery
                    LOG.debug("processAtlasClassificationDef: repository content manager found matching def with name {}", typeName);
                    classificationDef = existingClassificationDef;
                }
                else {
                    // If compare failed abandon processing of this ClassificationDef
                    LOG.debug("processAtlasClassificationDef: existing TypeDef did not match");

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(classificationDef.getName(), classificationDef.getGUID(), "classificationDef", methodName, repositoryName, classificationDef.toString());


                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "processAtlasClassificationDef",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
            else {
                // There is a type of this name but it is not a ClassificationDef - fail!
                LOG.debug("processAtlasClassificationDef: existing TypeDef not a ClassificationDef - has category {} ", existingTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(classificationDef.getName(), classificationDef.getGUID(), "classificationDef", methodName, repositoryName, classificationDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "processAtlasClassificationDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

        }

        // If we reached this point then we are good to go
        // Add the OM typedef to discoveredTypeDefs - this will get it into the TypeDefGallery
        LOG.debug("convertClassificationDef: OMRS ClassificationDef {}", classificationDef);
        // Add the OM typedef to TDBC - this will ultimately get it into the TypeDefGallery
        typeDefsForAPI.addClassificationDef(classificationDef);

    }





    private void processAtlasEntityDef(String userId, AtlasEntityDef atlasEntityDef)
            throws TypeErrorException, RepositoryErrorException
    {

        final String methodName = "processAtlasEntityDef";

        // Create an AtlasEntityDefMapper to convert to an OM EntityDef, then invoke the RH to verify
        // whether a type with the same name is already known, and if it compares.
        // Finally add the EntityDef to the TDBC typeDefsForAPI.

        String typeName = atlasEntityDef.getName();

        // Famous Five
        // Here we need to ensure that we do not process an Atlas Famous Five type
        // This may look odd because we use the omXX query method - this is because we need to to know whether,
        // if the Atlas type name were an OM type name, would it have been substituted?
        if (FamousFive.omTypeRequiresSubstitution(typeName)) {
            LOG.debug("processAtlasEntityDef: skip type {}", typeName);
            return;
        }

        AtlasEntityDefMapper atlasEntityDefMapper;
        EntityDef entityDef;
        try {
            atlasEntityDefMapper = new AtlasEntityDefMapper(this, userId, atlasEntityDef);
            entityDef = atlasEntityDefMapper.toOMEntityDef();
        }
        catch (RepositoryErrorException e) {
            LOG.error("processAtlasEntityDef: could not initialize mapper", e);
            throw e;
        }
        catch (TypeErrorException e) {
            LOG.error("processAtlasEntityDef: could not convert AtlasEntityDef {} to OM EntityDef", typeName, e);
            throw e;
        }

        if (entityDef == null) {
            LOG.error("processAtlasEntityDef: no OM EntityDef for type {}", typeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("entityDef", "load", metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "processAtlasEntityDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        LOG.debug("processAtlasEntityDef: AtlasEntityDef mapped to OM EntityDef {}", entityDef);

        // Query RepositoryContentManager to find whether it knows of a TypeDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.
        //
        // Error handling:
        // If at any point we decide not to proceed with the conversion of this EntityDef, we must log
        // the details of the error condition and return to the caller without having added the omEntityDef to typeDefsForAPI.

        // Ask RepositoryContentManager whether there is a known TypeDef with same name
        String source = metadataCollectionId;
        TypeDef existingTypeDef;
        try {
            existingTypeDef = repositoryHelper.getTypeDefByName(source, entityDef.getName());
        } catch (OMRSLogicErrorException e) {
            // Fail the conversion by returning without adding the type def to the TDBC
            LOG.error("processAtlasEntityDef: caught exception from RepositoryHelper", e);
            return;
        }

        if (existingTypeDef == null) {
            LOG.debug("processAtlasEntityDef: repository content manager returned name not found - proceed to publish");
            // In future may want to generate a descriptionGUID, but for now these are not used
            entityDef.setDescriptionGUID(null);
        }
        else {
            LOG.debug("processAtlasEntityDef: there is a TypeDef with name {} : {}", entityDef.getName(), existingTypeDef);

            if (existingTypeDef.getCategory() == ENTITY_DEF) {
                LOG.debug("processAtlasEntityDef: existing TypeDef has category {} ", existingTypeDef.getCategory());
                // There is a EntityDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                EntityDef existingEntityDef = (EntityDef) existingTypeDef;
                boolean typematch = comp.equivalent(existingEntityDef, entityDef);
                // If compare matches use the known type
                if (typematch) {
                    // We will add the typedef to the TypeDefGallery
                    LOG.debug("processAtlasEntityDef: repository content manager found matching def with name {}", entityDef.getName());
                    entityDef = existingEntityDef;
                }
                else {
                    // If compare failed abandon processing of this EntityDef
                    LOG.debug("processAtlasEntityDef: existing TypeDef did not match");

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage( entityDef.getName(), entityDef.getGUID(), "atlasEntityDef", methodName, repositoryName, entityDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "processAtlasEntityDef",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
            else {
                // There is a type of this name but it is not an EntityDef - fail!
                LOG.debug("processAtlasEntityDef: existing TypeDef not an EntityDef - has category {} ", existingTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage( entityDef.getName(), entityDef.getGUID(), "atlasEntityDef", methodName, repositoryName, entityDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "processAtlasEntityDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        // Add the OM typedef to TDBC - this will ultimately get it into the TypeDefGallery
        typeDefsForAPI.addEntityDef(entityDef);

    }


    private void processAtlasRelationshipDef(String userId, AtlasRelationshipDef atlasRelationshipDef)
            throws TypeErrorException, RepositoryErrorException
    {

        final String methodName = "processAtlasRelationshipDef";

        // Create an AtlasRelationshipDefMapper to convert to an OM RelationshipDef, then invoke the RH to verify
        // whether a type with the same name is already known, and if it compares.
        // Finally add the EntityDef to the TDBC typeDefsForAPI.

        String typeName = atlasRelationshipDef.getName();

        RelationshipDef relationshipDef;
        try {
            AtlasRelationshipDefMapper atlasRelationshipDefMapper = new AtlasRelationshipDefMapper(this, userId, atlasRelationshipDef);
            relationshipDef = atlasRelationshipDefMapper.toOMRelationshipDef();
        }
        catch (RepositoryErrorException e) {
            LOG.error("processAtlasClassificationDef: could not initialize mapper", e);
            throw e;
        }
        catch (TypeErrorException e) {
            LOG.error("processAtlasClassificationDef: could not convert AtlasEntityDef {} to OM EntityDef", typeName, e);
            throw e;
        }

        if (relationshipDef == null) {
            LOG.error("processAtlasRelationshipDef: could not convert AtlasRelationshipDef {} to OM RelationshipDef", typeName);
            return;
        }

        LOG.debug("processAtlasRelationshipDef: AtlasRelationshipDef mapped to OM EntityDef {}", relationshipDef);


        // Query RepositoryContentManager to find whether it knows of a RelationshipDef with the same name
        // If the type already exists (by name) perform a deep compare.
        // If there is no existing type (with this name) or there is an exact (deep) match we can publish the type
        // If there is an existing type that does not deep match exactly then we cannot publish the type.

        // Ask RepositoryContentManager whether there is a RelationshipDef with same name as typeName

        String source = metadataCollectionId;
        TypeDef existingTypeDef;
        try {
            existingTypeDef = repositoryHelper.getTypeDefByName(source, typeName);
        } catch (OMRSLogicErrorException e) {
            // Fail the conversion by returning without adding the def to the TypeDefGallery
            LOG.error("processAtlasRelationshipDef: caught exception from RepositoryHelper", e);
            return;
        }
        if (existingTypeDef == null) {
            LOG.debug("processAtlasRelationshipDef: repository content manager returned name not found - proceed to publish");
            // In future may want to generate a descriptionGUID, but for now these are not used
            relationshipDef.setDescriptionGUID(null);
        } else {
            LOG.debug("processAtlasRelationshipDef: there is a TypeDef with name {} : {}", typeName, existingTypeDef);

            if (existingTypeDef.getCategory() == RELATIONSHIP_DEF) {
                // There is a RelationshipDef with this name - perform deep compare and only publish if exact match
                // Perform a deep compare of the known type and new type
                Comparator comp = new Comparator();
                RelationshipDef existingRelationshipDef = (RelationshipDef) existingTypeDef;
                boolean typematch = comp.equivalent(existingRelationshipDef, relationshipDef);
                // If compare matches use the known type
                if (typematch) {
                    // We will add the typedef to the TypeDefGallery
                    LOG.debug("processAtlasRelationshipDef: repository content manager found matching def with name {}", typeName);
                    relationshipDef = existingRelationshipDef;
                } else {

                    // If compare failed abandon processing of this RelationshipDef
                    LOG.debug("processAtlasRelationshipDef: existing TypeDef did not match");

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage( relationshipDef.getName(), relationshipDef.getGUID(), "atlasRelationshipDef", methodName, repositoryName, relationshipDef.toString());

                    throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            "processAtlasRelationshipDef",
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
            else {
                // There is a type of this name but it is not a RelationshipDef - fail!
                LOG.error("processAtlasRelationshipDef: existing TypeDef not an RelationshipDef - has category {} ", existingTypeDef.getCategory());
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage( relationshipDef.getName(), relationshipDef.getGUID(), "atlasRelationshipDef", methodName, repositoryName, relationshipDef.toString());

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "processAtlasRelationshipDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
        }

        // If we reached this point then we are good to go
        // Add the OM typedef to discoveredTypeDefs - this will get it into the TypeDefGallery
        LOG.debug("processAtlasRelationshipDef: OMRS RelationshipDef {}", relationshipDef);
        typeDefsForAPI.addRelationshipDef(relationshipDef);

    }





    // Convert a List of OMRS TypeDefAttribute to a List of AtlasAttributeDef
    private ArrayList<AtlasStructDef.AtlasAttributeDef> convertOMAttributeDefs(List<TypeDefAttribute> omAttrs) {
        ArrayList<AtlasStructDef.AtlasAttributeDef> atlasAttributes;
        if (omAttrs == null || omAttrs.isEmpty()) {
            return null;
        }
        else {
            atlasAttributes = new ArrayList<>();

            for (TypeDefAttribute tda : omAttrs) {
                // Map from OMRS TypeDefAttribute to AtlasAttributeDef
                AtlasStructDef.AtlasAttributeDef aad = convertOMAttributeDef(tda);
                atlasAttributes.add(aad);
            }
        }
        return atlasAttributes;

    }




    private AtlasStructDef.AtlasAttributeDef convertOMAttributeDef(TypeDefAttribute tda) {

        // Convert OM TypeDefAttribute to AtlasAttributeDef
        //

        //
        // The AttributeTypeDef attributeType has:
        // AttributeTypeDefCategory category:
        //   OM defines the following values { UNKNOWN_DEF | PRIMITIVE | COLLECTION | ENUM_DEF }
        //   The corresponding Atlas TypeCategory in each case is:
        //     [OM]               [Atlas]
        //   UNKNOWN_DEF         error condition
        //   PRIMITIVE           PRIMITIVE
        //   COLLECTION          ARRAY or MAP - we can tell which by looking in the (subclass) CollectionDef CollectionDefCategory
        //                                    - which will be one of:
        //                                      OM_COLLECTION_UNKNOWN - treat as an error condition
        //                                      OM_COLLECTION_MAP     - convert to Atlas MAP
        //                                      OM_COLLECTION_ARRAY   - convert to Atlas ARRAY
        //                                      OM_COLLECTION_STRUCT  - treat as an error condition
        //   ENUM_DEF            ENUM
        //   OM guid is ignored - there is no GUID on AtlasAttributeDef
        //   OM name  -> used for AtlasAttributeDef.typeName
        //
        // The OM TypeDefAttribute has some fields that can be copied directly to the AtlasAttributeDef:
        //    [OM]                      [Atlas]
        // attributeName         ->   String name
        // valuesMinCount        ->   int valuesMinCount
        // valuesMaxCount        ->   int valuesMaxCount
        // isIndexable           ->   boolean isIndexable
        // isUnique              ->   boolean isUnique
        // defaultValue          ->   String defaultValue
        // attributeDescription  ->   String description
        //
        // OM attributeDescription --> AtlasAttributeDef.description
        //
        // Map OM Cardinality ----> combination of Atlas boolean isOptional & Cardinality cardinality  - use mapping function
        //   OM                       ->    ATLAS
        //   UNKNOWN                  ->    treat as an error condition
        //   AT_MOST_ONE              ->    isOptional && SINGLE
        //   ONE_ONLY                 ->    !isOptional && SINGLE
        //   AT_LEAST_ONE_ORDERED     ->    !isOptional && LIST
        //   AT_LEAST_ONE_UNORDERED   ->    !isOptional && SET
        //   ANY_NUMBER_ORDERED       ->    isOptional && LIST
        //   ANY_NUMBER_UNORDERED     ->    isOptional && SET

        // There are no constraints in OM so no Atlas constraints are set  List<AtlasConstraintDef> null
        // OM externalStandardMappings is ignored
        //
        LOG.debug("convertOMAttributeDef: OMAttributeDef is {}", tda);

        if (tda == null) {
            return null;
        }

        AtlasStructDef.AtlasAttributeDef aad = null;

        // We need to set the Atlas aad depending on what category of OM typedef we are converting.
        // If the OM def is a primitive or collection then there is no actual Atlas type to create;
        // we are just looking to set the Atlas typeName to one of the primitive type names or to
        // array<x> or map<x,y> as appropriate.
        // If the OM def is an EnumDef then we need to create an AtlasEnumDef and set the typename to
        // refer to it.
        AttributeTypeDef atd = tda.getAttributeType();
        AttributeTypeDefCategory category = atd.getCategory();
        switch (category) {
            case PRIMITIVE:
                aad = convertOMPrimitiveDef(tda);
                break;
            case COLLECTION:
                aad = convertOMCollectionDef(tda);
                break;
            case ENUM_DEF:
                // This is handled by setting the typeName to that of an AtlasEnumDef
                // created by an earlier call to addAttributeTypeDef. If this has not
                // been done then it is valid to bounce this call as an error condition.
                // OM has a TDA with an ATD that has an ATDCategory of ENUM_DEF
                // We want an AAD that uses the typeName of the AtlasEnumDef set to the ATD.name
                aad = convertOMEnumDefToAtlasAttributeDef(tda);
                break;
            case UNKNOWN_DEF:
                LOG.debug("convertOMAttributeDef: cannot convert OM attribute type def with category {}", category);
                break;
        }

        return aad;
    }


    private AtlasStructDef.AtlasAttributeDef convertOMPrimitiveDef(TypeDefAttribute tda) {


        if (tda == null) {
            return null;
        }
        AtlasStructDef.AtlasAttributeDef aad = new AtlasStructDef.AtlasAttributeDef();
        aad.setName(tda.getAttributeName());
        aad.setValuesMinCount(tda.getValuesMinCount());
        aad.setValuesMaxCount(tda.getValuesMaxCount());
        aad.setIsIndexable(tda.isIndexable());
        aad.setIsUnique(tda.isUnique());
        aad.setDefaultValue(tda.getDefaultValue());
        aad.setDescription(tda.getAttributeDescription());
        // Currently setting Atlas cardinality and optionality using a pair of method calls.
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality = convertOMCardinalityToAtlasCardinality(tda.getAttributeCardinality());
        aad.setCardinality(atlasCardinality);
        boolean atlasOptionality = convertOMCardinalityToAtlasOptionality(tda.getAttributeCardinality());
        aad.setIsOptional(atlasOptionality);
        aad.setConstraints(null);
        AttributeTypeDef atd = tda.getAttributeType();
        PrimitiveDef omPrimDef = (PrimitiveDef) atd;
        PrimitiveDefCategory primDefCat = omPrimDef.getPrimitiveDefCategory();
        aad.setTypeName(primDefCat.getName());
        return aad;
    }


    private AtlasStructDef.AtlasAttributeDef convertOMCollectionDef(TypeDefAttribute tda) {


        if (LOG.isDebugEnabled()) {
            LOG.debug("==>convertOMAttributeDef: TypeDefAttribute {}", tda);
        }
        if (tda == null) {
            return null;
        }
        AtlasStructDef.AtlasAttributeDef aad = new AtlasStructDef.AtlasAttributeDef();
        aad.setName(tda.getAttributeName());
        LOG.debug("==>convertOMAttributeDef: attribute name {}", tda.getAttributeName());
        aad.setValuesMinCount(tda.getValuesMinCount());
        aad.setValuesMaxCount(tda.getValuesMaxCount());
        aad.setIsIndexable(tda.isIndexable());
        aad.setIsUnique(tda.isUnique());
        aad.setDefaultValue(tda.getDefaultValue());
        aad.setDescription(tda.getAttributeDescription());
        // Currently setting Atlas cardinality and optionality using a pair of method calls.
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality = convertOMCardinalityToAtlasCardinality(tda.getAttributeCardinality());
        aad.setCardinality(atlasCardinality);
        boolean atlasOptionality = convertOMCardinalityToAtlasOptionality(tda.getAttributeCardinality());
        aad.setIsOptional(atlasOptionality);
        aad.setConstraints(null);
        AttributeTypeDef atd = tda.getAttributeType();
        CollectionDef omCollDef = (CollectionDef) atd;
        String collectionTypeName = omCollDef.getName();
        LOG.debug("==>convertOMAttributeDef: collection type name {}",collectionTypeName);
        CollectionDefCategory collDefCat = omCollDef.getCollectionDefCategory();
        String atlasTypeName;
        switch (collDefCat) {
            case OM_COLLECTION_ARRAY:
                String OM_ARRAY_PREFIX = "array<";
                String OM_ARRAY_SUFFIX = ">";
                int arrayStartIdx = OM_ARRAY_PREFIX.length();
                int arrayEndIdx = collectionTypeName.length() - OM_ARRAY_SUFFIX.length();
                String elementTypeName = collectionTypeName.substring(arrayStartIdx, arrayEndIdx);
                LOG.debug("convertOMAttributeDef: handling an OM array of elements of type {}", elementTypeName);
                atlasTypeName = ATLAS_TYPE_ARRAY_PREFIX + elementTypeName + ATLAS_TYPE_ARRAY_SUFFIX;
                break;
            case OM_COLLECTION_MAP:
                String OM_MAP_PREFIX = "map<";
                String OM_MAP_SUFFIX = ">";
                int mapStartIdx = OM_MAP_PREFIX.length();
                int mapEndIdx = collectionTypeName.length() - OM_MAP_SUFFIX.length();
                String kvTypeString = collectionTypeName.substring(mapStartIdx, mapEndIdx);
                String[] parts = kvTypeString.split(",");
                String keyType = parts[0];
                String valType = parts[1];
                atlasTypeName = ATLAS_TYPE_MAP_PREFIX + keyType + ATLAS_TYPE_MAP_KEY_VAL_SEP + valType + ATLAS_TYPE_MAP_SUFFIX;
                LOG.debug("convertOMAttributeDef: atlas type name is {}", atlasTypeName);
                break;
            default:
                LOG.debug("convertOMCollectionDef: cannot convert a collection def with category {}", collDefCat);
                return null;
        }
        aad.setTypeName(atlasTypeName);
        return aad;
    }

    /*
     * Utility functions to convert OM cardinality to Atlas cardinality and optionality
     */

    private AtlasStructDef.AtlasAttributeDef.Cardinality convertOMCardinalityToAtlasCardinality(AttributeCardinality omCard) {
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCard;
        switch (omCard) {
            case AT_MOST_ONE:
            case ONE_ONLY:
                atlasCard = SINGLE;
                break;
            case AT_LEAST_ONE_ORDERED:
            case ANY_NUMBER_ORDERED:
                atlasCard = LIST;
                break;
            case AT_LEAST_ONE_UNORDERED:
            case ANY_NUMBER_UNORDERED:
                atlasCard = SET;
                break;
            case UNKNOWN:
            default:
                LOG.info("convertOMCardinalityToAtlasCardinality: not specified by caller, defaulting to SINGLE");
                // There is no 'good' choice for return code here but default to single...
                atlasCard = SINGLE;
                break;
        }
        return atlasCard;
    }

    private boolean convertOMCardinalityToAtlasOptionality(AttributeCardinality omCard) {
        switch (omCard) {
            case AT_MOST_ONE:
            case ANY_NUMBER_ORDERED:
            case ANY_NUMBER_UNORDERED:
                return true;
            case ONE_ONLY:
            case AT_LEAST_ONE_ORDERED:
            case AT_LEAST_ONE_UNORDERED:
                return false;
            case UNKNOWN:
            default:
                LOG.info("convertOMCardinalityToAtlasOptionality: not specified by caller, defaulting to TRUE");
                // There is no 'good' choice for return code here - but default to optional
                return true;
        }
    }

    /**
     * Method to convert an OM EnumDef into an AtlasAttributeDef - they are handled differently by the two type systems
     * @param tda - the TypeDefAttribute to be converted
     * @return    - AtlasStructDef resulting from conversion of the OM EnumDef
     */
    private AtlasStructDef.AtlasAttributeDef convertOMEnumDefToAtlasAttributeDef(TypeDefAttribute tda) {

        if (tda == null) {
            return null;
        }
        AtlasStructDef.AtlasAttributeDef aad = new AtlasStructDef.AtlasAttributeDef();
        aad.setName(tda.getAttributeName());
        aad.setValuesMinCount(tda.getValuesMinCount());
        aad.setValuesMaxCount(tda.getValuesMaxCount());
        aad.setIsIndexable(tda.isIndexable());
        aad.setIsUnique(tda.isUnique());
        aad.setDefaultValue(tda.getDefaultValue());
        aad.setDescription(tda.getAttributeDescription());
        // Currently setting Atlas cardinality and optionality using a pair of method calls.
        AtlasStructDef.AtlasAttributeDef.Cardinality atlasCardinality = convertOMCardinalityToAtlasCardinality(tda.getAttributeCardinality());
        aad.setCardinality(atlasCardinality);
        boolean atlasOptionality = convertOMCardinalityToAtlasOptionality(tda.getAttributeCardinality());
        aad.setIsOptional(atlasOptionality);
        aad.setConstraints(null);

        AttributeTypeDef atd = tda.getAttributeType();
        String atlasTypeName = atd.getName();
        aad.setTypeName(atlasTypeName);
        return aad;

    }




    // Convert from an OM relCat to equivalent Atlas type
    //private AtlasRelationshipDef.RelationshipCategory convertOMRelationshipCategoryToAtlasRelationshipCategory(RelationshipCategory omRelCat) {
    //    AtlasRelationshipDef.RelationshipCategory ret;
    //    switch (omRelCat) {
    //        case ASSOCIATION:
    //            ret = AtlasRelationshipDef.RelationshipCategory.ASSOCIATION;
    //            break;
    //        case AGGREGATION:
    //            ret = AtlasRelationshipDef.RelationshipCategory.AGGREGATION;
    //            break;
    //        case COMPOSITION:
    //            ret = AtlasRelationshipDef.RelationshipCategory.COMPOSITION;
    //            break;
    //        default:
    //            // Anything else is invalid
    //            LOG.debug("convertOMRelCatToAtlas: unknown relationship category {}", omRelCat);
    //            ret = null;
    //            break;
    //    }
    //    return ret;
    //}




    // This class loads all the Atlas TypeDefs into the typeDefsCache
    private void loadAtlasTypeDefs(String userId)
            throws RepositoryErrorException
    {

        final String methodName = "loadAtlasTypeDefs";

        // Retrieve the typedefs from Atlas, and return a TypeDefGallery that contains two lists, each sorted by category
        // as follows:
        // TypeDefGallery.attributeTypeDefs contains:
        // 1. PrimitiveDefs
        // 2. CollectionDefs
        // 3. EnumDefs
        // TypeDefGallery.newTypeDefs contains:
        // 1. EntityDefs
        // 2. RelationshipDefs
        // 3. ClassificationDefs

        // The result of the load is constructed in typeDefsForAPI - a copy of which is made in the typeDefsCache.

        // Strategy: use searchTypesDef with a null (default) SearchFilter.
        SearchFilter emptySearchFilter = new SearchFilter();
        AtlasTypesDef atd;
        try {

            atd = typeDefStore.searchTypesDef(emptySearchFilter);

        } catch (AtlasBaseException e) {

            LOG.error("loadAtlasTypeDefs: caught exception from Atlas searchTypesDef", e);

            // This is pretty serious - if Atlas cannot retrieve any types we are in trouble...
            OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(repositoryName, methodName, e.getMessage());

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "convertAtlasAttributeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        // Parse the Atlas TypesDef
        // Strategy is to walk the Atlas TypesDef object - i.e. looking at each list of enumDefs, classificationDefs, etc..
        // and for each list try to convert each element (i.e. each type def) to a corresponding OM type def. If a problem
        // is encountered within a typedef - for example we encounter a reference attribute or anything else that is not
        // supported in OM - then we skip (silently) over the Atlas type def. i.e. The metadatacollection will convert the
        // things that it understands, and will silently ignore anything that it doesn't understand (e.g. structDefs) or
        // anything that contains something that it does not understand (e.g. a reference attribute or a collection that
        // contains anything other than primitives).

        // This method will populate the typeDefsForAPI object.
        if (atd != null) {
            convertAtlasTypeDefs(userId, atd);
        }

    }


    // ------------------------------------------------------------------------------------------------
    private AtlasRelationshipDef.PropagateTags convertOMPropagationRuleToAtlasPropagateTags(ClassificationPropagationRule omPropRule)
        throws
        TypeErrorException
    {
        final String methodName = "convertOMPropagationRuleToAtlasPropagateTags";
        AtlasRelationshipDef.PropagateTags atlasPropTags;
        switch (omPropRule) {
            case NONE:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.NONE;
                break;
            case ONE_TO_TWO:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
                break;
            case TWO_TO_ONE:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.TWO_TO_ONE;
                break;
            case BOTH:
                atlasPropTags = AtlasRelationshipDef.PropagateTags.BOTH;
                break;
            default:
                LOG.debug("convertOMPropagationRuleToAtlasPropagateTags: could not convert OM propagation rule {}", omPropRule);

                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_PROPAGATION_RULE;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(omPropRule.toString(), methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }
        return atlasPropTags;
    }

    // Utility method to convert from OM properties to Atlas attributes map
    //
    private Map<String, Object> convertOMPropertiesToAtlasAttributes(InstanceProperties instanceProperties) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> convertOMPropertiesToAtlasAttributes(instanceProperties={})", instanceProperties);
        }
        Map<String, Object> atlasAttrs = null;

        if (instanceProperties != null) {
            Iterator<String> propNames = instanceProperties.getPropertyNames();
            if (propNames.hasNext()) {
                atlasAttrs = new HashMap<>();
                while (propNames.hasNext()) {
                    // Create an atlas attribute for this property
                    String propName = propNames.next();
                    // Retrieve the IPV - this will actually be of a concrete class such as PrimitivePropertyValue....
                    InstancePropertyValue ipv = instanceProperties.getPropertyValue(propName);
                    InstancePropertyCategory ipvCat = ipv.getInstancePropertyCategory();
                    switch (ipvCat) {
                        case PRIMITIVE:
                            LOG.debug("==> convertOMPropertiesToAtlasAttributes: handling primitive value ipv={}",ipv);
                            PrimitivePropertyValue primitivePropertyValue = (PrimitivePropertyValue) ipv;
                            Object primValue = primitivePropertyValue.getPrimitiveValue();
                            atlasAttrs.put(propName, primValue);
                            break;
                        case ARRAY:
                            // Get the ArrayPropertyValue and then use getArrayCount and getArrayValues to get the length and an
                            // InstanceProperties object. You can then use the map contained in the InstanceProperties to construct
                            // and Array object that you can set sa the value for Atlas...
                            ArrayPropertyValue apv = (ArrayPropertyValue) ipv;
                            int arrayLen = apv.getArrayCount();
                            ArrayList<Object> atlasArray = null;
                            if (arrayLen > 0) {
                                atlasArray = new ArrayList<>();
                                InstanceProperties arrayProperties = apv.getArrayValues();
                                Iterator<String> keys = arrayProperties.getPropertyNames();
                                while (keys.hasNext()) {
                                    String key = keys.next();
                                    Object val = arrayProperties.getPropertyValue(key);
                                    atlasArray.add(val);
                                }
                            }
                            atlasAttrs.put(propName,atlasArray);
                            break;
                        case MAP:
                            // Get the MapPropertyValue and then use getMapElementCount and getMapValues to get the length and an
                            // InstanceProperties object. You can then use the map contained in the InstanceProperties to construct
                            // a Map object that you can set sa the value for Atlas...
                            MapPropertyValue mpv = (MapPropertyValue) ipv;
                            int mapLen = mpv.getMapElementCount();
                            HashMap<String,Object> atlasMap = null;
                            if (mapLen > 0) {
                                atlasMap = new HashMap<>();
                                InstanceProperties mapProperties = mpv.getMapValues();
                                Iterator<String> keys = mapProperties.getPropertyNames();
                                while (keys.hasNext()) {
                                    String key = keys.next();
                                    Object val = mapProperties.getPropertyValue(key);
                                    atlasMap.put(key,val);
                                }
                            }
                            atlasAttrs.put(propName,atlasMap);
                            break;
                        case ENUM:
                            EnumPropertyValue enumPropertyValue = (EnumPropertyValue) ipv;
                            Object enumValue = enumPropertyValue.getSymbolicName();
                            atlasAttrs.put(propName, enumValue);
                            break;
                        case STRUCT:
                        case UNKNOWN:
                        default:
                            LOG.debug("convertOMPropertiesToAtlasAttributes: Unsupported attribute {} ignored", propName);
                            break;
                    }
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== convertOMPropertiesToAtlasAttributes(atlasAttrs={})", atlasAttrs);
        }
        return atlasAttrs;
    }

    /*
     * Convenience method to translate an Atlas status enum value to the corresponding OM InstanceValue enum value
     */
    private InstanceStatus convertAtlasStatusToOMInstanceStatus(AtlasEntity.Status atlasStatus) {
        switch (atlasStatus) {
            case ACTIVE:
                return InstanceStatus.ACTIVE;
            case DELETED:
                return InstanceStatus.DELETED;
            default:
                return null;
        }
    }


    private  List<EntityDetail>  findEntitiesByPropertyUsingDSL(String               userId,
                                                                String               entityTypeGUID,
                                                                String               searchCriteria,
                                                                List<InstanceStatus> limitResultsByStatus,
                                                                List<String>         limitResultsByClassification,
                                                                int                  offset,
                                                                int                  pageSize)

            throws TypeErrorException, RepositoryErrorException, PropertyErrorException
    {

        final String methodName = "findEntitiesByPropertyUsingDSL";
        final String entityTypeGUIDParameterName = "entityTypeGUID";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByPropertyUsingDSL(userId={}, entityTypeGUID={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    userId, entityTypeGUID, searchCriteria, limitResultsByStatus, limitResultsByClassification);
        }

        // This method is used from findEntitiesByPropertyValue which needs to search string properties
        // only, with the string contained in searchCriteria.

        // Find the type def, examine the properties and add each string property to an InstanceProperties
        // object with the string given in searchCriteria, then formulate a DSL query by delegating to the
        // companion method that accepts InstanceProperties

        // Use the entityTypeGUID to retrieve the type name
        TypeDef typeDef;
        try {

            typeDef = _getTypeDefByGUID(userId, entityTypeGUID);

        }
        catch (RepositoryErrorException | TypeDefNotKnownException e) {
            // Handle below
            LOG.error("findEntitiesByPropertyUsingDSL: Caught exception from _getTypeDefByGUID {}", e);
            typeDef = null;
        }

        if (typeDef == null) {

            LOG.error("findEntitiesByPropertyUsingDSL: Could not retrieve entity type using GUID {}", entityTypeGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        List<EntityDetail> retList;

        try {
            // Retrieve the type def attributes from the type def
            InstanceProperties matchStringProperties = null;
            List<String> matchStringPropertyNames = null; // used for debug
            List<TypeDefAttribute> attrDefs = getAllDefinedProperties(userId, typeDef);
            if (attrDefs != null) {
                for (TypeDefAttribute tda : attrDefs) {
                    AttributeTypeDef atd = tda.getAttributeType();
                    AttributeTypeDefCategory atdCat = atd.getCategory();
                    if (atdCat == AttributeTypeDefCategory.PRIMITIVE) {
                        PrimitiveDef pDef = (PrimitiveDef) atd;
                        PrimitiveDefCategory pDefCat = pDef.getPrimitiveDefCategory();
                        if (pDefCat == PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING) {
                            // this is a string property...
                            if (matchStringProperties == null) {
                                // first string property...
                                matchStringProperties = new InstanceProperties();
                                matchStringPropertyNames = new ArrayList<>();
                            }
                            PrimitivePropertyValue ppv = new PrimitivePropertyValue();
                            ppv.setPrimitiveDefCategory(pDefCat);
                            ppv.setPrimitiveValue(searchCriteria);
                            matchStringProperties.setProperty(tda.getAttributeName(), ppv);
                            matchStringPropertyNames.add(tda.getAttributeName());
                        }
                    }
                }
            }
            LOG.debug("findEntitiesByPropertyUsingDSL: will look for {} in the following properties {}",searchCriteria,matchStringPropertyNames);


            // Delegate
            /* Because there is no post-processing in this method (this one, not the one it is about to call) it is
             * safe to pass offset and pageSize through to the delegated-to method.
             */
            retList = findEntitiesByPropertyUsingDSL(userId, entityTypeGUID, matchStringProperties,
                    MatchCriteria.ANY, limitResultsByStatus, limitResultsByClassification, offset, pageSize);

        } catch (TypeDefNotKnownException e) {

            LOG.error("findEntitiesByPropertyUsingDSL: Could not retrieve properties of entity type with GUID {}", entityTypeGUID, e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("unknown", entityTypeGUID, entityTypeGUIDParameterName, methodName, repositoryName);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByPropertyUsingDSL(userId={}, entityTypeGUID={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): retList={}",
                    userId, entityTypeGUID, searchCriteria, limitResultsByStatus, limitResultsByClassification, retList);
        }
        return retList;

    }

    /**
     * findEntitiesByPropertyUsingDSL is a helper method for compiling a DSL query
     * @param userId                   - unique identifier for requesting user.
     * @param entityTypeGUID           - the GUID of the EntityDef
     * @param matchProperties          - a set of properties that must match
     * @param matchCriteria            - nature of the properties match: ALL, ANY, NONE
     * @param limitResultsByStatus     - whether to permit results with only these status values
     * @param limitResultsByClassification  - whether to permit only results with these classifications
     * @return a list of entities that satisfy the search criteria
     * @throws TypeErrorException        - entityTypeGUID does not relate to a valid EntityDef
     * @throws RepositoryErrorException  - internal error in underlying repository
     * @throws PropertyErrorException    - a specified property is not valid
     */
    private  List<EntityDetail>   findEntitiesByPropertyUsingDSL(String                    userId,
                                                                 String                    entityTypeGUID,
                                                                 InstanceProperties        matchProperties,
                                                                 MatchCriteria             matchCriteria,
                                                                 List<InstanceStatus>      limitResultsByStatus,
                                                                 List<String>              limitResultsByClassification,
                                                                 int                       offset,
                                                                 int                       pageSize)

            throws
            TypeErrorException,
            RepositoryErrorException,
            PropertyErrorException

    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByPropertyUsingDSL(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification);
        }

        final String methodName = "findEntitiesByPropertyUsingDSL";

        // Where a property is of type String the match value should be used as a substring (fuzzy) match.
        // For all other types of property the match value needs to be an exact match.

        // Use the entityTypeGUID to retrieve the type name
        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, entityTypeGUID);
        }
        catch (TypeDefNotKnownException | RepositoryErrorException e) {
            LOG.error("findEntitiesByPropertyUsingDSL: Caught exception from _getTypeDefByGUID", e);
            // handle below...
            typeDef = null;
        }
        if (typeDef == null) {
            LOG.debug("findEntitiesByPropertyUsingDSL: could not retrieve typedef for guid {}", entityTypeGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }
        String typeName = typeDef.getName();


        // Formulate the query string...
        StringBuilder dslString = new StringBuilder();
        dslString.append(typeName);

        DSLQueryHelper dslQueryHelper = new DSLQueryHelper();

        // Add a WHERE clause if needed...
        try {
            String whereClause = dslQueryHelper.createWhereClause(typeName, matchProperties, matchCriteria, limitResultsByClassification);
            LOG.debug("findEntitiesByPropertyUsingDSL: whereClause is {}", whereClause);
            if (whereClause != null)
                dslString.append(whereClause);
        }
        catch (RepositoryErrorException | PropertyErrorException e) {
            LOG.error("findEntitiesByPropertyUsingDSL: re-throwing exception from createWhereClause", e);
            throw e;
        }

        LOG.debug("findEntitiesByPropertyUsingDSL: query string is <{}>", dslString);

        String dslSearchString = dslString.toString();

        /* Care is needed at this point. If this method were to pass offset and pageSize (limit) through to the
         * entityDisocveryService we could miss valid results. This is because any instanceStatus checking is
         * performed as a post-process in the current method, so we must not offset or limit the search performed
         * by the discovery service.
         *
         * To understand why this would be a problen consider the following:
         * Suppose there are 100 entities that would match our search, including classification filtering.
         * If we were to specify an offset of 20 and pageSize of 30 we would receive back entities 20 - 49.
         * If any of those entities fails the subsequent instance status filter then we will get a subset of
         * a page of entities - none of the current method, the caller nor the end user can tell whether there
         * are other entities outside the range 20-49 that would have been valid and passed the status filter.
         * To provide a valid and maximal result we must only apply offset and pageSize once the post-filtering
         * is complete.
         *
         * In theory, we could allow Atlas to perform a narrower search where we know we will be filtering by
         * instanceStatus by testing (limitResultsByStatus != null). However, there is also filtering out of
         * entity proxies and since we do not know which entities are proxies until after the search is complete,
         * we must always search as widely as Atlas will allow.
         */
        int atlasSearchLimit = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();
        int atlasSearchOffset = 0;
        AtlasSearchResult atlasSearchResult;
        try {
            atlasSearchResult = entityDiscoveryService.searchUsingDslQuery(dslSearchString, atlasSearchLimit, atlasSearchOffset);

        }
        catch (AtlasBaseException e) {
            LOG.error("findEntitiesByPropertyUsingDSL: Atlas DSL query threw exception {}", e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }


        // Construct the result

        ArrayList<EntityDetail> returnList = null;
        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                // Filter by status if requested...
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if ( !match ) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }
                // We need to use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                // to get an EntityDetail object.

                /* An AtlasEntityHeader has:
                 * String                    guid
                 * AtlasEntity.Status        status
                 * String                    displayText
                 * List<String>              classificationNames
                 * List<AtlasClassification> classifications
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                try {
                    atlasEntityWithExt = entityStore.getById(aeh.getGuid());
                } catch (AtlasBaseException e) {

                    LOG.error("findEntitiesByPropertyUsingDSL: caught exception from Atlas entity store getById {}", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

                LOG.debug("findEntitiesByPropertyUsingDSL: atlasEntity {}", atlasEntity);


                if (atlasEntity.isProxy()) {
                    // This entity is only a proxy - do not include it in the search result.
                    LOG.debug("findEntitiesByPropertyUsingDSL: ignoring atlasEntity because it is a proxy {}", atlasEntity);
                    continue;
                }

                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("findEntitiesByPropertyUsingDSL: om entity {}", omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (TypeErrorException | InvalidEntityException e) {

                    LOG.error("findEntitiesByPropertyUsingDSL: could not map AtlasEntity to EntityDetail", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }
            }
        }

        /* We did not apply offset or pageSize earlier due to the existence of post-filtering (for status and proxy). So we
         * must apply offset and pageSize now.
         */
        LOG.debug("findEntitiesByPropertyUsingDSL: returnList={}", returnList);
        LOG.debug("findEntitiesByPropertyUsingDSL: apply offset and pageSize");
        List<EntityDetail> limitedReturnList;
        try {
            limitedReturnList = formatEntityResults(returnList, offset, null, null, pageSize);
        }
        catch (PagingErrorException e) {
            LOG.error("findEntitiesByPropertyUsingSearchParameters: caught exception from result formatter", e);

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.PAGING_ERROR;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(String.valueOf(offset), String.valueOf(pageSize), methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByPropertyUsingDSL(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}: returnList={})",
                    userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification, limitedReturnList);
        }
        return limitedReturnList;
    }


    private  List<EntityDetail>   findEntitiesByPropertyUsingSearchParameters(String                  userId,
                                                                              String                  searchCriteria,
                                                                              List<InstanceStatus>    limitResultsByStatus,
                                                                              List<String>            limitResultsByClassification,
                                                                              int                     offset,
                                                                              int                     pageSize)
            throws
            PropertyErrorException,
            RepositoryErrorException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByPropertyUsingSearchParameters(userId={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    userId, searchCriteria, limitResultsByStatus, limitResultsByClassification);
        }

        final String methodName = "findEntitiesByPropertyUsingSearchParameters";

        // Special case when searchCriteria is null or empty string - return an empty list
        if (searchCriteria == null || searchCriteria.equals("")) {
            // Nothing to search for
            LOG.debug("findEntitiesByPropertyUsingSearchParameters: no search criteria supplied, returning null");
            return null;
        }

        // This method is used from findEntitiesByPropertyValue which needs to search string properties
        // only, with the string contained in searchCriteria.

        // We have no entityTypeGUID - so we are operating across all types. We therefore do not have
        // a specified set of properties to examine. But we only want to match string properties. We can
        // therefore use a full text search.

        SearchParameters searchParameters = new SearchParameters();
        searchParameters.setTypeName(null);

        boolean postFilterClassifications = false;
        if (limitResultsByClassification != null) {
            if (limitResultsByClassification.size() == 1) {
                searchParameters.setClassification(limitResultsByClassification.get(0));          // with exactly one classification, classification will be part of search
            } else {
                postFilterClassifications = true;
                searchParameters.setClassification(null);                                         // classifications will be post-filtered if requested
            }
        } else {
            searchParameters.setClassification(null);
        }

        // We know we have no typeGUID. We have enforced that there must be a non-empty searchCriteria string.

        // Frame the query string in asterisks so that it can appear anywhere in a value...
        String searchPattern = "*"+searchCriteria+"*";
        searchParameters.setQuery(searchPattern);


        searchParameters.setExcludeDeletedEntities(false);
        searchParameters.setIncludeClassificationAttributes(false);
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setIncludeSubClassifications(false);

        /* Care is needed at this point. If this method were to pass offset and pageSize (limit) through to the
         * entityDiscoveryService we could miss valid results. This is because any instanceStatus checking is
         * performed as a post-process in the current method, so we must not offset or limit the search performed
         * by the discovery service.
         *
         * To understand why this would be a problem consider the following:
         * Suppose there are 100 entities that would match our search.
         * If we were to specify an offset of 20 and pageSize of 30 we would receive back entities 20 - 49.
         * If any of those entities fails the subsequent instance status or classification filter then we will get
         * a subset of a page of entities - none of the current method, the caller nor the end user can tell whether
         * there are other entities outside the range 20-49 that would have been valid and passed the status filter.
         * To provide a valid and maximal result we must only apply offset and pageSize once the post-filtering
         * is complete.
         *
         * In theory, we could allow Atlas to perform a narrower search where we know we will be filtering by
         * instanceStatus by testing (limitResultsByStatus != null). However, there is also filtering out of
         * entity proxies and since we do not know which entities are proxies until after the search is complete,
         * we must always search as widely as Atlas will allow.
         */
        int atlasSearchLimit = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();
        int atlasSearchOffset = 0;
        searchParameters.setLimit(atlasSearchLimit);
        searchParameters.setOffset(atlasSearchOffset);
        searchParameters.setTagFilters(null);
        searchParameters.setAttributes(null);
        searchParameters.setEntityFilters(null);

        AtlasSearchResult atlasSearchResult;
        try {

            atlasSearchResult = entityDiscoveryService.searchWithParameters(searchParameters);

        } catch (AtlasBaseException e) {

            LOG.error("findEntitiesByPropertyUsingSearchParameters: entity discovery service searchWithParameters threw exception", e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(searchCriteria, "searchCriteria", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        ArrayList<EntityDetail> returnList = null;


        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }
                // We need to use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                // to get an EntityDetail object.

                /* An AtlasEntityHeader has:
                 * String                    guid
                 * AtlasEntity.Status        status
                 * String                    displayText
                 * List<String>              classificationNames
                 * List<AtlasClassification> classifications
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;

                try {

                    atlasEntityWithExt = entityStore.getById(aeh.getGuid());

                } catch (AtlasBaseException e) {

                    LOG.error("findEntitiesByPropertyUsingSearchParameters: entity store getById threw exception", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();


                if (atlasEntity.isProxy()) {
                    // This entity is only a proxy - do not include it in the search result.
                    LOG.debug("findEntitiesByPropertyUsingSearchParameters: ignoring atlasEntity because it is a proxy {}", atlasEntity);
                    continue;
                }

                if (postFilterClassifications) {
                    // We need to ensure that this entity has all of the specified filter classifications...
                    List<AtlasClassification> entityClassifications = atlasEntity.getClassifications();
                    int numFilterClassifications = limitResultsByClassification.size();
                    int cursor = 0;
                    boolean missingClassification = false;
                    while (!missingClassification && cursor < numFilterClassifications - 1) {
                        // Look for this filterClassification in the entity's classifications
                        String filterClassificationName = limitResultsByClassification.get(cursor);
                        boolean match = false;
                        for (AtlasClassification atlasClassification : entityClassifications) {
                            if (atlasClassification.getTypeName().equals(filterClassificationName)) {
                                match = true;
                                break;
                            }
                        }
                        if ( !match ) {
                            missingClassification = true;     // stop looking, one miss is enough
                        }
                        cursor++;
                    }
                    if (missingClassification)
                        continue;                  // skip this entity and process the next, if any remain
                }


                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("findEntitiesByPropertyUsingSearchParameters: om entity {}", omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (TypeErrorException | InvalidEntityException e) {

                    LOG.error("findEntitiesByPropertyUsingSearchParameters: could not map AtlasEntity to entityDetail, exception", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
        }

        /* We did not apply offset or pageSize earlier due to the existence of post-filtering (for status and proxy). So we
         * must apply offset and pageSize now.
         */
        LOG.debug("findEntitiesByPropertyUsingSearchParameters: returnList={}", returnList);
        LOG.debug("findEntitiesByPropertyUsingSearchParameters: apply offset and pageSize");
        List<EntityDetail> limitedReturnList;
        try {
            limitedReturnList = formatEntityResults(returnList, offset, null, null, pageSize);
        }
        catch (PagingErrorException e) {
            LOG.error("findEntitiesByPropertyUsingSearchParameters: caught exception from result formatter", e);

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.PAGING_ERROR;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(String.valueOf(offset), String.valueOf(pageSize), methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByPropertyUsingSearchParameters(userId={}, searchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnList={}",
                    userId, searchCriteria, limitResultsByStatus, limitResultsByClassification, limitedReturnList);
        }
        return limitedReturnList;
    }


    /*
     *
     * Internal method for search using SearchParameters
     *
     */
    private  List<EntityDetail>   findEntitiesByPropertyUsingSearchParameters(String                   userId,
                                                                              InstanceProperties       matchProperties,
                                                                              MatchCriteria            matchCriteria,
                                                                              List<InstanceStatus>     limitResultsByStatus,
                                                                              List<String>             limitResultsByClassification,
                                                                              int                      offset,
                                                                              int                      pageSize)
            throws
            PropertyErrorException,
            RepositoryErrorException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByPropertyUsingSearchParameters(userId={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    userId, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification);
        }

        final String methodName = "findEntitiesByPropertyUsingSearchParameters";

        // This is the hardest case of a findEntitiesByProperty - you do not have a typeGUID so the search applies to all entity
        // types. The method will therefore find all the entity types and do a query for each type, then union the results.
        // You may have match properties - although the case of exactly one property could be delegated to basic query
        // it seems simpler to always delegate to searchWithParameters with optionally 0, 1 or many entityFilters.
        // You may also have status and/or classification filters. Those are both post-filtered.
        //

        List<TypeDef> allEntityTypes;
        try {
            allEntityTypes = _findTypeDefsByCategory(userId, TypeDefCategory.ENTITY_DEF);
        }
        catch (InvalidParameterException | UserNotAuthorizedException e) {

            LOG.error("findEntitiesByPropertyUsingSearchParameters: caught exception from _findTypeDefsByCategory", e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("any EntityDef", "TypeDefCategory.ENTITY_DEF", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        if (allEntityTypes == null) {
            LOG.debug("findEntitiesByPropertyUsingSearchParameters: found no entity types");
            return null;
        }

        ArrayList<EntityDetail> returnEntities = null;

        LOG.debug("findEntitiesByPropertyUsingSearchParameters: There are {} entity types defined",allEntityTypes.size());

        if (allEntityTypes.size() > 0) {

            // Iterate over the known entity types performing a search for each...

            for (TypeDef typeDef : allEntityTypes) {
                LOG.debug("findEntitiesByPropertyUsingSearchParameters: checking entity type {}", typeDef.getName());

                // If there are any matchProperties
                if (matchProperties != null) {
                    // Validate that the type has the specified properties...
                    // Whether matchCriteria is ALL | ANY | NONE we need ALL the match properties to be defined in the type definition
                    // For a property definition to match a property in the matchProperties, we need name and type to match.

                    // Find what properties are defined on the type
                    // getAllDefinedProperties() will recurse up the supertype hierarchy
                    List<TypeDefAttribute> definedAttributes;
                    try {

                        definedAttributes = getAllDefinedProperties(userId, typeDef);

                    }
                    catch (TypeDefNotKnownException e) {

                        LOG.error("findEntitiesByPropertyUsingSearchParameters: caught exception from property finder", e);

                        OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage("find properties", "TypeDef", methodName, metadataCollectionId);

                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                    if (definedAttributes == null) {
                        // It is obvious that this type does not have the match properties - because it has no properties
                        // Proceed to the next entity type
                        LOG.debug("findEntitiesByPropertyUsingSearchParameters: entity type {} has no properties - will be ignored", typeDef.getName());
                        // continue;
                    }
                    else {
                        // We know this type has some properties...
                        // Iterate over the match properties... (matching on name and type)
                        Iterator<String> matchPropNames = matchProperties.getPropertyNames();
                        boolean allPropsDefined = true;
                        while (matchPropNames.hasNext()) {
                            String matchPropName = matchPropNames.next();
                            InstancePropertyValue matchPropValue = matchProperties.getPropertyValue(matchPropName);
                            String matchPropType = matchPropValue.getTypeName();
                            LOG.debug("findEntitiesByPropertyUsingSearchParameters: matchProp has name {} type {}", matchPropName, matchPropType);
                            // Find the current match prop in the type def
                            boolean propertyDefined = false;
                            for (TypeDefAttribute defType: definedAttributes) {
                                AttributeTypeDef atd = defType.getAttributeType();
                                String defTypeName = atd.getName();
                                if (defType.getAttributeName().equals(matchPropName) && defTypeName.equals(matchPropType)) {
                                    // Entity type def has the current match property...
                                    LOG.debug("findEntitiesByPropertyUsingSearchParameters: entity type {} has property name {} type {}", typeDef.getName(),matchPropName,matchPropType);
                                    propertyDefined = true;
                                    break;
                                }
                            }
                            if (!propertyDefined) {
                                // this property is missing from the def
                                LOG.debug("findEntitiesByPropertyUsingSearchParameters: entity type {} does not have property name {} type {}", typeDef.getName(),matchPropName,matchPropType);
                                allPropsDefined = false;
                            }
                        }
                        if (!allPropsDefined) {
                            // At least one property in the match props is not defined on the type - skip the type
                            LOG.debug("findEntitiesByPropertyUsingSearchParameters: entity type {} does not have all match properties - will be ignored", typeDef.getName());
                            //continue;
                        }
                        else {
                            // The current type is suitable for a find of instances of this type.
                            LOG.debug("findEntitiesByPropertyUsingSearchParameters: entity type {} will be searched", typeDef.getName());

                            // Extract the type guid and invoke a type specific search...
                            String typeDefGUID = typeDef.getGUID();

                            /* Do not pass on the offset and pageSize - these need to applied once on the aggregated result (from all types)
                             * So make this search as broad as possible - i.e. set offset to 0 and pageSze to MAX.
                             */

                            ArrayList<EntityDetail> entitiesForCurrentType = findEntitiesByPropertyUsingSearchParameters(
                                    userId, typeDefGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification, 0, AtlasConfiguration.SEARCH_MAX_LIMIT.getInt());

                            if (entitiesForCurrentType != null && !entitiesForCurrentType.isEmpty()) {
                                if (returnEntities == null) {
                                    returnEntities = new ArrayList<>();
                                }
                                LOG.debug("findEntitiesByPropertyUsingSearchParameters: for type {} found {} entities", typeDef.getName(),entitiesForCurrentType.size());
                                returnEntities.addAll(entitiesForCurrentType);
                            }
                            else {
                                LOG.debug("findEntitiesByPropertyUsingSearchParameters: for type {} found no entities", typeDef.getName());
                            }
                        }
                    }
                }
            }
        }

        int resultSize = 0;
        if (returnEntities!=null)
            resultSize = returnEntities.size();

        LOG.debug("findEntitiesByPropertyUsingSearchParameters: Atlas found {} entities", resultSize);

        /* We did not apply offset or pageSize earlier due to the aggregation across types, so we
         * must apply offset and pageSize now.
         */
        LOG.debug("findEntitiesByPropertyUsingSearchParameters: returnEntities={}", returnEntities);
        LOG.debug("findEntitiesByPropertyUsingSearchParameters: apply offset and pageSize");
        List<EntityDetail> limitedReturnList;
        try {
            limitedReturnList = formatEntityResults(returnEntities, offset, null, null, pageSize);
        }
        catch (PagingErrorException e) {
            LOG.error("findEntitiesByPropertyUsingSearchParameters: caught exception from result formatter", e);

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.PAGING_ERROR;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(String.valueOf(offset), String.valueOf(pageSize), methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByPropertyUsingSearchParameters(userId={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnEntities={}",
                    userId, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification, limitedReturnList);
        }
        return limitedReturnList;
    }



    // Utility method to recurse up supertype hierarchy fetching property defs
    // Deliberately not scoped for access - default to package private
    List<TypeDefAttribute> getAllDefinedProperties(String userId, TypeDef tdef)
    throws RepositoryErrorException, TypeDefNotKnownException
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getAllDefinedProperties(userId={}, typedef={})", userId, tdef);
        }

        List<TypeDefAttribute> propDefs = new ArrayList<>();

        // Look at the supertype (if any) and then get the properties for the current type def (if any)
        if (tdef.getSuperType() != null) {
            // recurse up the supertype hierarchy until you hit the top
            // Get the supertype's type def
            TypeDefLink superTypeDefLink = tdef.getSuperType();
            String superTypeName = superTypeDefLink.getName();
            try {
                TypeDef superTypeDef = _getTypeDefByName(userId, superTypeName);
                List<TypeDefAttribute> inheritedProps = getAllDefinedProperties(userId, superTypeDef);
                if (inheritedProps != null && !inheritedProps.isEmpty()) {
                    propDefs.addAll(inheritedProps);
                }
            }
            catch (RepositoryErrorException | TypeDefNotKnownException e) {
                LOG.error("getAllDefinedProperties: caught exception from _getTypeDefByName", e);
                throw e;
            }
        }
        // Add the properties defined for the current type
        List<TypeDefAttribute> currentTypePropDefs = tdef.getPropertiesDefinition();
        if (currentTypePropDefs != null && !currentTypePropDefs.isEmpty()) {
            propDefs.addAll(currentTypePropDefs);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getAllDefinedProperties(userId={}, typedef={}): propDefs={}", userId, tdef, propDefs);
        }
        return propDefs;
    }


    private  ArrayList<EntityDetail>   findEntitiesByPropertyUsingSearchParameters(String                   userId,
                                                                                   String                   entityTypeGUID,
                                                                                   InstanceProperties       matchProperties,
                                                                                   MatchCriteria            matchCriteria,
                                                                                   List<InstanceStatus>     limitResultsByStatus,
                                                                                   List<String>             limitResultsByClassification,
                                                                                   int                      offset,
                                                                                   int                      pageSize)
            throws
            PropertyErrorException,
            RepositoryErrorException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByPropertyUsingSearchParameters(userId={}, entityTypeGUID={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={})",
                    userId, entityTypeGUID, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification);
        }

        final String methodName = "findEntitiesByPropertyUsingSearchParameters";

        SearchParameters searchParameters = new SearchParameters();

        // If there is a non-null type specified find the type name for use in SearchParameters

        String typeName = null;
        if (entityTypeGUID != null) {
            // find the entity type name
            TypeDef tDef;
            try {
                tDef = _getTypeDefByGUID(userId, entityTypeGUID);
            }
            catch (TypeDefNotKnownException e) {
                LOG.error("findEntitiesByPropertyUsingSearchParameters: caught exception from attempt to look up type by GUID {}", entityTypeGUID, e);
                return null;
            }
            if (tDef == null) {
                LOG.error("findEntitiesByPropertyUsingSearchParameters: null returned by look up of type with GUID {}", entityTypeGUID);
                return null;
            }
            typeName = tDef.getName();
        }
        searchParameters.setTypeName(typeName);

        boolean postFilterClassifications = false;
        if (limitResultsByClassification != null) {
            if (limitResultsByClassification.size() == 1) {
                searchParameters.setClassification(limitResultsByClassification.get(0));          // with exactly one classification, classification will be part of search
            } else {
                postFilterClassifications = true;
                searchParameters.setClassification(null);                                         // classifications will be post-filtered if requested
            }
        } else {
            searchParameters.setClassification(null);
        }

        // We know we have no typeGUID. If we also have no classifications in the search parameters (due to no
        // classifications being specified or multiple classifications and hence post-filtering) then you must have
        // a fulltext component to the search otherwise there will be no search processor in the SearchContext.
        // Attribute filtering (entityFilters) alone is not sufficient. So must fake up a fulltext search on "*"
        if (searchParameters.getClassification() == null) {
            searchParameters.setQuery("*");
        } else {
            searchParameters.setQuery(null);
        }


        searchParameters.setExcludeDeletedEntities(false);
        searchParameters.setIncludeClassificationAttributes(false);
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setIncludeSubClassifications(false);
        // offset and limit are set from offset and pageSize
        searchParameters.setLimit(pageSize);
        searchParameters.setOffset(offset);
        searchParameters.setTagFilters(null);
        searchParameters.setAttributes(null);

        searchParameters.setEntityFilters(null);
        if (matchProperties != null) {

            SearchParameters.Operator operator;                    // applied to a single filter criterion
            SearchParameters.FilterCriteria.Condition condition;   // applied to a list of filter criteria
            if (matchCriteria == null) {
                matchCriteria = MatchCriteria.ALL;                        // default to matchCriteria of ALL
            }
            switch (matchCriteria) {
                case ALL:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.AND;
                    break;
                case ANY:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.OR;
                    break;
                case NONE:
                    // Temporary restriction until figured out how to perform equivalent of a negative regex
                    //operator = NEQ;
                    //condition = SearchParameters.FilterCriteria.Condition.AND;
                    //break;
                default:
                    LOG.error("findEntitiesByPropertyUsingSearchParameters: only supports matchCriteria ALL, ANY");
                    return null;
            }

            List<SearchParameters.FilterCriteria> filterCriteriaList = new ArrayList<>();

            Iterator<String> matchNames = matchProperties.getPropertyNames();
            while (matchNames.hasNext()) {

                // For the next property allocate and initialise a filter criterion and add it to the list...

                // Extract the name and value as strings for the filter criterion
                String matchPropertyName = matchNames.next();
                boolean stringProp = false;
                String strValue;
                InstancePropertyValue matchValue = matchProperties.getPropertyValue(matchPropertyName);
                InstancePropertyCategory cat = matchValue.getInstancePropertyCategory();
                switch (cat) {
                    case PRIMITIVE:
                        // matchValue is a PPV
                        PrimitivePropertyValue ppv = (PrimitivePropertyValue) matchValue;
                        // Find out if this is a string property
                        PrimitiveDefCategory pdc = ppv.getPrimitiveDefCategory();
                        if (pdc == PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING) {
                            stringProp = true;
                            String actualValue = ppv.getPrimitiveValue().toString();
                            // Frame the actual value so it is suitable for regex search
                            strValue = "'*" + actualValue + "*'";
                            LOG.debug("findEntitiesByPropertyUsingSearchParameters: string property {} has filter value {}", matchPropertyName, strValue);
                        } else {
                            // We need a string for DSL query - this does not reflect the property type
                            strValue = ppv.getPrimitiveValue().toString();
                            LOG.debug("findEntitiesByPropertyUsingSearchParameters: non-string property {} has filter value {}", matchPropertyName, strValue);
                        }
                        break;
                    case ARRAY:
                    case MAP:
                    case ENUM:
                    case STRUCT:
                    case UNKNOWN:
                    default:
                        LOG.error("findEntitiesByPropertyUsingSearchParameters: match property of cat {} not supported", cat);

                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_PROPERTY_CATEGORY;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(cat.toString(), methodName, repositoryName);

                        throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());


                }

                // Override the operator (locally, only for this property) if it is a string
                SearchParameters.Operator localOperator = stringProp ? SearchParameters.Operator.LIKE : operator;

                // Set up a criterion for this property
                SearchParameters.FilterCriteria filterCriterion = new SearchParameters.FilterCriteria();
                filterCriterion.setAttributeName(matchPropertyName);
                filterCriterion.setAttributeValue(strValue);
                filterCriterion.setOperator(localOperator);
                filterCriteriaList.add(filterCriterion);
            }
            // Finalize with a FilterCriteria that contains the list of FilterCriteria and applies the appropriate condition (based on matchCriteria)
            SearchParameters.FilterCriteria entityFilters = new SearchParameters.FilterCriteria();
            entityFilters.setCriterion(filterCriteriaList);
            entityFilters.setCondition(condition);
            searchParameters.setEntityFilters(entityFilters);
        }


        AtlasSearchResult atlasSearchResult;
        try {
            atlasSearchResult = entityDiscoveryService.searchWithParameters(searchParameters);

        } catch (AtlasBaseException e) {

            LOG.error("findEntitiesByPropertyUsingSearchParameters: entity discovery service searchWithParameters threw exception", e);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("searchWithParameters", "searchParameters", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        ArrayList<EntityDetail> returnList = null;

        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if ( !match ) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }
                /* We need to use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                 * to get an EntityDetail object.
                 *
                 * An AtlasEntityHeader has:
                 * String                    guid
                 * AtlasEntity.Status        status
                 * String                    displayText
                 * List<String>              classificationNames
                 * List<AtlasClassification> classifications
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                try {

                    atlasEntityWithExt = entityStore.getById(aeh.getGuid());

                } catch (AtlasBaseException e) {

                    LOG.debug("findEntitiesByPropertyUsingSearchParameters: Caught exception from Atlas entity store getById method {}", e.getMessage());

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
                }

                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

                if (postFilterClassifications) {
                    // We need to ensure that this entity has all of the specified filter classifications...
                    List<AtlasClassification> entityClassifications = atlasEntity.getClassifications();
                    int numFilterClassifications = limitResultsByClassification.size();
                    int cursor = 0;
                    boolean missingClassification = false;
                    while (!missingClassification && cursor < numFilterClassifications - 1) {
                        // Look for this filterClassification in the entity's classifications
                        String filterClassificationName = limitResultsByClassification.get(cursor);
                        boolean match = false;
                        for (AtlasClassification atlasClassification : entityClassifications) {
                            if (atlasClassification.getTypeName().equals(filterClassificationName)) {
                                match = true;
                                break;
                            }
                        }
                        missingClassification = !match;   // stop looking, one miss is enough
                        cursor++;
                    }
                    if (missingClassification)
                        continue;                  // skip this entity and process the next, if any remain
                }


                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("findEntitiesByProperty: om entity {}", omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (Exception e) {

                    LOG.error("findEntitiesByPropertyUsingSearchParameters: could not map AtlasEntity to EntityDetail", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasEntity.getGuid(), "atlasEntity", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByPropertyUsingSearchParameters(userId={}, matchProperties={}, matchCriteria={}, limitResultsByStatus={}, limitResultsByClassification={}): returnList={}",
                    userId, matchProperties, matchCriteria, limitResultsByStatus, limitResultsByClassification, returnList);
        }
        return returnList;

    }



    /*
     * Internal method for constructing search using DSL
     */

    private  List<EntityDetail>   findEntitiesByClassificationUsingDSL(String                 userId,
                                                                       String                 entityTypeGUID,
                                                                       String                 classificationName,
                                                                       InstanceProperties     matchClassificationProperties,
                                                                       MatchCriteria          matchCriteria,
                                                                       List<InstanceStatus>   limitResultsByStatus,
                                                                       int                    offset,
                                                                       int                    pageSize)

            throws
            TypeErrorException,
            RepositoryErrorException,
            PropertyErrorException

    {


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByClassificationUsingDSL(userId={}, entityTypeGUID={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={})",
                    userId, entityTypeGUID, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus);
        }

        final String methodName = "findEntitiesByClassificationUsingDSL";

        // Use the entityTypeGUID to retrieve the type name
        TypeDef typeDef;
        try {
            typeDef = _getTypeDefByGUID(userId, entityTypeGUID);
        }
        catch (RepositoryErrorException | TypeDefNotKnownException e) {
            LOG.error("findEntitiesByClassificationUsingDSL: caught exception from _getTypeDefByGUID", e);
            // handle below
            typeDef = null;
        }

        if (typeDef == null) {

            LOG.error("findEntitiesByClassificationUsingDSL: could not retrieve typedef for guid {}", entityTypeGUID);

            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(entityTypeGUID, "entityTypeGUID", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        String typeName = typeDef.getName();

        // Formulate the query string...
        StringBuilder dslString = new StringBuilder();
        dslString.append(typeName);

        DSLQueryHelper dslQueryHelper = new DSLQueryHelper();

        ArrayList<String> classificationsFilter = new ArrayList<>();
        classificationsFilter.add(classificationName);

        // Add a WHERE clause if needed...
        try {
            String whereClause = dslQueryHelper.createWhereClause(typeName, matchClassificationProperties, matchCriteria, classificationsFilter);
            LOG.debug("findEntitiesByClassificationUsingDSL: whereClause is {}", whereClause);
            if (whereClause != null)
                dslString.append(whereClause);
        }
        catch (PropertyErrorException e) {
            LOG.error("findEntitiesByClassificationUsingDSL: re-throwing exception from createWhereClause", e);
            throw e;

        }

        LOG.debug("findEntitiesByClassificationUsingDSL: query string is <{}>", dslString);

        String dslSearchString = dslString.toString();
        int limit = pageSize;
        //int offset = offset;
        AtlasSearchResult atlasSearchResult;
        try {
            atlasSearchResult = entityDiscoveryService.searchUsingDslQuery(dslSearchString, limit, offset);

        }
        catch (AtlasBaseException e) {

            LOG.error("findEntitiesByClassificationUsingDSL: Atlas searchUsingDslQuery threw exception", e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(dslSearchString, "dslSearchString", methodName, metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }



        // Filter by status if requested...

        ArrayList<EntityDetail> returnList = null;
        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if ( !match  ) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }
                /* We need to use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                 * to get an EntityDetail object.
                 *
                 * An AtlasEntityHeader has:
                 * String                    guid
                 * AtlasEntity.Status        status
                 * String                    displayText
                 * List<String>              classificationNames
                 * List<AtlasClassification> classifications
                 */
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                try {

                    atlasEntityWithExt = entityStore.getById(aeh.getGuid());

                } catch (AtlasBaseException e) {

                    LOG.error("findEntitiesByClassificationUsingDSL: Atlas entity store getById threw exception", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("findEntitiesByClassificationUsingDSL: om entity {}", omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (Exception e) {

                    LOG.error("findEntitiesByClassificationUsingDSL: could not map AtlasEntity to EntityDetail, exception", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasEntity.getGuid(), "Atlas entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByClassificationUsingDSL(userId={}, entityTypeGUID={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={}): returnList={}",
                    userId, entityTypeGUID, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus, returnList);
        }
        return returnList;
    }


    private  List<EntityDetail>   findEntitiesByClassificationUsingSearchParameters(String               userId,
                                                                                    String               classificationName,
                                                                                    InstanceProperties   matchClassificationProperties,
                                                                                    MatchCriteria        matchCriteria,
                                                                                    List<InstanceStatus> limitResultsByStatus,
                                                                                    int                  offset,
                                                                                    int                  pageSize)


            throws
            PropertyErrorException,
            RepositoryErrorException
    {


        if (LOG.isDebugEnabled()) {
            LOG.debug("==> findEntitiesByClassificationUsingSearchParameters(userId={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={})",
                    userId, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus);
        }


        final String methodName = "findEntitiesByClassificationUsingSearchParameters";

        SearchParameters searchParameters = new SearchParameters();

        searchParameters.setQuery(null);
        searchParameters.setTypeName(null);
        searchParameters.setClassification(classificationName);
        searchParameters.setExcludeDeletedEntities(false);
        searchParameters.setIncludeClassificationAttributes(false);
        searchParameters.setIncludeSubTypes(false);
        searchParameters.setIncludeSubClassifications(false);
        searchParameters.setLimit(pageSize);
        searchParameters.setOffset(offset);
        searchParameters.setEntityFilters(null);
        searchParameters.setAttributes(null);


        searchParameters.setTagFilters(null);
        if (matchClassificationProperties != null) {

            SearchParameters.Operator operator;                    // applied to a single filter criterion
            SearchParameters.FilterCriteria.Condition condition;   // applied to a list of filter criteria
            if (matchCriteria == null) {
                matchCriteria = MatchCriteria.ALL;                        // default to matchCriteria of ALL
            }
            switch (matchCriteria) {
                case ALL:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.AND;
                    break;
                case ANY:
                    operator = EQ;
                    condition = SearchParameters.FilterCriteria.Condition.OR;
                    break;
                case NONE:
                    //operator = NEQ;
                    //condition = SearchParameters.FilterCriteria.Condition.AND;
                    //break;
                default:
                    LOG.error("findEntitiesByClassificationUsingSearchParameters: only supports matchCriteria ALL, ANY");
                    return null;
            }

            List<SearchParameters.FilterCriteria> filterCriteriaList = new ArrayList<>();

            Iterator<String> matchNames = matchClassificationProperties.getPropertyNames();
            while (matchNames.hasNext()) {

                // For the next property allocate and initialise a filter criterion and add it to the list...

                // Extract the name and value as strings for the filter criterion
                String matchPropertyName = matchNames.next();
                boolean stringProp = false;
                String strValue;
                InstancePropertyValue matchValue = matchClassificationProperties.getPropertyValue(matchPropertyName);
                InstancePropertyCategory cat = matchValue.getInstancePropertyCategory();
                switch (cat) {
                    case PRIMITIVE:
                        // matchValue is a PPV
                        PrimitivePropertyValue ppv = (PrimitivePropertyValue) matchValue;
                        // Find out if this is a string property
                        PrimitiveDefCategory pdc = ppv.getPrimitiveDefCategory();
                        if (pdc == PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING) {
                            String actualValue = ppv.getPrimitiveValue().toString();
                            // Frame the actual value so it is suitable for regex search
                            strValue = "'*" + actualValue + "*'";
                            LOG.debug("findEntitiesByClassificationUsingSearchParameters: string property {} has filter value {}", matchPropertyName,strValue);
                        }
                        else {
                            // We need a string for DSL query - this does not reflect the property type
                            strValue = ppv.getPrimitiveValue().toString();
                            LOG.debug("findEntitiesByClassificationUsingSearchParameters: non-string property {} has filter value {}", matchPropertyName,strValue);
                        }
                        break;
                    case ARRAY:
                    case MAP:
                    case ENUM:
                    case STRUCT:
                    case UNKNOWN:
                    default:
                        LOG.error("findEntitiesByClassificationUsingSearchParameters: match property of cat {} not supported", cat);

                        LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_PROPERTY_CATEGORY;

                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(cat.toString(), methodName, repositoryName);

                        throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());

                }
                // Set up a criterion for this property
                SearchParameters.FilterCriteria filterCriterion = new SearchParameters.FilterCriteria();
                filterCriterion.setAttributeName(matchPropertyName);
                filterCriterion.setAttributeValue(strValue);
                filterCriterion.setOperator(operator);
                filterCriteriaList.add(filterCriterion);
            }
            // Finalize with a FilterCriteria that contains the list of FilterCriteria and applies the appropriate condition (based on matchCriteria)
            SearchParameters.FilterCriteria tagFilters = new SearchParameters.FilterCriteria();
            tagFilters.setCriterion(filterCriteriaList);
            tagFilters.setCondition(condition);
            searchParameters.setTagFilters(tagFilters);
        }


        AtlasSearchResult atlasSearchResult;
        try {
            atlasSearchResult = entityDiscoveryService.searchWithParameters(searchParameters);

        } catch (AtlasBaseException e) {

            LOG.error("findEntitiesByClassificationUsingSearchParameters: entity discovery service searchWithParameters threw exception", e);

            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(classificationName, "classificationName", methodName, metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        ArrayList<EntityDetail> returnList = null;

        List<AtlasEntityHeader> atlasEntities = atlasSearchResult.getEntities();
        if (atlasEntities != null) {
            returnList = new ArrayList<>();
            for (AtlasEntityHeader aeh : atlasEntities) {
                if (limitResultsByStatus != null) {
                    // Need to check that the AEH status is in the list of allowed status values
                    AtlasEntity.Status atlasStatus = aeh.getStatus();
                    InstanceStatus effectiveInstanceStatus = convertAtlasStatusToOMInstanceStatus(atlasStatus);
                    boolean match = false;
                    for (InstanceStatus allowedStatus : limitResultsByStatus) {
                        if (effectiveInstanceStatus == allowedStatus) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        continue;  // skip this AEH and process the next, if any remain
                    }
                }
                // We need to use the AEH to look up the real AtlasEntity then we can use the relevant converter method
                // to get an EntityDetail object.

                // An AtlasEntityHeader has:
                // String                    guid
                // AtlasEntity.Status        status
                // String                    displayText
                // List<String>              classificationNames
                // List<AtlasClassification> classifications
                AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExt;
                try {

                    atlasEntityWithExt = entityStore.getById(aeh.getGuid());

                } catch (AtlasBaseException e) {

                    LOG.error("findEntitiesByClassificationUsingSearchParameters: entity store getById threw exception", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(aeh.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
                AtlasEntity atlasEntity = atlasEntityWithExt.getEntity();

                // Project the AtlasEntity as an EntityDetail

                try {

                    AtlasEntityMapper atlasEntityMapper = new AtlasEntityMapper(this, userId, atlasEntity);
                    EntityDetail omEntityDetail = atlasEntityMapper.toEntityDetail();
                    LOG.debug("findEntitiesByClassificationUsingSearchParameters: om entity {}", omEntityDetail);
                    returnList.add(omEntityDetail);

                } catch (Exception e) {

                    LOG.error("findEntitiesByClassificationUsingSearchParameters: could not map AtlasEntity to EntityDetail, exception", e);

                    OMRSErrorCode errorCode = OMRSErrorCode.INVALID_ENTITY_FROM_STORE;

                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(atlasEntity.getGuid(), "entity GUID", methodName, metadataCollectionId);

                    throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());

                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== findEntitiesByClassificationUsingSearchParameters(userId={}, classificationName={}, matchClassificationProperties={}, matchCriteria={}, limitResultsByStatus={}): returnList={}",
                    userId, classificationName, matchClassificationProperties, matchCriteria, limitResultsByStatus, returnList);
        }


        return returnList;

    }



    /*
     * Helper method to avoid needing to pass all the store references to the various mappers
     */
    List<TypeDefAttribute> convertAtlasAttributeDefs(String userId, List<AtlasStructDef.AtlasAttributeDef> aads)
        throws
            RepositoryErrorException,
            TypeErrorException
    {
        // Get a mapper, do the conversion and pass back the result
        AtlasAttributeDefMapper aadm;
        try {
            aadm = new AtlasAttributeDefMapper(this, userId, typeDefStore, typeRegistry, repositoryHelper, typeDefsForAPI, aads);
        } catch (RepositoryErrorException e) {
            LOG.error("convertAtlasAttributeDefs: could not create mapper", e);
            throw e;
        }
        List<TypeDefAttribute> result = aadm.convertAtlasAttributeDefs();

        return result;
    }


    /*
     * Helper method for mappers so that they can retrieve Atlas types from the store
     * The results can be filtered by Atlas TypeCategory if the filter param is not null
     */

    // package private
    TypeDefLink constructTypeDefLink(String typeName, TypeCategory categoryFilter) {
        TypeDefLink tdl = new TypeDefLink();
        try {
            AtlasBaseTypeDef atlasType = typeDefStore.getByName(typeName);
            if (!useRegistry) {
                // Look in the Atlas type def store
                atlasType = typeDefStore.getByName(typeName);
            }
            else {
                // Using registry
                atlasType = typeRegistry.getTypeDefByName(typeName);
            }
            TypeCategory atlasCategory = atlasType.getCategory();
            if (categoryFilter != null && atlasCategory != categoryFilter) {
                // the category does not match the specific filter
                return null;
            }
            else {
                // there is no category filter or there is and the atlasCategory matches it
                tdl.setName(typeName);
                tdl.setGUID(atlasType.getGuid());
            }
        }
        catch (AtlasBaseException e) {
            return null;
        }
        return tdl;
    }

    /**
     * Validate that type's identifier is not null.
     *
     * @param sourceName - source of the request (used for logging)
     * @param guidParameterName - name of the parameter that passed the guid.
     * @param guid - unique identifier for a type or an instance passed on the request
     * @param methodName - method receiving the call
     * @throws TypeErrorException - no guid provided
     */
    public  void validateTypeGUID(String userId,
                                  String sourceName,
                                  String guidParameterName,
                                  String guid,
                                  String methodName) throws TypeErrorException
    {
        if  (guid != null)
        {
            TypeDef foundDef;

            try {
                foundDef = _getTypeDefByGUID(userId, guid);
            }
            catch (Exception e) {
                // swallow this exception - we are throwing TypeErrorException below, if def was not found
                foundDef = null;
            }

            if (foundDef == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(guid, guidParameterName, methodName, sourceName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

            }
        }
    }

    /**
     * Use the paging and sequencing parameters to format the results for a repository call that returns a list of
     * entity instances.
     *
     * @param fullResults - the full list of results in an arbitrary order
     * @param fromElement - the starting element number of the instances to return. This is used when retrieving elements
     *                    beyond the first page of results. Zero means start from the first element.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return results array as requested
     * @throws PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     */
    private List<EntityDetail>    formatEntityResults(List<EntityDetail>   fullResults,
                                                      int                  fromElement,
                                                      String               sequencingProperty,
                                                      SequencingOrder      sequencingOrder,
                                                      int                  pageSize)
            throws
            PagingErrorException,
            PropertyErrorException
    {
        if (fullResults == null)
        {
            return null;
        }

        if (fullResults.isEmpty())
        {
            return null;
        }

        if (fromElement > fullResults.size())
        {
            return null;
        }

        List<EntityDetail>  sortedResults = fullResults;
        // TODO!! sort list according to properties

        if ((pageSize == 0) || (pageSize > sortedResults.size()))
        {
            return sortedResults;
        }

        return new ArrayList<>(fullResults.subList(fromElement, fromElement + pageSize - 1));
    }


    /**
     * Use the paging and sequencing parameters to format the results for a repository call that returns a list of
     * entity instances.
     *
     * @param fullResults - the full list of results in an arbitrary order
     * @param fromElement - the starting element number of the instances to return. This is used when retrieving elements
     *                    beyond the first page of results. Zero means start from the first element.
     * @param sequencingProperty - String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder - Enum defining how the results should be ordered.
     * @param pageSize - the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return results array as requested
     * @throws PropertyErrorException - the sequencing property specified is not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException - the paging/sequencing parameters are set up incorrectly.
     */
    private List<Relationship>    formatRelationshipResults(List<Relationship>   fullResults,
                                                            int                  fromElement,
                                                            String               sequencingProperty,
                                                            SequencingOrder      sequencingOrder,
                                                            int                  pageSize)
            throws
            PagingErrorException,
            PropertyErrorException
    {
        if (fullResults == null)
        {
            return null;
        }

        if (fullResults.isEmpty())
        {
            return null;
        }

        if (fromElement > fullResults.size())
        {
            return null;
        }

        List<Relationship>  sortedResults = fullResults;
        // TODO sort list according to properties

        if ((pageSize == 0) || (pageSize > sortedResults.size()))
        {
            return sortedResults;
        }

        return new ArrayList<>(fullResults.subList(fromElement, fromElement + pageSize - 1));
    }



    private TypeDef convertAtlasTypeDefToOMTypeDef(String userId, AtlasBaseTypeDef abtd)
        throws TypeErrorException, RepositoryErrorException
    {

        final String methodName = "convertAtlasTypeDefToOMTypeDef";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> convertAtlasTypeDefToOMTypeDef: AtlasBaseTypeDef {}", abtd);
        }

        if (abtd == null) {
            LOG.debug("convertAtlasTypeDefToOMTypeDef: cannot convert null type");

            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("abtd", "convertAtlasTypeDefToOMTypeDef", metadataCollectionId);

            throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "convertAtlasTypeDefToOMTypeDef",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());

        }

        String name = abtd.getName();

        // Generate a candidate OM TypeDef
        TypeDef candidateTypeDef;

        // Find the category of the Atlas typedef and invoke the relevant conversion method.
        // The only Atlas type categories that we can convert to OM TypeDef are:
        // ENTITY, RELATIONSHIP & CLASSIFICATION
        // Anything else we will bounce and return null.
        // Each of the processAtlasXXXDef methods will convert from Atlas to OM,
        // perform the RCM helper check, and finally populate the TDBC with the
        // appropriate OM type.
        // On return we just need to retrieve the TypeDef nd return it.
        TypeCategory atlasCat = abtd.getCategory();
        switch (atlasCat) {

            case ENTITY:
                // Detect whether the OM Type is one of the Famous Five
                if (FamousFive.atlasTypeRequiresSubstitution(name)) {
                    // Translate the type name
                    name = FamousFive.getOMTypeName(name);
                }
                AtlasEntityDef atlasEntityDef = (AtlasEntityDef) abtd;
                processAtlasEntityDef(userId, atlasEntityDef);
                candidateTypeDef = typeDefsForAPI.getEntityDef(name);
                break;

            case RELATIONSHIP:
                AtlasRelationshipDef atlasRelationshipDef = (AtlasRelationshipDef) abtd;
                processAtlasRelationshipDef(userId, atlasRelationshipDef);
                candidateTypeDef = typeDefsForAPI.getRelationshipDef(name);
                break;

            case CLASSIFICATION:
                AtlasClassificationDef atlasClassificationDef = (AtlasClassificationDef) abtd;
                processAtlasClassificationDef(userId, atlasClassificationDef);
                candidateTypeDef = typeDefsForAPI.getClassificationDef(name);
                break;

            case PRIMITIVE:
            case ENUM:
            case ARRAY:
            case MAP:
            case STRUCT:
            case OBJECT_ID_TYPE:
            default:
                LOG.debug("convertAtlasTypeDefToOMTypeDef: Atlas type has category cannot be converted to OM TypeDef, category {} ", atlasCat);
                LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.INVALID_TYPEDEF_CATEGORY;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(atlasCat.toString(), name, methodName, repositoryName);

                throw new TypeErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "convertAtlasTypeDefToOMTypeDef",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== convertAtlasTypeDefToOMTypeDef: returning TypeDef {}", candidateTypeDef);
        }
        return candidateTypeDef;
    }


    // package-private
    OMRSRepositoryHelper getRepositoryHelper() {
        return this.repositoryHelper;
    }

    /*
     * Utility method to validate status fields can be modelled in Atlas
     */
    private boolean validateStatusFields(TypeDef typeDef) {

        // Validate the initialStatus and validInstanceStatus fields of the passed TypeDef
        ArrayList<InstanceStatus> statusValuesCorrespondingToAtlas = new ArrayList<>(); // These are OM status values that relate to values valid in Atlas
        statusValuesCorrespondingToAtlas.add(InstanceStatus.ACTIVE);
        statusValuesCorrespondingToAtlas.add(InstanceStatus.DELETED);

        InstanceStatus initialStatus = typeDef.getInitialStatus();
        boolean isInitialStatusOK;
        if (initialStatus != null) {
            isInitialStatusOK = statusValuesCorrespondingToAtlas.contains(initialStatus);
            if (isInitialStatusOK)
                LOG.debug("validateStatusFields initialStatus {} is OK",initialStatus);
            else
                LOG.debug("validateStatusFields initialStatus {} is not OK",initialStatus);
        } else {
            isInitialStatusOK = true;
            LOG.debug("validateStatusFields - initialStatus is null - which is OK",isInitialStatusOK);
        }

        List<InstanceStatus> validStatusList = typeDef.getValidInstanceStatusList();
        boolean isStatusListOK = true;
        if (validStatusList != null) {
            for (InstanceStatus thisStatus : validStatusList) {
                if (!statusValuesCorrespondingToAtlas.contains(thisStatus)) {
                    isStatusListOK = false;
                    LOG.debug("validateStatusFields - validInstanceStatusList contains {} - which is not OK",thisStatus);
                    break;
                }
            }
            if (isStatusListOK)
                LOG.debug("validateStatusFields - all members of validInstanceStatusList are OK");
        } else {
            isStatusListOK = true;
            LOG.debug("validateStatusFields - validInstanceStatusList is null - which is OK");
        }

        return (isInitialStatusOK && isStatusListOK);

    }


    /*
     * Helper method for event mapper
     */


    public String _getTypeDefGUIDByAtlasTypeName(String userId,
                                                 String atlasTypeName)
            throws
            RepositoryErrorException,
            TypeDefNotKnownException
    {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _getTypeDefGUIDByAtlasTypeName(userId={}, atlasTypeName={})", userId, atlasTypeName);
        }
        // Strategy:
        // Check name is not null. null => throw
        // Use Atlas typedef store getByName(), retrieve the GUID and return it.


        // Look in the Atlas type def store

        AtlasBaseTypeDef abtd;
        try {
            if (!useRegistry) {
                // Look in the Atlas type def store
                abtd = typeDefStore.getByName(atlasTypeName);
            }
            else {
                // Using registry
                abtd = typeRegistry.getTypeDefByName(atlasTypeName);
            }

        } catch (AtlasBaseException e) {

            if (e.getAtlasErrorCode() == AtlasErrorCode.TYPE_NAME_NOT_FOUND) {
                LOG.debug("_getTypeDefByName: Atlas does not have the type with name {} ", atlasTypeName);
                // The AttributeTypeDef was not found - return null
                return null;

            } else {
                LOG.error("_getTypeDefByName: Caught exception trying to retrieve Atlas type with name {} ", atlasTypeName, e);
                OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage("name", "_getTypeDefByName", metadataCollectionId);

                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        "_getTypeDefByName",
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        if (abtd == null) {
            LOG.debug("_getTypeDefByName: Atlas does not have the type with name {} ", atlasTypeName);
            OMRSErrorCode errorCode = OMRSErrorCode.NO_TYPEDEF;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage("name", "_getTypeDefByName", metadataCollectionId);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    "_getTypeDefByName",
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }


        String typeDefGUID = abtd.getGuid();

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> _getTypeDefGUIDByAtlasTypeName: atlasTypeDefGUID={}", typeDefGUID);
        }
        return typeDefGUID;

    }


    public void setEventMapper(AtlasOMRSRepositoryEventMapper eventMapper) {
        LOG.debug("setEventMapper: eventMapper being set to {}", eventMapper);
        this.eventMapper = eventMapper;
    }
}