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
package org.apache.atlas.openmetadata.adapters.eventmapper;



import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.notification.entity.EntityMessageDeserializer;
import org.apache.atlas.openmetadata.adapters.repositoryconnector.LocalAtlasOMRSErrorCode;
import org.apache.atlas.openmetadata.adapters.repositoryconnector.LocalAtlasOMRSMetadataCollection;
import org.apache.atlas.openmetadata.adapters.repositoryconnector.LocalAtlasOMRSRepositoryConnector;
import static org.apache.atlas.kafka.KafkaNotification.ATLAS_ENTITIES_TOPIC;

import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditLog;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditingComponent;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.EntityDetail;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceType;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryeventmapper.OMRSRepositoryEventMapperBase;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryeventmapper.OMRSRepositoryEventMapperConnector;
import org.odpi.openmetadata.repositoryservices.events.OMRSEventCategory;
import org.odpi.openmetadata.repositoryservices.events.OMRSEventOriginator;
import org.odpi.openmetadata.repositoryservices.events.OMRSInstanceEventType;
import org.odpi.openmetadata.repositoryservices.events.beans.v1.OMRSEventV1;
import org.odpi.openmetadata.repositoryservices.events.beans.v1.OMRSEventV1InstanceSection;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;




/**
 * AtlasOMRSRepositoryEventMapper provides an implementation of a repository event mapper for the
 * Apache Atlas metadata repository.
 */

public class AtlasOMRSRepositoryEventMapper extends OMRSRepositoryEventMapperConnector {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasOMRSRepositoryEventMapper.class);
    private static final OMRSAuditLog AUDITLOG = new OMRSAuditLog(OMRSAuditingComponent.LOCAL_REPOSITORY_EVENT_MAPPER);

    // Kafka Topic Consumer
    // Some properties can be hard coded but the bootstrap server need to be read from the Atlas properties
    private static final String TOPIC_NAME                 = ATLAS_ENTITIES_TOPIC;
    private static final String CONSUMER_GROUP_ID          = "AtlasOMRSRepositoryEventMapperConsumerGroup";  // OK to hard code - only the event mapper should be using this
    private static final String BOOTSTRAP_SERVERS_DEFAULT  = "localhost:9027";
    private static final String BOOTSTRAP_SERVERS_PROPERTY = "atlas.kafka.bootstrap.servers";

    private String                           bootstrapServers   = null;
    private RunnableConsumer                 runnableConsumer   = null;
    private Thread                           consumerThread     = null;
    private EntityMessageDeserializer        deserializer       = new EntityMessageDeserializer();
    private LocalAtlasOMRSMetadataCollection metadataCollection = null;

    /**
     * Default constructor
     */
    public AtlasOMRSRepositoryEventMapper() throws RepositoryErrorException {
        super();
        LOG.debug("AtlasOMRSRepositoryEventMapper constructor invoked");

        try {
            bootstrapServers = ApplicationProperties.get().getString(BOOTSTRAP_SERVERS_PROPERTY, BOOTSTRAP_SERVERS_DEFAULT);
            LOG.debug("AtlasOMRSRepositoryEventMapper: bootstrapServers {}",bootstrapServers);
        } catch (AtlasException e) {
            LOG.error("AtlasOMRSRepositoryEventMapper: Could not find bootstrap servers, giving up");
            String actionDescription = "LocalAtlasOMRSMetadataCollection Constructor";

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ATLAS_CONFIGURATION;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    actionDescription,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        this.runnableConsumer = createConsumer();

    }

    private RunnableConsumer createConsumer() {

        KafkaConsumer consumer = null;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);

        // Deserialization - at this level should support Kafka records - String,String should be sufficient
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        RunnableConsumer runnableConsumer = new RunnableConsumer(consumer);

        return runnableConsumer;

    }

    public class RunnableConsumer implements Runnable {
        private final KafkaConsumer consumer;
        private       boolean       keepOnRunning;

        // package-private
        RunnableConsumer(KafkaConsumer consumer) {
            this.consumer = consumer;
            keepOnRunning = true;
        }

        // package-private
        void stopRunning() {
            keepOnRunning = false;
        }

        @Override
        public void run() {

            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasOMRSRepositoryEventMapper RunnableConsumer.run()");
            }

            while (keepOnRunning) {

                ConsumerRecords<?, ?> records  = consumer.poll(1000);
                if (records != null) {

                     /* Since the resolution to ATLAS-2853, it is safe to immediately process any notifications, because
                      * the Atlas graph transaction will already have committed prior to the notification having been semt.
                      */

                    for (ConsumerRecord<?, ?> record : records) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received Message topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        }

                        EntityNotification entityNotification = deserializer.deserialize(record.value().toString());
                        if (entityNotification != null) {
                            processEntityNotification(entityNotification);
                        }
                    }
                }
                consumer.commitAsync();
            }
            consumer.close();

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RunnableConsumer.run() ending");
            }
        }
    }

    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException  {

        final String methodName = "start";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasOMRSRepositoryEventMapper start()");
        }
        super.start();

        LOG.debug("AtlasOMRSRepositoryEventMapper.start: metadataCollectionId {}, repositoryConnector {}, localServerUserId {}", localMetadataCollectionId, repositoryConnector, localServerUserId);

        // Get and verify the metadataCollection...
        boolean metadataCollectionOK = false;
        LocalAtlasOMRSRepositoryConnector repositoryConnector = (LocalAtlasOMRSRepositoryConnector) this.repositoryConnector;
        try {
            metadataCollection = (LocalAtlasOMRSMetadataCollection) repositoryConnector.getMetadataCollection();
        }
        catch (RepositoryErrorException e) {
            LOG.error("AtlasOMRSRepositoryEventMapper.start: Exception from getMetadataCollection, message = {}", e.getMessage());
            metadataCollectionOK = false;
        }
        if (metadataCollection != null) {
            // Check that the metadataCollection is responding...
            try {
                String id = metadataCollection.getMetadataCollectionId();
                if (id.equals(localMetadataCollectionId)) {
                    LOG.debug("AtlasOMRSRepositoryEventMapper.start: metadataCollection verified");
                    metadataCollectionOK = true;
                } else {
                    LOG.error("AtlasOMRSRepositoryEventMapper.start: Could not retrieve metadataCollection");
                    metadataCollectionOK = false;
                }
            } catch (RepositoryErrorException e) {
                metadataCollectionOK = false;
            }
        }
        if (!metadataCollectionOK) {
            LOG.error("AtlasOMRSRepositoryEventMapper.ctor: Could not access metadata collection");

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.METADATA_COLLECTION_NOT_FOUND;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(localMetadataCollectionId);

            throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }

        /* Register the event mapper with the metadataCollection. This is so that the metadataCollection can respond
         * to requests such as refreshEntityReferenceCopy by delegating to the event mapper to call the event processor.
         */
        LOG.debug("AtlasOMRSRepositoryEventMapper: set eventMapper in metadataCollection");
        metadataCollection.setEventMapper(this);

        if (this.runnableConsumer == null) {
            LOG.error("AtlasOMRSRepositoryEventMapper: No runnable consumer!!!");

           OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(repositoryEventMapperName, methodName, "RunnableConsumer not created");

            throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        else {
            consumerThread = new Thread(runnableConsumer);
            consumerThread.setDaemon(true);
            consumerThread.start();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasOMRSRepositoryEventMapper start()");
        }
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void disconnect() throws ConnectorCheckedException {
        LOG.debug("AtlasOMRSRepositoryEventMapper disconnect()");
        runnableConsumer.stopRunning();
        LOG.debug("AtlasOMRSRepositoryEventMapper: runnable consumer thread told to stop");
        super.disconnect();

    }

    /**
     * Method to process an EntityNotification message
     *
     * @param notification - EntityNotification received from ATLAS_ENTITIES topic
     */
    public void processEntityNotification(EntityNotification notification) {

        LOG.debug("AtlasOMRSRepositoryEventMapper.processMessage {}", notification);

        // check the notification version (sometimes referred to as 'type')
        EntityNotification.EntityNotificationType version = notification.getType();
        switch (version) {

            case ENTITY_NOTIFICATION_V2:
                EntityNotification.EntityNotificationV2 notificationV2 = (EntityNotification.EntityNotificationV2)notification;
                handleAtlasEntityNotification(notificationV2);
                break;

            case ENTITY_NOTIFICATION_V1:
            default:
                LOG.error("AtlasOMRSRepositoryEventMapper.processMessage: Message skipped!! - not expecting a V1 entity notification...");
                // skip the message...
                break;
        }

    }


    private void handleAtlasEntityNotification(EntityNotification.EntityNotificationV2 notification) {

        final String methodName = "AtlasOMRSRepositoryEventMapper.processEvent";

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}: notification={}", methodName, notification);
        }

        /*
         * Depending on the operation being notified, this method needs to retrieve slightly different
         * things to pass to the event processor. In almost all cases, it needs to retrieve the entity detail
         * from the store. In the case of a purge (hard) delete there will be no entity detail in the store,
         * but the event processor for purge only needs the entity GUID, type name and type GUID. The first two
         * of these are available in the AtlasEntityHeader given in the notification. The type GUID will require
         * a request to the AtlasTypeStore or TypeRegistry. The event mapper makes use of the metadatacollection
         * for all access to Atlas.
         *
         * The key thing to establish is a) whether this is an entity deleted notification and b) whether Atlas
         * is configured for hard delete. If both of those are true then we need to use the type name to retrieve
         * the TypeDefGUID from Atlas.
         */

        EntityNotification.EntityNotificationV2.OperationType operationType = notification.getOperationType();
        long notificationTime = notification.getEventTime();
        // No way yet to pass timestamp to event processor methods, but have raised EGERIA-52. For now just log it (again)...
        LOG.debug("Notification has eventTime {}", notificationTime);

        AtlasEntityHeader atlasEntityHeader = notification.getEntity();
        LOG.debug("{}: atlasEntityHeader={}", methodName, atlasEntityHeader);

        String atlasEntityGuid = atlasEntityHeader.getGuid();
        String atlasTypeName = atlasEntityHeader.getTypeName();
        String typeDefGUID = null;

        EntityDetail entityDetail = null;

        try {
            // Check if the entity is a proxy and if so ignore it...
            boolean isProxy = metadataCollection.isEntityProxy(atlasEntityGuid);
            if (isProxy) {
                // The entity is a proxy - it was added by an OMRS action and we are not interested in this atlas event
                LOG.debug("{}: event relates to an EntityProxy with guid {} - ignored", methodName, atlasEntityGuid);
                return;
            }
            // Check if it is a remote object and if so ignore it ...
            boolean isLocal = metadataCollection.isEntityLocal(atlasEntityGuid);
            if (!isLocal) {
                // The entity is remote - we are not interested in this atlas event
                LOG.debug("{}: event relates to a remotely mastered Entity with guid {} - ignored", methodName, atlasEntityGuid);
                return;
            }
            // (!isProxy && isLocal)
            LOG.debug("{}: event relates to a locally mastered Entity with guid {} - process", methodName, atlasEntityGuid);

            // If this is a delete notification we need to establish whether Atlas is performing hard dor soft deletes.

            if (operationType == EntityNotification.EntityNotificationV2.OperationType.ENTITY_DELETE && metadataCollection.isAtlasConfiguredForHardDelete() == true) {
                // Use the entity type name to retrieve the type GUID, to pass to the purge processor.
                typeDefGUID = metadataCollection._getTypeDefGUIDByAtlasTypeName(this.localServerUserId, atlasTypeName);
                LOG.debug("{}: typeDefGUID = {}", methodName, typeDefGUID);
            }
            else {
                // Retrieve the EntityDetail, to pass to the purge processor.
                entityDetail = metadataCollection.getEntityDetail(this.localServerUserId, atlasEntityGuid);
                LOG.debug("{}: entity detail = {}", methodName, entityDetail);
            }


        } catch (Exception e) {
            LOG.error("{}: Exception from metadataCollection, message={}", methodName, e.getMessage());
            // It is possible that the event is plain wrong. Ignore the event...
            return;
        }


        // By this point we either have a typeDefGUID, typeName and entityGUID or we have a complete entityDetail...

        // We have not yet set the eventType but that is done inline below with the call to the event processor...

        // Atlas does not yet generate undone, purged, restored, rehomed, reidentified or retyped events or refresh requests or events
        // It doesn't support relationship events yet either
        // Nor is there any support for conflicting types or conflicting instances events


        LOG.debug("{}: Handle event: operationType {}, typeDefGUID {}, typeDefName {}",
                    methodName, operationType, typeDefGUID, atlasTypeName );

        switch (operationType) {

            case ENTITY_CREATE:
                LOG.debug("{}: invoke processNewEntityEvent", methodName);
                repositoryEventProcessor.processNewEntityEvent(
                        repositoryEventMapperName,
                        localMetadataCollectionId,
                        localServerName,
                        localServerType,
                        localOrganizationName,
                        entityDetail);
                break;

            case ENTITY_UPDATE:
                LOG.debug("{}: invoke processUpdatedEntityEvent", methodName);
                repositoryEventProcessor.processUpdatedEntityEvent(
                        repositoryEventMapperName,
                        localMetadataCollectionId,
                        localServerName,
                        localServerType,
                        localOrganizationName,
                        null,        // We do not have the old entity - this will be addressed further up the stack if available
                        entityDetail);
                break;

            case ENTITY_DELETE:
                // If hard delete, invoke purge processor, else invoke delete processor...
                if (metadataCollection.isAtlasConfiguredForHardDelete() == true) {
                    LOG.debug("{}: invoke processDeletedEntityEvent", methodName);
                    repositoryEventProcessor.processPurgedEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            typeDefGUID,
                            atlasTypeName,
                            atlasEntityGuid);
                } else {
                    LOG.debug("{}: invoke processDeletedEntityEvent", methodName);
                    repositoryEventProcessor.processDeletedEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            entityDetail);
                }
                break;

            case CLASSIFICATION_ADD:
                LOG.debug("{}: invoke processClassifiedEntityEvent", methodName);
                repositoryEventProcessor.processClassifiedEntityEvent(
                        repositoryEventMapperName,
                        localMetadataCollectionId,
                        localServerName,
                        localServerType,
                        localOrganizationName,
                        entityDetail);
                break;

            case CLASSIFICATION_UPDATE:
                LOG.debug("{}: invoke processReclassifiedEntityEvent", methodName);
                repositoryEventProcessor.processReclassifiedEntityEvent(
                        repositoryEventMapperName,
                        localMetadataCollectionId,
                        localServerName,
                        localServerType,
                        localOrganizationName,
                        entityDetail);
                break;

            case CLASSIFICATION_DELETE:
                LOG.debug("{}: invoke processDeclassifiedEntityEvent", methodName);
                repositoryEventProcessor.processDeclassifiedEntityEvent(
                        repositoryEventMapperName,
                        localMetadataCollectionId,
                        localServerName,
                        localServerType,
                        localOrganizationName,
                        entityDetail);
                break;

            default:
                LOG.error("{}: operation type {} not supported", methodName, operationType);
                break;
        }

    }


    /*
     * Helper method for repository connector metadata collection
     */
    public void processRefreshEvent(EntityDetail entityDetail) {
        final String methodName = "processRefreshEvent";

        LOG.debug("{}: invoke processNewEntityEvent", methodName);

        repositoryEventProcessor.processRefreshEntityEvent(
                repositoryEventMapperName,
                localMetadataCollectionId,
                localServerName,
                localServerType,
                localOrganizationName,
                entityDetail);
    }

}