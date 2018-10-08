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
package org.apache.atlas.openmetadata.adapters.repositoryconnector;;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnector;


/**
 * The LocalAtlasOMRSRepositoryConnector is a connector to a local Apache Atlas repository. This is the connector
 * used by the EnterpriseOMRSRepositoryConnector to make a direct call to a local Atlas repository.
 */
public class LocalAtlasOMRSRepositoryConnector extends OMRSRepositoryConnector
{
    /**
     * Default constructor used by the OCF Connector Provider.
     */
    public LocalAtlasOMRSRepositoryConnector()
    {
        /*
         * Nothing to do (yet !)
         */
    }


    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String     metadataCollectionId)
    {
        this.metadataCollectionId = metadataCollectionId;

        /*
         * Initialize the metadata collection only once the connector is properly set up.
         */
        super.metadataCollection = new LocalAtlasOMRSMetadataCollection(
                this,
                super.serverName,
                repositoryHelper,
                repositoryValidator,
                metadataCollectionId);
    }
}