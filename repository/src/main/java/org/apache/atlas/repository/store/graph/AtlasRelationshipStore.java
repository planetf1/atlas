/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasRelationship;

/**
 * Persistence/Retrieval API for AtlasRelationship
 */
public interface AtlasRelationshipStore {
    /**
     * Create a new relationship instance.
     * @param relationship relationship instance definition
     * @return AtlasRelationship d
     */
    AtlasRelationship create(AtlasRelationship relationship) throws AtlasBaseException;

    /**
     * Update an existing relationship instance.
     * @param relationship relationship instance definition
     * @return AtlasRelationship d
     */
    AtlasRelationship update(AtlasRelationship relationship) throws AtlasBaseException;

    /**
     * Retrieve a relationship instance using guid.
     * @param guid relationship instance guid
     * @return AtlasRelationship
     */
    AtlasRelationship getById(String guid) throws AtlasBaseException;

    /**
     * Delete a relationship instance using guid.
     * @param guid relationship instance guid
     */
    void deleteById(String guid) throws AtlasBaseException;
}
