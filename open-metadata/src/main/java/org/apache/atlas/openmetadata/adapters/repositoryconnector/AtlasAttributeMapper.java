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

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;

import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.CollectionDefCategory.OM_COLLECTION_ARRAY;
import static org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.CollectionDefCategory.OM_COLLECTION_MAP;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AtlasAttributeMapper {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAttributeMapper.class);

    private LocalAtlasOMRSMetadataCollection metadataCollection = null;
    private String userId                                       = null;

    public AtlasAttributeMapper(LocalAtlasOMRSMetadataCollection metadataCollection, String userId) {
        this.metadataCollection = metadataCollection;
        this.userId = userId;

    }


    /*
     * Utility method to parse AtlasAttributes into an OM InstanceProperties map.
     *
     * @param typeDef
     * @param atlasAttrs
     * @return InstanceProperties
     */
    public InstanceProperties convertAtlasAttributesToOMProperties(TypeDef typeDef, Map<String, Object> atlasAttrs, boolean uniqueOnly) {

        // Approach:
        // Start with the TypeDef of the immediate type - and work your way up the supertypes. At
        // each level in the inheritance graph observe the property names - i.e. TypeDefAttributes
        // for properties defined at that level, and look for them in the Atlas instance.
        // Stop when you reach the top.
        //
        // For each TDA match by attributeName to key in Atlas entity attributes map.
        // For each matched key, form an OM IPV and put it into an IP map.
        // Finally return the IP map.

        // If uniqueOnly is set then only include attributes that are defined as unique (in the TDA)

        InstanceProperties instanceProperties = null;
        List<TypeDefAttribute> typeDefAttributes = typeDef.getPropertiesDefinition();
        if (typeDefAttributes != null) {
            instanceProperties = new InstanceProperties();
            for (TypeDefAttribute typeDefAttribute : typeDefAttributes) {
                if ( !uniqueOnly || typeDefAttribute.isUnique() ) {
                    String attrName = typeDefAttribute.getAttributeName();
                    // try to access the atlas attribute with this name
                    Object atlasAttrValue = atlasAttrs.get(attrName);
                    if (atlasAttrValue != null) {
                        AttributeTypeDef attributeTypeDef = typeDefAttribute.getAttributeType();
                        AttributeTypeDefCategory category = attributeTypeDef.getCategory();
                        String typeGuid = attributeTypeDef.getGUID();
                        String typeName = attributeTypeDef.getName();
                        InstancePropertyValue instancePropertyValue = null;
                        switch (category) {

                            case PRIMITIVE:
                                PrimitiveDef primitiveDef = (PrimitiveDef) attributeTypeDef;
                                PrimitiveDefCategory primitiveDefCategory = primitiveDef.getPrimitiveDefCategory();
                                PrimitivePropertyValue primitivePropertyValue = new PrimitivePropertyValue();
                                primitivePropertyValue.setPrimitiveDefCategory(primitiveDefCategory);
                                primitivePropertyValue.setTypeGUID(typeGuid);
                                primitivePropertyValue.setTypeName(typeName);
                                primitivePropertyValue.setPrimitiveValue(atlasAttrValue);
                                instancePropertyValue = primitivePropertyValue;
                                break;

                            case COLLECTION:
                                CollectionDef collectionDef = (CollectionDef) attributeTypeDef;
                                CollectionDefCategory collectionDefCategory = collectionDef.getCollectionDefCategory();
                                if (collectionDefCategory == OM_COLLECTION_ARRAY) {
                                    // We are expecting to find an array attribute in the atlas attributes...
                                    int collectionDefArgCount = collectionDef.getArgumentCount();
                                    // An array property must have exactly 1 type.
                                    if (collectionDefArgCount != 1) {
                                        LOG.error("convertAtlasAttributesToOMProperties: array collection attributeTypeDef has {} type arguments", collectionDefArgCount);
                                        // instancePropertyValue remains null - attribute will be skipped at end of loop
                                    }
                                    // OM only deals with collections of primitives.
                                    PrimitiveDefCategory primDefCat = collectionDef.getArgumentTypes().get(0);
                                    // Get the object value from Atlas and create an OM property value of appropriate type, we are expecting type defined by primDefCat
                                    ArrayPropertyValue arrayPropertyValue = null;
                                    ArrayList atlasArray = (ArrayList)atlasAttrValue;
                                    int atlasArrayLen = atlasArray.size();
                                    if (atlasArrayLen > 0) {
                                        arrayPropertyValue = new ArrayPropertyValue();
                                        arrayPropertyValue.setTypeGUID(typeGuid);
                                        arrayPropertyValue.setTypeName(typeName);
                                        // InstanceProperties is map from String to InstancePropertyValue
                                        // InstancePropertyValue needs typeName, typeGuid and InstancePropertyCategory
                                        // InstancePropertyCategory must be primitive (see above)
                                        InstanceProperties arrayProps = new InstanceProperties();
                                        for (int i=0; i<atlasArrayLen; i++) {
                                            Object val = atlasArray.get(i);
                                            // Create an InstancePropertyValue
                                            PrimitivePropertyValue ppv = new PrimitivePropertyValue();
                                            ppv.setPrimitiveDefCategory(primDefCat);
                                            ppv.setTypeName(primDefCat.getName());
                                            ppv.setTypeGUID(primDefCat.getGUID());
                                            ppv.setPrimitiveValue(val);
                                            // OM array is a map with String keys reflecting array indices...
                                            arrayProps.setProperty(""+i,ppv);
                                        }
                                        arrayPropertyValue.setArrayValues(arrayProps);
                                        arrayPropertyValue.setArrayCount(atlasArrayLen);
                                    }
                                    instancePropertyValue = arrayPropertyValue;
                                }
                                else  if (collectionDefCategory == OM_COLLECTION_MAP) {
                                    // We are expecting to find a map attribute in the atlas attributes...
                                    int collectionDefArgCount = collectionDef.getArgumentCount();
                                    // A map property must have exactly 2 type arguments
                                    if (collectionDefArgCount != 2) {
                                        LOG.error("convertAtlasAttributesToOMProperties: map collection attributeTypeDef has {} type arguments", collectionDefArgCount);
                                        // instancePropertyValue remains null - attribute will be skipped at end of loop
                                    }
                                    // OM only deals with collections of primitives.
                                    // In general, maps are always <String,String> but tolerate <String,Primitive> - cannot relax the key type as InstanceProperties defines key type as String
                                    PrimitiveDefCategory primDefCatKey = collectionDef.getArgumentTypes().get(0);
                                    PrimitiveDefCategory primDefCatVal = collectionDef.getArgumentTypes().get(1);
                                    // Get the object value from Atlas and create an OM property value of appropriate type, we are expecting type defined by primDefCat
                                    MapPropertyValue mapPropertyValue = null;
                                    try {
                                        Map<String, Object> atlasMap = (Map<String, Object>) atlasAttrValue;

                                        int atlasMapSize = atlasMap.size();
                                        if (atlasMapSize > 0) {
                                            mapPropertyValue = new MapPropertyValue();
                                            mapPropertyValue.setTypeGUID(typeGuid);
                                            mapPropertyValue.setTypeName(typeName);
                                            // InstanceProperties is map from String to InstancePropertyValue
                                            // InstancePropertyValue needs typeName, typeGUID and InstancePropertyCategory (which is set on construction of subtype)
                                            // InstancePropertyCategory must be primitive (see above)
                                            InstanceProperties mapProps = new InstanceProperties();

                                            for (Object oKey : atlasMap.keySet()) {                      // for each Atlas map entry
                                                if (oKey instanceof String) {            // otherwise this map entry will be skipped
                                                    String key = (String) oKey;
                                                    Object val = atlasMap.get(key);
                                                    // Create an InstancePropertyValue
                                                    PrimitivePropertyValue ppv = new PrimitivePropertyValue();
                                                    ppv.setPrimitiveDefCategory(primDefCatVal);
                                                    ppv.setTypeName(primDefCatVal.getName());
                                                    ppv.setTypeGUID(primDefCatVal.getGUID());
                                                    ppv.setPrimitiveValue(val);
                                                    // OM array is a map with String keys reflecting array indices...
                                                    mapProps.setProperty(key, ppv);
                                                }
                                            }
                                            mapPropertyValue.setMapValues(mapProps);
                                        }
                                        instancePropertyValue = mapPropertyValue;
                                    }
                                    catch (ClassCastException e) {
                                        // Could not cast atlas attribute to Map<String,Object> - game over...
                                        LOG.debug("convertAtlasAttributesToOMProperties: cannot handle an Atlas attribute {} as it is not a map<String,Object>", attrName);
                                        // instancePropertyValue remains null - attribute will be skipped at end of loop
                                    }
                                }
                                else {
                                    LOG.debug("convertAtlasAttributesToOMProperties: ignoring collection attribute {} - not an array or map",attrName);
                                    // instancePropertyValue remains null - attribute will be skipped at end of loop
                                }

                                break;

                            case ENUM_DEF:
                                EnumPropertyValue enumPropertyValue = new EnumPropertyValue();
                                enumPropertyValue.setTypeGUID(typeGuid);
                                enumPropertyValue.setTypeName(typeName);
                                EnumDef enumDef = (EnumDef) attributeTypeDef;
                                List<EnumElementDef> elems = enumDef.getElementDefs();
                                boolean matched = false;
                                for (EnumElementDef elem : elems) {
                                    if (elem.getValue().equals(atlasAttrValue)) {
                                        enumPropertyValue.setSymbolicName(elem.getValue());
                                        enumPropertyValue.setOrdinal(elem.getOrdinal());
                                        enumPropertyValue.setDescription(elem.getDescription());
                                        matched = true;
                                        break;
                                    }
                                }
                                if (matched) {
                                    instancePropertyValue = enumPropertyValue;
                                } else {
                                    LOG.debug("convertAtlasAttributesToOMProperties: could not match enum value in attribute {}", attrName);
                                    // instancePropertyValue remains null - attribute will be skipped at end of loop
                                }
                                break;

                            case UNKNOWN_DEF:
                            default:
                                LOG.debug("convertAtlasAttributesToOMProperties: attribute {} has unknown category - attribute ignored", attrName);
                                break;
                        }
                        // Add the IPV to the IP map
                        if (instancePropertyValue != null) {
                            instanceProperties.setProperty(attrName, instancePropertyValue);
                        }
                    } else {
                        LOG.debug("convertAtlasAttributesToOMProperties: attribute {} value is null - attribute ignored", attrName);
                    }
                }
            }
        }
        // Visit the next type in the supertype hierarchy, if any
        TypeDefLink superTypeLink = typeDef.getSuperType();
        if (superTypeLink != null) {
            // Retrieve the supertype - the TDL gives us its GUID and name
            if (superTypeLink.getName() != null) {
                TypeDef superTypeDef = null;
                try {
                    superTypeDef = metadataCollection._getTypeDefByName(userId, superTypeLink.getName());
                } catch (Exception e) {
                    LOG.error("convertAtlasAttributesToOMProperties: caught exception from getTypeDefByName for {}", superTypeLink.getName(), e);
                }
                if (superTypeDef != null) {
                    InstanceProperties additionalProps = convertAtlasAttributesToOMProperties(superTypeDef, atlasAttrs, uniqueOnly);
                    if (additionalProps != null) {
                        // Add the additional properties to any we already found at this level...
                        if (instanceProperties == null) {
                            // We did not already find any properties (at the original instance level) so need
                            // to allocate the InstanceProperties now.
                            instanceProperties = new InstanceProperties();
                        }
                        Iterator<String> propIter = additionalProps.getPropertyNames();
                        while (propIter.hasNext()) {
                            String propName = propIter.next();
                            instanceProperties.setProperty(propName, additionalProps.getPropertyValue(propName));
                        }
                    }
                }
            }
        }
        return instanceProperties;
    }
}
