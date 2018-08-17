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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;


/**
 * The LocalAtlasOMRSErrorCode contains additional error codes specific to the AtlasConnector that are not in OMRSErrorCode.
 *
 * The 5 fields in the enum are:
 * <ul>
 *     <li>HTTP Error Code - for translating between REST and JAVA - Typically the numbers used are:</li>
 *     <li><ul>
 *         <li>500 - internal error</li>
 *         <li>501 - not implemented </li>
 *         <li>503 - Service not available</li>
 *         <li>400 - invalid parameters</li>
 *         <li>401 - unauthorized</li>
 *         <li>404 - not found</li>
 *         <li>405 - method not allowed</li>
 *         <li>409 - data conflict errors - eg item already defined</li>
 *     </ul></li>
 *     <li>Error Message Id - to uniquely identify the message</li>
 *     <li>Error Message Text - includes placeholder to allow additional values to be captured</li>
 *     <li>SystemAction - describes the result of the error</li>
 *     <li>UserAction - describes how a user should correct the error</li>
 * </ul>
 */

public enum LocalAtlasOMRSErrorCode
{
    NULL_INSTANCE(400, "OMRS-ATLAS-REPOSITORY-400-001 ",
            "The value specified for \"{0}\" is null",
            "The system is unable to proceed because no instance has been provided.",
            "Check the system logs and diagnose or report the problem."),
    NULL_VERSION(400, "OMRS-ATLAS-REPOSITORY-400-002 ",
            "The value specified for \"{0}\" is null",
            "The system is unable to proceed because the value is invalid.",
            "Check the system logs and diagnose or report the problem."),
    NULL_PARAMETER(400, "OMRS-ATLAS-REPOSITORY-400-003 ",
            "The value specified for \"{0}\" passed to method \"{1}\" is null",
            "The system is unable to proceed because the value is invalid.",
            "Check the system logs and diagnose or report the problem."),
    INVALID_PARAMETER(400, "OMRS-ATLAS-REPOSITORY-400-004 ",
            "The value specified for \"{0}\" passed to method \"{1}\" is invalid",
            "The system is unable to proceed because the value is invalid.",
            "Check the system logs and diagnose or report the problem."),
    ATLAS_CONFIGURATION(400, "OMRS-ATLAS-REPOSITORY-400-005 ",
            "Could not access the delete handler configuration for Atlas repository",
            "The connector is unable to proceed",
            "Check the system logs and diagnose or report the problem."),
    ATLAS_CONFIGURATION_HARD(400, "OMRS-ATLAS-REPOSITORY-400-006 ",
            "A soft delete cannot be performed for the instance with GUID \"{0}\" by method \"{1}\" on repository \"{2}\"",
            "The repository is configured to perform hard (permanent) deletes",
            "Check the repository configuration or use purge instead."),
    ENTITY_NOT_DELETED(400, "OMRS-ATLAS-REPOSITORY-400-007 ",
            "A purge of an entity resulted in an exception, entity GUID \"{0}\" by method \"{1}\" on repository \"{2}\"",
            "The repository failed to purge an entity",
            "Check the existence and state of the entity in the repository."),
    RELATIONSHIP_NOT_DELETED(400, "OMRS-ATLAS-REPOSITORY-400-007 ",
            "A purge of an relationship resulted in an exception, relationship GUID \"{0}\" by method \"{1}\" on repository \"{2}\"",
            "The repository failed to purge an relationship",
            "Check the existence and state of the relationship in the repository."),
    ENTITY_IS_PROXY(400, "OMRS-ATLAS-REPOSITORY-400-008 ",
            "The Atlas entity with GUID \"{0}\" is a proxy, as reported by method \"{1}\" on repository \"{2}\"",
            "The Atlas entity cannot be projected as an EntityDetail",
            "Check why the caller is expecting to find a complete entity."),
    INVALID_PROPAGATION_RULE(400, "OMRS-ATLAS-REPOSITORY-400-009 ",
            "The propagation rule \"{0}\" is not valid, as reported by method \"{1}\" on repository \"{2}\"",
            "The propagation rule cannot be converted to an Atlas propagateTags value",
            "Check the propagation rule supplied."),
    INVALID_PROPERTY_CATEGORY(400, "OMRS-ATLAS-REPOSITORY-400-010 ",
            "The property category \"{0}\" is not valid, as reported by method \"{1}\" on repository \"{2}\"",
            "The property category is not supported by the Atlas connector",
            "Check the property category supplied."),
    INVALID_TYPEDEF_CATEGORY(400, "OMRS-ATLAS-REPOSITORY-400-011 ",
            "The category \"{0}\" of typedef \"{1}\" is not valid, as reported by method \"{2}\" on repository \"{3}\"",
            "The typedef category is not supported by the Atlas connector",
            "Check the typedef category supplied."),
    REPOSITORY_TYPEDEF_CREATE_FAILED(400, "OMRS-ATLAS-REPOSITORY-400-012 ",
            "The typedef \"{0}\" could not be created, as reported by method \"{1}\" on repository \"{2}\"",
            "The typedef could not be created by the Atlas repository",
            "Check the typedef supplied and the state of the repository."),
    INVALID_EVENT_FORMAT(400, "OMRS-ATLAS-REPOSITORY-400-013 ",
            "Event \"{0}\" on topic \"{1}\" could not be parsed, by the Atlas event mapper",
            "The system is unable to process the request.",
            "Verify the format of events published to the topic."),
    ATLAS_CONFIGURATION_BOOTSTRAP_SERVERS(400, "OMRS-ATLAS-REPOSITORY-400-013 ",
            "Could not access the bootstrap server configuration for Atlas repository",
            "The connector is unable to proceed",
            "Check the system logs and diagnose or report the problem."),
    METADATA_COLLECTION_NOT_FOUND(400, "OMRS-ATLAS-REPOSITORY-400-014 ",
            "Could not access the metadata collection with id \"{0}\" for the Atlas repository",
            "The connector is unable to proceed",
            "Check the system logs and diagnose or report the problem."),
    ENTITY_NOT_CREATED(400, "OMRS-ATLAS-REPOSITORY-400-015 ",
            "Failed to store the entity \"{0}\" in method \"{1}\" into the Atlas repository \"{2}\"",
            "The entity could not be stored in the repository",
            "Check the system logs and diagnose or report the problem."),
    PAGING_ERROR(400, "OMRS-ATLAS-REPOSITORY-400-016 ",
            "Applying offset \"{0}\" and pageSize \"{1}\" to the result list failed in method \"{2}\" into the Atlas repository \"{3}\"",
            "The entity result list could not be processed",
            "Check the system logs and diagnose or report the problem."),
    EVENT_MAPPER_NOT_INITIALIZED(400, "OMRS-ATLAS-REPOSITORY-400-017 ",
            "There is no valid event mapper for repository \"{1}\"",
            "The refresh request could not be processed",
            "Check the system logs and diagnose or report the problem."),
    REPOSITORY_ERROR(400, "OMRS-ATLAS-REPOSITORY-400-018 ",
            "Caught an exception repository \"{1}\"",
            "The requested operation failed",
            "Examine the exception to diagnose or report the problem.")



    ;

    private int    httpErrorCode;
    private String errorMessageId;
    private String errorMessage;
    private String systemAction;
    private String userAction;

    private static final Logger log = LoggerFactory.getLogger(LocalAtlasOMRSErrorCode.class);


    /**
     * The constructor for LocalAtlasOMRSErrorCode expects to be passed one of the enumeration rows defined in
     * LocalAtlasOMRSErrorCode above.   For example:
     *
     *     LocalAtlasOMRSErrorCode   errorCode = LocalAtlasOMRSErrorCode.NULL_INSTANCE;
     *
     * This will expand out to the 5 parameters shown below.
     *
     * @param newHTTPErrorCode - error code to use over REST calls
     * @param newErrorMessageId - unique Id for the message
     * @param newErrorMessage - text for the message
     * @param newSystemAction - description of the action taken by the system when the error condition happened
     * @param newUserAction - instructions for resolving the error
     */
    LocalAtlasOMRSErrorCode(int  newHTTPErrorCode, String newErrorMessageId, String newErrorMessage, String newSystemAction, String newUserAction)
    {
        this.httpErrorCode  = newHTTPErrorCode;
        this.errorMessageId = newErrorMessageId;
        this.errorMessage   = newErrorMessage;
        this.systemAction   = newSystemAction;
        this.userAction     = newUserAction;
    }


    public int getHTTPErrorCode()
    {
        return httpErrorCode;
    }


    /**
     * Returns the unique identifier for the error message.
     *
     * @return errorMessageId
     */
    public String getErrorMessageId()
    {
        return errorMessageId;
    }


    /**
     * Returns the error message with placeholders for specific details.
     *
     * @return errorMessage (unformatted)
     */
    public String getUnformattedErrorMessage()
    {
        return errorMessage;
    }


    /**
     * Returns the error message with the placeholders filled out with the supplied parameters.
     *
     * @param params - strings that plug into the placeholders in the errorMessage
     * @return errorMessage (formatted with supplied parameters)
     */
    public String getFormattedErrorMessage(String... params)
    {
        if (log.isDebugEnabled())
        {
            log.debug(String.format("==> LocalAtlasOMRSErrorCode.getMessage(%s)", Arrays.toString(params)));
        }

        MessageFormat mf = new MessageFormat(errorMessage);
        String result = mf.format(params);

        if (log.isDebugEnabled())
        {
            log.debug(String.format("<== LocalAtlasOMRSErrorCode.getMessage(%s): %s", Arrays.toString(params), result));
        }

        return result;
    }


    /**
     * Returns a description of the action taken by the system when the condition that caused this exception was
     * detected.
     *
     * @return systemAction
     */
    public String getSystemAction()
    {
        return systemAction;
    }


    /**
     * Returns instructions of how to resolve the issue reported in this exception.
     *
     * @return userAction
     */
    public String getUserAction()
    {
        return userAction;
    }
}
