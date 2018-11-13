/*-
 * ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 * Autonomic Proprietary 1.0
 * ——————————————————————————————————————————————————————————————————————————————
 * Copyright (C) 2018 Autonomic, LLC - All rights reserved
 * ——————————————————————————————————————————————————————————————————————————————
 * Proprietary and confidential.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Autonomic, LLC and its suppliers, if any.  The intellectual and technical
 * concepts contained herein are proprietary to Autonomic, LLC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and are
 * protected by trade secret or copyright law. Dissemination of this information
 * or reproduction of this material is strictly forbidden unless prior written
 * permission is obtained from Autonomic, LLC.
 * 
 * Unauthorized copy of this file, via any medium is strictly prohibited.
 * ______________________________________________________________________________
 */
package com.autonomic.iam.client.grpc;

import static com.google.common.base.Preconditions.checkState;

import com.autonomic.iam.client.exceptions.AlreadyExistsException;
import com.autonomic.iam.client.exceptions.BadArgumentException;
import com.autonomic.iam.client.exceptions.FailedPreconditionException;
import com.autonomic.iam.client.exceptions.MalformedDataException;
import com.autonomic.iam.client.exceptions.NotFoundException;
import com.autonomic.iam.client.models.AuthorizationRequest;
import com.autonomic.iam.client.models.AuthorizationResponse;
import com.autonomic.iam.client.models.BatchAuthorizationRequest;
import com.autonomic.iam.client.models.BatchAuthorizationResponse;
import com.autonomic.iam.client.models.ObjectAccessRequest;
import com.autonomic.iam.client.models.ObjectAccessResponse;
import com.autonomic.iam.client.models.SearchResponse;
import com.autonomic.iam.common.models.Action;
import com.autonomic.iam.common.models.PermissionModel;
import com.autonomic.iam.common.models.Resource;
import com.autonomic.iam.common.models.roles.Role;
import com.autonomic.iam.common.models.roles.RoleDefinition;
import com.autonomic.iam.common.serde.Serde;
import com.autonomic.iam.proto.CreatePermissionRequest;
import com.autonomic.iam.proto.CreateUserRequest;
import com.autonomic.iam.proto.DeleteUserRequest;
import com.autonomic.iam.proto.GetAuthorizationRequest;
import com.autonomic.iam.proto.GetAuthorizationResponse;
import com.autonomic.iam.proto.GetAuthorizationResponse.AuthorizationStatus;
import com.autonomic.iam.proto.GetBatchAuthorizationRequest;
import com.autonomic.iam.proto.GetBatchAuthorizationResponse;
import com.autonomic.iam.proto.GetRoleRequest;
import com.autonomic.iam.proto.GetRoleResponse;
import com.autonomic.iam.proto.IamServiceGrpc;
import com.autonomic.iam.proto.IamServiceGrpc.IamServiceBlockingStub;
import com.autonomic.iam.proto.Permission;
import com.autonomic.iam.proto.SearchPermissionsRequest;
import com.autonomic.iam.proto.SearchPermissionsResponse;
import com.autonomic.iam.proto.UpsertPermissionRequest;
import com.autonomic.iam.proto.UpsertPermissionResponse;
import com.autonomic.qlang.protos.Expression;
import com.autonomic.qlang.protos.Expressions;
import com.autonomic.qlang.protos.Junct;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IamClient {
    private static final Logger logger = LoggerFactory.getLogger(IamClient.class);

    private static final String CALLER_ID_KEY = "CALLER_ID";

    private ChannelBuilder builder;
    protected RetryingStub<IamServiceBlockingStub> retryingStub;

    private final ObjectMapper serde = Serde.getObjectMapper();
    private long timeoutMillis = 3000;

    private int numRetries = 5; // default to 5 retries

    /* Caching role names to UUID mappings to save on Role-get API calls to
     * IAM server.
     *
     * Note: This is trivial caching with no update or expiration policy.
     * Its based on the assumption that the roles in the system are static and
     * do not change. If at any time that assumption changes we will need to
     * with a more sophisticated Cache.
     */
    private Map<String, UUID> roleCache = new HashMap<>();

    /**
     * Create a new IAM client.
     *
     * @param callerId The name of the app (caller) who is instanciating the IamClient. e.g. Vehicle-API
     */
    public IamClient(ChannelBuilder builder, String callerId) {
        this.builder = builder;
        this.retryingStub = new RetryingStub<>(builder, (mc) -> getBlockingStubWithCallerHeader(callerId, mc));
    }

    static IamServiceBlockingStub getBlockingStubWithCallerHeader(String callerId,
                                                                  ManagedChannel mc) {
        IamServiceBlockingStub bs = IamServiceGrpc.newBlockingStub(mc);
        Metadata callerIdHeader = new Metadata();
        callerIdHeader.put(
                Metadata.Key.of(CALLER_ID_KEY, Metadata.ASCII_STRING_MARSHALLER), callerId);
        MetadataUtils.attachHeaders(bs, callerIdHeader);
        return bs;
    }

    /**
     * Create a new IAM client.
     *
     * @param callerId The name of the app (caller) who is instanciating the IamClient. e.g. Vehicle-API
     */
    public static IamClient create(ChannelBuilder builder, String callerId) {
        return new IamClient(builder, callerId);
    }

    public IamClient setNumRetries(int numRetries) {
        this.numRetries = numRetries;
        return this;
    }

    public IamClient setTimeoutMillis(long millis) {
        this.timeoutMillis = millis;
        return this;
    }

    public void shutdown() throws InterruptedException {
        retryingStub.shutdown();
    }

    /**
     * Get authorization for a particular subject to perform a particular action on a particular
     * object.
     * @param request
     * @return An AuthorizationResponse
     */
    public AuthorizationResponse getAuthorization(AuthorizationRequest request) {
        GetAuthorizationResponse response = retryingStub.run(
            (stub) -> stub.getAuthorization(GetAuthorizationRequest.newBuilder()
                        .setAction(request.getAction().toString())
                        .setObjectAui(request.getObject().toString())
                        .setSubjectAui(request.getSubject().toString()).build()),
            timeoutMillis, numRetries);
        boolean isAuthorized = (response.getStatus() == AuthorizationStatus.ALLOW);
        return new AuthorizationResponse(isAuthorized, response.getStatementJson(), response.getRoleAui());
    }

    /**
     * Request Authorization for a single subject over multiple object/action pairs.
     * @param request Request
     * @return Batch response.
     */
    public BatchAuthorizationResponse getBatchAuthorization(BatchAuthorizationRequest request) {

        GetBatchAuthorizationRequest.Builder batchAuthBuilder = GetBatchAuthorizationRequest.newBuilder();

        batchAuthBuilder.setSubjectAui(request.getSubject().toString());

        for (ObjectAccessRequest oar : request.getObjectAccessRequests()) {
            com.autonomic.iam.proto.ObjectAccessRequest protoOar = com.autonomic.iam.proto.ObjectAccessRequest
                    .newBuilder()
                    .setAction(oar.getAction().toString())
                    .setObjectAui(oar.getObject().toString())
                    .build();
            batchAuthBuilder.addObjectAccessRequests(protoOar);
        }

        List<ObjectAccessResponse> result = retryingStub.run(
                stub -> getBatchAuthorizationResult(stub, batchAuthBuilder),
                timeoutMillis,
                numRetries);

        return new BatchAuthorizationResponse(request.getSubject(), result);
    }

    private List<ObjectAccessResponse> getBatchAuthorizationResult(
            IamServiceBlockingStub stub,
            GetBatchAuthorizationRequest.Builder batchAuthBuilder) {

        List<ObjectAccessResponse> result = new ArrayList<>();
        Iterator<GetBatchAuthorizationResponse> responseBatches =
            stub.getBatchAuthorization(batchAuthBuilder.build());

        while (responseBatches.hasNext()) {
            GetBatchAuthorizationResponse responseBatch = responseBatches.next();

            for (com.autonomic.iam.proto.ObjectAccessResponse oar : responseBatch.getResponsesList()) {
                final GetAuthorizationResponse response = oar.getResponse();

                AuthorizationResponse authResponse =
                    new AuthorizationResponse(
                            response.getStatus() == AuthorizationStatus.ALLOW,
                            response.getStatementJson(),
                            response.getRoleAui());

                ObjectAccessRequest originalRequest =
                    new ObjectAccessRequest(
                            Resource.fromString(oar.getOriginalRequest().getObjectAui()),
                            Action.fromString(oar.getOriginalRequest().getAction()));

                ObjectAccessResponse objectAccessResponse =
                    new ObjectAccessResponse(originalRequest, authResponse);

                result.add(objectAccessResponse);
            }
        }
        return result;
    }

    /**
     * Return true if IAM contains the triple: (subjectAui, objectAui, roleAui).
     *
     * @param subjectAui A subject Aui.
     * @param objectAui An object Aui.
     * @param roleAui A role Aui.
     * @return true if IAM contains the triple: (subjectAui, objectAui, roleAui)
     */
    public boolean permissionExists(String subjectAui, String objectAui, String roleAui) {
        Optional<PermissionModel> permission = searchBySubjectAuiAndObjectAuiAndRoleAui(subjectAui,
            objectAui, roleAui);
        return permission.isPresent();
    }

    /**
     * Return PermissionModel for the triple: (subjectAui, objectAui, roleAui).
     *
     * @param subjectAui A subject Aui.
     * @param objectAui An object Aui.
     * @param roleAui A role Aui.
     * @return PermissionModel for (subjectAui, objectAui, roleAui)
     */
    public Optional<PermissionModel> searchBySubjectAuiAndObjectAuiAndRoleAui(
            String subjectAui, String objectAui, String roleAui) {
        Expression subjectAuiExp = Expressions.eq("subjectAui", subjectAui);
        Expression objectAuiExp = Expressions.eq("objectAui", objectAui);

        Resource role = Resource.fromString(roleAui);
        String[] names = role.getName();
        String roleName = String.join("/", names);
        Expression roleJoinExp = Expressions.join("role",
            Expressions.and(Expressions.eq("name", roleName)));

        Expression andExp = Expressions.and(subjectAuiExp, objectAuiExp, roleJoinExp);
        SearchPermissionsResponse response = retryingStub.run(
            (stub) -> stub.searchPermissions(SearchPermissionsRequest
                .newBuilder()
                .setExpression(andExp)
                .build()),
            timeoutMillis, numRetries);
        if (response.getPermissionsCount() == 0) {
            return Optional.empty();
        } else {
            checkState(response.getPermissionsCount() == 1,
                "have more than one row for ({}, {}, {})", subjectAui, objectAui, roleAui);
            return Optional.of(IamClient.permissionProtoToPermissionPojo(response.getPermissions(0)));
        }
    }

    /**
     * Search for permissions with a given subject and object which are active
     * at the given instant. The search will use default search parameters.
     *
     * @param subjectAui A subject aui. May end in '*' to trigger a wildcard query.
     * @param objectAui An object aui. May end in '*' to trigger a wildcard query.
     * @return A search response containing permissions and optional next page token
     * @throws BadArgumentException if the request argument(s) were invalid
     */
    public SearchResponse<PermissionModel> searchBySubjectAuiAndObjectAui(
            String subjectAui,
            String objectAui) throws BadArgumentException {
        return searchBySubjectAuiAndObjectAui(subjectAui, objectAui, SearchParams.getDefaultParams());
    }

    /**
     * Search for permissions with a given subject and object which are active at the given instant.
     * Aui's may end in wildcard which will match anything with a given prefix.
     *
     * e.g. searchBySubjectAuiAndObjectAui(
     *            "aui:iam:user/XXX",
     *            "aui:asset:vehicle/*");
     *
     * Will match any of user XXX's permissions against vehicles.
     *
     * @param subjectAui A subject aui. May end in '*' to trigger a wildcard query.
     * @param objectAui An object aui. May end in '*' to trigger a wildcard query.
     * @param params A token from the previous search request and pageSize of it.
     * @return A search response containing permissions and optional next page token
     * @throws BadArgumentException if the request argument(s) were invalid
     */
    public SearchResponse<PermissionModel> searchBySubjectAuiAndObjectAui(
            String subjectAui,
            String objectAui,
            SearchParams params)
            throws BadArgumentException {
        Expression subjectAuiExp = convertWildcardExpression(subjectAui, "subjectAui");
        Expression objectAuiExp = convertWildcardExpression(objectAui, "objectAui");
        Expression andExp = Expressions.and(subjectAuiExp, objectAuiExp);

        SearchPermissionsResponse response;
        try {
            response = retryingStub.run(
                    (stub) -> stub.searchPermissions(SearchPermissionsRequest
                            .newBuilder()
                            .setExpression(andExp)
                            .setPageSize(params.pageSize)
                            .setPageToken(params.pageToken)
                            .build()),
                    timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
                throw new BadArgumentException(e);
            }

            throw e;
        }

        List<PermissionModel> permissionModels = response.getPermissionsList().stream()
                .map(IamClient::permissionProtoToPermissionPojo)
                .collect(Collectors.toList());
        return new SearchResponse<>(permissionModels, response.getNextPageToken());
    }

    /**
     * Get the permissions of a particular object.
     *
     * @param objectAui An object aui
     * @param params A token from the previous search request and pageSize of it.
     * @return A search response containing permissions and optional next page token
     * @throws BadArgumentException if the request argument(s) were invalid
     */
    public SearchResponse<PermissionModel> searchByObjectAui(String objectAui, SearchParams params)
            throws BadArgumentException {
        Expression objectAuiExp = Expressions.eq("objectAui", objectAui);

        SearchPermissionsResponse response;
        try {
            response = retryingStub.run(
                    (stub) -> stub.searchPermissions(SearchPermissionsRequest
                            .newBuilder()
                            .setExpression(objectAuiExp)
                            .setPageSize(params.pageSize)
                            .setPageToken(params.pageToken)
                            .build()),
                    timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
                throw new BadArgumentException(e);
            }

            throw e;
        }

        List<PermissionModel> permissionModels = response.getPermissionsList().stream()
                .map(IamClient::permissionProtoToPermissionPojo)
                .collect(Collectors.toList());
        return new SearchResponse<>(permissionModels, response.getNextPageToken());
    }

    /**
     * Get all the permissions that match the given subject and roles
     *
     * @param objectAui Match permissions on this objectAui and
     * @param roleNames A var-arg list of roles to match on
     * @return A search response containing permissions and optional next page token
     * @throws BadArgumentException if the request argument(s) were invalid
     */
    public SearchResponse<PermissionModel> searchByObjectAuiAndRoleAui(
            String objectAui,
            String... roleNames)
            throws BadArgumentException {

        /* Check search arguments */
        if (objectAui == null) {
            throw new BadArgumentException("No object AUI specified");
        }
        if (roleNames.length < 1) {
            throw new BadArgumentException("No roles AUI(s) specified");
        }

        ArrayList<Expression> orPredicates = new ArrayList<>();
        for (String roleName : roleNames) {
            UUID roleId;
            if (roleCache.containsKey(roleName)) {
                /* Fetch from cache */
                roleId = roleCache.get(roleName);
            } else {
                /* Call the IAM service to get role information */
                GetRoleResponse response = tryGetRoleResponse(roleName);
                roleId = UUID.fromString(response.getRoleId());
                /* Update cache */
                roleCache.put(roleName, roleId);
            }
            orPredicates.add(Expressions.eq("roleId", roleId));
        }

        Expression.Builder andPredicate = Expression.newBuilder();
        Junct.Builder junct = Junct.newBuilder().setType(Junct.JunctType.OR).addAllExpressions(orPredicates);
        andPredicate.setJunct(junct.build());
        Expression objectAuiExp = Expressions.eq("objectAui", objectAui);
        Expression queryExp = Expressions.and(objectAuiExp, andPredicate.build());

        SearchPermissionsResponse response;
        try {
            response = retryingStub.run(
                                (stub) -> stub.searchPermissions(SearchPermissionsRequest
                                        .newBuilder()
                                        .setExpression(queryExp)
                                        .build()),
                                timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
                throw new BadArgumentException(e);
            }
            throw e;
        }

        List<PermissionModel> permissionModels = response.getPermissionsList().stream()
                .map(IamClient::permissionProtoToPermissionPojo)
                .collect(Collectors.toList());
        return new SearchResponse<>(permissionModels, response.getNextPageToken());
    }

    /**
     * Get all the permissions that match the given subject and roles
     *
     * @param objectAui Match permissions on this objectAui and
     * @param roleNames A list of roles to match on
     * @return A search response containing permissions and optional next page token
     * @throws BadArgumentException if the request argument(s) were invalid
     */
    public SearchResponse<PermissionModel> searchByObjectAuiAndRoleAui(
            String objectAui,
            List<String> roleNames)
            throws BadArgumentException {
        return searchByObjectAuiAndRoleAui(objectAui, roleNames.toArray(new String[roleNames.size()]));
    }

    /**
     * @param objectAui The Aui of the object which the permission is formed.
     * @param permissionId The id of the permission
     * @return null if the permission was not found. Otherwise, the permission
     * @throws BadArgumentException if the request argument(s) were invalid
     */
    public PermissionModel getByObjectAuiAndId(String objectAui, Long permissionId)
            throws BadArgumentException {
        Expression objectAuiExp = Expressions.eq("objectAui", objectAui);
        Expression idExp = Expressions.eq("id", permissionId);
        Expression andExp = Expressions.and(objectAuiExp, idExp);

        SearchPermissionsResponse response;
        try {
            response = retryingStub.run(
                    (stub) -> stub.searchPermissions(SearchPermissionsRequest
                            .newBuilder()
                            .setExpression(andExp)
                            .setPageSize(1)
                            .build()),
                    timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
                throw new BadArgumentException(e);
            }

            throw e;
        }

        List<PermissionModel> permissionModels = response.getPermissionsList().stream()
                .map(IamClient::permissionProtoToPermissionPojo)
                .collect(Collectors.toList());

        if (permissionModels.size() == 1) {
            return permissionModels.get(0);
        }
        return null;
    }

    /**
     * Delete a permission by DeletePermissionRequest which includes the id we need to use it to
     * delete the associated permission.
     *
     * @param builder newBuilder for delete request.
     * @throws FailedPreconditionException if the precondition specified in the delete request was not met
     */
    public void deletePermission(DeletePermissionRequestBuilder builder) throws FailedPreconditionException {
        try {
            retryingStub.run((stub) -> stub.deletePermission(builder.build()), timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.FAILED_PRECONDITION) {
                throw new FailedPreconditionException(e);
            }

            throw e;
        }
    }

    /**
     * Create a permission if it does not already exist. If a permission already exists but it has
     * a different role than the requested role, the existing permission is deleted and the new
     * permission is instantiated.
     * @param subjectAui
     * @param objectAui
     * @param roleAui
     * @param requestingSubjectAui
     * @return id of the permission which was updated or created.
     * @throws NotFoundException if the role definition was not found
     * @throws FailedPreconditionException if the role definition was invalid
     * @throws BadArgumentException if the role or subject Aui was invalid
     *  role or subject Aui was invalid
     */
    public long upsertPermission(String subjectAui,
                                 String objectAui,
                                 String roleAui,
                                 String requestingSubjectAui)
            throws NotFoundException, FailedPreconditionException, BadArgumentException {
        try {
            UpsertPermissionResponse response = retryingStub.run((stub) -> stub.upsertPermission(
                    UpsertPermissionRequest.newBuilder()
                            .setSubjectAui(subjectAui)
                            .setObjectAui(objectAui)
                            .setRoleAui(roleAui)
                            .setRequestingSubjectAui(requestingSubjectAui)
                            .build()), timeoutMillis, numRetries);
            return response.getId();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                throw new NotFoundException("Role definition not found", e);
            } else if (e.getStatus().getCode() == Code.FAILED_PRECONDITION) {
                throw new FailedPreconditionException(e);
            } else if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
                throw new BadArgumentException(e);
            }

            throw e;
        }
    }

    /**
     * Creates a new permission. There cannot be a pre-existing role between the subject/object.
     * Assumes the start time is now and the permission time is unbounded.
     * @param subjectAui
     * @param objectAui
     * @param roleAui
     * @return Id of the permission which was created.
     * @throws NotFoundException if the role definition was not found
     * @throws FailedPreconditionException if the role definition was invalid
     * @throws BadArgumentException if the role or subject Aui was invalid
     *  role or subject Aui was invalid
     */
    public long createPermission(String subjectAui,
                                 String objectAui,
                                 String roleAui,
                                 String requestingSubjectAui)
            throws NotFoundException, FailedPreconditionException, BadArgumentException {
        try {
            return retryingStub.run(
                    (stub) -> stub.createPermission(
                            CreatePermissionRequest.newBuilder()
                                    .setSubjectAui(subjectAui)
                                    .setObjectAui(objectAui)
                                    .setRoleAui(roleAui)
                                    .setRequestingSubjectAui(requestingSubjectAui)
                                    .build()),
                    timeoutMillis, numRetries).getId();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                throw new NotFoundException("Role definition not found", e);
            } else if (e.getStatus().getCode() == Code.FAILED_PRECONDITION) {
                throw new FailedPreconditionException(e);
            } else if (e.getStatus().getCode() == Code.INVALID_ARGUMENT) {
                throw new BadArgumentException(e);
            }

            throw e;
        }
    }

    /**
     * Creates a new IAM user
     *
     * @param name name of the user
     * @param email email address of the user
     * @throws AlreadyExistsException if the user already exists in IAM
     */
    public void createUser(String name, String email)
            throws AlreadyExistsException {
        try {
            retryingStub.run(
                    (stub) -> stub.createUser(CreateUserRequest.newBuilder()
                            .setName(name)
                            .setEmail(email)
                            .build()),
                    timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.ALREADY_EXISTS) {
                throw new AlreadyExistsException(
                            "User " + name + "already exists", e.getCause());
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete an IAM user
     *
     * @param name name of the user to delete
     * @throws NotFoundException if the user was not found in IAM
     */
    public void deleteUser(String name) throws NotFoundException {
        try {
            retryingStub.run(
                    (stub) -> stub.deleteUser(DeleteUserRequest.newBuilder()
                            .setName(name)
                            .build()),
                    timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Code.NOT_FOUND) {
                throw new NotFoundException("User " + name + " not found", e.getCause());
            } else {
                throw e;
            }
        }
    }

    /**
     * Return Role with Id for a given role name.
     *
     * If the server returns corrupted data and the role cannot be parsed, MalformedDataException
     * will be thrown.
     *
     * @param roleName Name for the role.
     * @return Role for a given role name. If the role does not exist, an empty optional will be
     * returned.
     */
    public Optional<Role> getRole(String roleName) {
        GetRoleResponse response = tryGetRoleResponse(roleName);
        if (response == null) {
            return Optional.empty();
        }

        String id = response.getRoleId();
        String json = response.getRoleDefinition();

        try {
            UUID uuid = UUID.fromString(id);
            RoleDefinition roleDefinition = serde.readValue(json, RoleDefinition.class);
            return Optional.of(new Role(uuid, roleDefinition));
        } catch (IOException | IllegalArgumentException exp) {
            throw new MalformedDataException(
                    "error in parsing role '" + id + "' with json: '" + json + "'", exp);
        }
    }

    /**
     * @return null if the role was not found.
     */
    protected GetRoleResponse tryGetRoleResponse(String roleName) {
        try {
            return retryingStub.run((stub) -> stub.getRole(
                    GetRoleRequest.newBuilder()
                            .setName(roleName)
                            .build()), timeoutMillis, numRetries);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                return null;
            } else {
                throw e;
            }
        }
    }

    protected static List<PermissionModel> permissionsToPojo(List<Permission> permissions) {
        return permissions
                .stream()
                .map(IamClient::permissionProtoToPermissionPojo)
                .collect(Collectors.toList());
    }

    protected static Timestamp toTimestamp(Date date) {
        if (date == null) {
            return Timestamp.getDefaultInstance();
        }
        return Timestamp.newBuilder()
                .setSeconds(TimeUnit.MILLISECONDS.toSeconds(date.getTime()))
                .setNanos((int) TimeUnit.MILLISECONDS.toNanos(date.getTime() % 1000))
                .build();
    }

    protected static Date toDate(Timestamp timestamp) {
        if (timestamp == Timestamp.getDefaultInstance()) {
            return null;
        }
        return Date.from(Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()));
    }

    /**
     * To support wild card feature in subject and object
     * @param value the value of subject or object
     * @param field the name of field, could be either "subjectAui" or "objectAui"
     * @return Expression of eq or like depending on if the input uses a wildcard
     */
    protected static Expression convertWildcardExpression(String value, String field) {
        if (value != null && value.endsWith("*")) {
            return Expressions.like(field, value.substring(0, value.length() - 1) + "%");
        } else if (value != null) {
            return Expressions.eq(field, value);
        } else {
            return null;
        }
    }

    protected static PermissionModel permissionProtoToPermissionPojo(Permission i) {
        return new PermissionModel(toDate(i.getCreateTime()),
                                   toDate(i.getUpdateTime()),
                                   i.getId(),
                                   i.getSubjectAui(),
                                   i.getObjectAui(),
                                   i.getRoleAui());
    }

}
