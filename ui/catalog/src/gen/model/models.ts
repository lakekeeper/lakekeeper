import localVarRequest from 'request';

export * from './authZBackend';
export * from './azCredential';
export * from './azCredentialClientCredentials';
export * from './azdlsProfile';
export * from './bootstrapRequest';
export * from './createProjectRequest';
export * from './createProjectResponse';
export * from './createRoleRequest';
export * from './createUserRequest';
export * from './createWarehouseRequest';
export * from './createWarehouseResponse';
export * from './deleteKind';
export * from './deletedTabularResponse';
export * from './gcsCredential';
export * from './gcsCredentialServiceAccountKey';
export * from './gcsProfile';
export * from './gcsServiceKey';
export * from './getNamespaceAccessResponse';
export * from './getNamespaceAssignmentsResponse';
export * from './getNamespaceResponse';
export * from './getProjectAccessResponse';
export * from './getProjectAssignmentsResponse';
export * from './getProjectResponse';
export * from './getRoleAccessResponse';
export * from './getRoleAssignmentsResponse';
export * from './getServerAccessResponse';
export * from './getServerAssignmentsResponse';
export * from './getTableAccessResponse';
export * from './getTableAssignmentsResponse';
export * from './getViewAccessResponse';
export * from './getViewAssignmentsResponse';
export * from './getWarehouseAccessResponse';
export * from './getWarehouseAssignmentsResponse';
export * from './getWarehouseResponse';
export * from './listDeletedTabularsResponse';
export * from './listProjectsResponse';
export * from './listRolesResponse';
export * from './listUsersResponse';
export * from './listWarehousesRequest';
export * from './listWarehousesResponse';
export * from './namespaceAction';
export * from './namespaceAssignment';
export * from './namespaceAssignmentCreate';
export * from './namespaceAssignmentDescribe';
export * from './namespaceAssignmentManageGrants';
export * from './namespaceAssignmentModify';
export * from './namespaceAssignmentOwnership';
export * from './namespaceAssignmentPassGrants';
export * from './namespaceAssignmentSelect';
export * from './namespaceRelation';
export * from './projectAction';
export * from './projectAssignment';
export * from './projectAssignmentCreate';
export * from './projectAssignmentDescribe';
export * from './projectAssignmentModify';
export * from './projectAssignmentProjectAdmin';
export * from './projectAssignmentRoleCreator';
export * from './projectAssignmentSecurityAdmin';
export * from './projectAssignmentSelect';
export * from './projectAssignmentWarehouseAdmin';
export * from './projectRelation';
export * from './renameProjectRequest';
export * from './renameWarehouseRequest';
export * from './role';
export * from './roleAction';
export * from './roleAssignment';
export * from './roleAssignmentAssignee';
export * from './roleAssignmentOwnership';
export * from './s3Credential';
export * from './s3CredentialAccessKey';
export * from './s3Flavor';
export * from './s3Profile';
export * from './searchRoleRequest';
export * from './searchRoleResponse';
export * from './searchUser';
export * from './searchUserRequest';
export * from './searchUserResponse';
export * from './serverAction';
export * from './serverAssignment';
export * from './serverAssignmentGlobalAdmin';
export * from './serverInfo';
export * from './serverRelation';
export * from './setManagedAccessRequest';
export * from './storageCredential';
export * from './storageCredentialAz';
export * from './storageCredentialGcs';
export * from './storageCredentialS3';
export * from './storageProfile';
export * from './storageProfileAzdls';
export * from './storageProfileGcs';
export * from './storageProfileS3';
export * from './tableAction';
export * from './tableAssignment';
export * from './tableAssignmentCreate';
export * from './tableAssignmentDescribe';
export * from './tableAssignmentManageGrants';
export * from './tableAssignmentOwnership';
export * from './tableAssignmentPassGrants';
export * from './tableAssignmentSelect';
export * from './tableRelation';
export * from './tabularDeleteProfile';
export * from './tabularDeleteProfileHard';
export * from './tabularDeleteProfileSoft';
export * from './tabularType';
export * from './updateNamespaceAssignmentsRequest';
export * from './updateProjectAssignmentsRequest';
export * from './updateRoleAssignmentsRequest';
export * from './updateRoleRequest';
export * from './updateServerAssignmentsRequest';
export * from './updateTableAssignmentsRequest';
export * from './updateUserRequest';
export * from './updateViewAssignmentsRequest';
export * from './updateWarehouseAssignmentsRequest';
export * from './updateWarehouseCredentialRequest';
export * from './updateWarehouseDeleteProfileRequest';
export * from './updateWarehouseStorageRequest';
export * from './user';
export * from './userLastUpdatedWith';
export * from './userOrRole';
export * from './userOrRoleRole';
export * from './userOrRoleUser';
export * from './userType';
export * from './viewAction';
export * from './viewAssignment';
export * from './viewAssignmentDescribe';
export * from './viewAssignmentManageGrants';
export * from './viewAssignmentModify';
export * from './viewAssignmentOwnership';
export * from './viewAssignmentPassGrants';
export * from './viewRelation';
export * from './warehouseAction';
export * from './warehouseAssignment';
export * from './warehouseAssignmentCreate';
export * from './warehouseAssignmentDescribe';
export * from './warehouseAssignmentManageGrants';
export * from './warehouseAssignmentModify';
export * from './warehouseAssignmentOwnership';
export * from './warehouseAssignmentPassGrants';
export * from './warehouseAssignmentSelect';
export * from './warehouseRelation';
export * from './warehouseStatus';

import * as fs from 'fs';

export interface RequestDetailedFile {
    value: Buffer;
    options?: {
        filename?: string;
        contentType?: string;
    }
}

export type RequestFile = string | Buffer | fs.ReadStream | RequestDetailedFile;


import { AuthZBackend } from './authZBackend';
import { AzCredential } from './azCredential';
import { AzCredentialClientCredentials } from './azCredentialClientCredentials';
import { AzdlsProfile } from './azdlsProfile';
import { BootstrapRequest } from './bootstrapRequest';
import { CreateProjectRequest } from './createProjectRequest';
import { CreateProjectResponse } from './createProjectResponse';
import { CreateRoleRequest } from './createRoleRequest';
import { CreateUserRequest } from './createUserRequest';
import { CreateWarehouseRequest } from './createWarehouseRequest';
import { CreateWarehouseResponse } from './createWarehouseResponse';
import { DeleteKind } from './deleteKind';
import { DeletedTabularResponse } from './deletedTabularResponse';
import { GcsCredential } from './gcsCredential';
import { GcsCredentialServiceAccountKey } from './gcsCredentialServiceAccountKey';
import { GcsProfile } from './gcsProfile';
import { GcsServiceKey } from './gcsServiceKey';
import { GetNamespaceAccessResponse } from './getNamespaceAccessResponse';
import { GetNamespaceAssignmentsResponse } from './getNamespaceAssignmentsResponse';
import { GetNamespaceResponse } from './getNamespaceResponse';
import { GetProjectAccessResponse } from './getProjectAccessResponse';
import { GetProjectAssignmentsResponse } from './getProjectAssignmentsResponse';
import { GetProjectResponse } from './getProjectResponse';
import { GetRoleAccessResponse } from './getRoleAccessResponse';
import { GetRoleAssignmentsResponse } from './getRoleAssignmentsResponse';
import { GetServerAccessResponse } from './getServerAccessResponse';
import { GetServerAssignmentsResponse } from './getServerAssignmentsResponse';
import { GetTableAccessResponse } from './getTableAccessResponse';
import { GetTableAssignmentsResponse } from './getTableAssignmentsResponse';
import { GetViewAccessResponse } from './getViewAccessResponse';
import { GetViewAssignmentsResponse } from './getViewAssignmentsResponse';
import { GetWarehouseAccessResponse } from './getWarehouseAccessResponse';
import { GetWarehouseAssignmentsResponse } from './getWarehouseAssignmentsResponse';
import { GetWarehouseResponse } from './getWarehouseResponse';
import { ListDeletedTabularsResponse } from './listDeletedTabularsResponse';
import { ListProjectsResponse } from './listProjectsResponse';
import { ListRolesResponse } from './listRolesResponse';
import { ListUsersResponse } from './listUsersResponse';
import { ListWarehousesRequest } from './listWarehousesRequest';
import { ListWarehousesResponse } from './listWarehousesResponse';
import { NamespaceAction } from './namespaceAction';
import { NamespaceAssignment } from './namespaceAssignment';
import { NamespaceAssignmentCreate } from './namespaceAssignmentCreate';
import { NamespaceAssignmentDescribe } from './namespaceAssignmentDescribe';
import { NamespaceAssignmentManageGrants } from './namespaceAssignmentManageGrants';
import { NamespaceAssignmentModify } from './namespaceAssignmentModify';
import { NamespaceAssignmentOwnership } from './namespaceAssignmentOwnership';
import { NamespaceAssignmentPassGrants } from './namespaceAssignmentPassGrants';
import { NamespaceAssignmentSelect } from './namespaceAssignmentSelect';
import { NamespaceRelation } from './namespaceRelation';
import { ProjectAction } from './projectAction';
import { ProjectAssignment } from './projectAssignment';
import { ProjectAssignmentCreate } from './projectAssignmentCreate';
import { ProjectAssignmentDescribe } from './projectAssignmentDescribe';
import { ProjectAssignmentModify } from './projectAssignmentModify';
import { ProjectAssignmentProjectAdmin } from './projectAssignmentProjectAdmin';
import { ProjectAssignmentRoleCreator } from './projectAssignmentRoleCreator';
import { ProjectAssignmentSecurityAdmin } from './projectAssignmentSecurityAdmin';
import { ProjectAssignmentSelect } from './projectAssignmentSelect';
import { ProjectAssignmentWarehouseAdmin } from './projectAssignmentWarehouseAdmin';
import { ProjectRelation } from './projectRelation';
import { RenameProjectRequest } from './renameProjectRequest';
import { RenameWarehouseRequest } from './renameWarehouseRequest';
import { Role } from './role';
import { RoleAction } from './roleAction';
import { RoleAssignment } from './roleAssignment';
import { RoleAssignmentAssignee } from './roleAssignmentAssignee';
import { RoleAssignmentOwnership } from './roleAssignmentOwnership';
import { S3Credential } from './s3Credential';
import { S3CredentialAccessKey } from './s3CredentialAccessKey';
import { S3Flavor } from './s3Flavor';
import { S3Profile } from './s3Profile';
import { SearchRoleRequest } from './searchRoleRequest';
import { SearchRoleResponse } from './searchRoleResponse';
import { SearchUser } from './searchUser';
import { SearchUserRequest } from './searchUserRequest';
import { SearchUserResponse } from './searchUserResponse';
import { ServerAction } from './serverAction';
import { ServerAssignment } from './serverAssignment';
import { ServerAssignmentGlobalAdmin } from './serverAssignmentGlobalAdmin';
import { ServerInfo } from './serverInfo';
import { ServerRelation } from './serverRelation';
import { SetManagedAccessRequest } from './setManagedAccessRequest';
import { StorageCredential } from './storageCredential';
import { StorageCredentialAz } from './storageCredentialAz';
import { StorageCredentialGcs } from './storageCredentialGcs';
import { StorageCredentialS3 } from './storageCredentialS3';
import { StorageProfile } from './storageProfile';
import { StorageProfileAzdls } from './storageProfileAzdls';
import { StorageProfileGcs } from './storageProfileGcs';
import { StorageProfileS3 } from './storageProfileS3';
import { TableAction } from './tableAction';
import { TableAssignment } from './tableAssignment';
import { TableAssignmentCreate } from './tableAssignmentCreate';
import { TableAssignmentDescribe } from './tableAssignmentDescribe';
import { TableAssignmentManageGrants } from './tableAssignmentManageGrants';
import { TableAssignmentOwnership } from './tableAssignmentOwnership';
import { TableAssignmentPassGrants } from './tableAssignmentPassGrants';
import { TableAssignmentSelect } from './tableAssignmentSelect';
import { TableRelation } from './tableRelation';
import { TabularDeleteProfile } from './tabularDeleteProfile';
import { TabularDeleteProfileHard } from './tabularDeleteProfileHard';
import { TabularDeleteProfileSoft } from './tabularDeleteProfileSoft';
import { TabularType } from './tabularType';
import { UpdateNamespaceAssignmentsRequest } from './updateNamespaceAssignmentsRequest';
import { UpdateProjectAssignmentsRequest } from './updateProjectAssignmentsRequest';
import { UpdateRoleAssignmentsRequest } from './updateRoleAssignmentsRequest';
import { UpdateRoleRequest } from './updateRoleRequest';
import { UpdateServerAssignmentsRequest } from './updateServerAssignmentsRequest';
import { UpdateTableAssignmentsRequest } from './updateTableAssignmentsRequest';
import { UpdateUserRequest } from './updateUserRequest';
import { UpdateViewAssignmentsRequest } from './updateViewAssignmentsRequest';
import { UpdateWarehouseAssignmentsRequest } from './updateWarehouseAssignmentsRequest';
import { UpdateWarehouseCredentialRequest } from './updateWarehouseCredentialRequest';
import { UpdateWarehouseDeleteProfileRequest } from './updateWarehouseDeleteProfileRequest';
import { UpdateWarehouseStorageRequest } from './updateWarehouseStorageRequest';
import { User } from './user';
import { UserLastUpdatedWith } from './userLastUpdatedWith';
import { UserOrRole } from './userOrRole';
import { UserOrRoleRole } from './userOrRoleRole';
import { UserOrRoleUser } from './userOrRoleUser';
import { UserType } from './userType';
import { ViewAction } from './viewAction';
import { ViewAssignment } from './viewAssignment';
import { ViewAssignmentDescribe } from './viewAssignmentDescribe';
import { ViewAssignmentManageGrants } from './viewAssignmentManageGrants';
import { ViewAssignmentModify } from './viewAssignmentModify';
import { ViewAssignmentOwnership } from './viewAssignmentOwnership';
import { ViewAssignmentPassGrants } from './viewAssignmentPassGrants';
import { ViewRelation } from './viewRelation';
import { WarehouseAction } from './warehouseAction';
import { WarehouseAssignment } from './warehouseAssignment';
import { WarehouseAssignmentCreate } from './warehouseAssignmentCreate';
import { WarehouseAssignmentDescribe } from './warehouseAssignmentDescribe';
import { WarehouseAssignmentManageGrants } from './warehouseAssignmentManageGrants';
import { WarehouseAssignmentModify } from './warehouseAssignmentModify';
import { WarehouseAssignmentOwnership } from './warehouseAssignmentOwnership';
import { WarehouseAssignmentPassGrants } from './warehouseAssignmentPassGrants';
import { WarehouseAssignmentSelect } from './warehouseAssignmentSelect';
import { WarehouseRelation } from './warehouseRelation';
import { WarehouseStatus } from './warehouseStatus';

/* tslint:disable:no-unused-variable */
let primitives = [
                    "string",
                    "boolean",
                    "double",
                    "integer",
                    "long",
                    "float",
                    "number",
                    "any"
                 ];

let enumsMap: {[index: string]: any} = {
        "AuthZBackend": AuthZBackend,
        "AzCredential.CredentialTypeEnum": AzCredential.CredentialTypeEnum,
        "AzCredentialClientCredentials.CredentialTypeEnum": AzCredentialClientCredentials.CredentialTypeEnum,
        "DeleteKind": DeleteKind,
        "GcsCredential.CredentialTypeEnum": GcsCredential.CredentialTypeEnum,
        "GcsCredentialServiceAccountKey.CredentialTypeEnum": GcsCredentialServiceAccountKey.CredentialTypeEnum,
        "NamespaceAction": NamespaceAction,
        "NamespaceAssignment.TypeEnum": NamespaceAssignment.TypeEnum,
        "NamespaceAssignmentCreate.TypeEnum": NamespaceAssignmentCreate.TypeEnum,
        "NamespaceAssignmentDescribe.TypeEnum": NamespaceAssignmentDescribe.TypeEnum,
        "NamespaceAssignmentManageGrants.TypeEnum": NamespaceAssignmentManageGrants.TypeEnum,
        "NamespaceAssignmentModify.TypeEnum": NamespaceAssignmentModify.TypeEnum,
        "NamespaceAssignmentOwnership.TypeEnum": NamespaceAssignmentOwnership.TypeEnum,
        "NamespaceAssignmentPassGrants.TypeEnum": NamespaceAssignmentPassGrants.TypeEnum,
        "NamespaceAssignmentSelect.TypeEnum": NamespaceAssignmentSelect.TypeEnum,
        "NamespaceRelation": NamespaceRelation,
        "ProjectAction": ProjectAction,
        "ProjectAssignment.TypeEnum": ProjectAssignment.TypeEnum,
        "ProjectAssignmentCreate.TypeEnum": ProjectAssignmentCreate.TypeEnum,
        "ProjectAssignmentDescribe.TypeEnum": ProjectAssignmentDescribe.TypeEnum,
        "ProjectAssignmentModify.TypeEnum": ProjectAssignmentModify.TypeEnum,
        "ProjectAssignmentProjectAdmin.TypeEnum": ProjectAssignmentProjectAdmin.TypeEnum,
        "ProjectAssignmentRoleCreator.TypeEnum": ProjectAssignmentRoleCreator.TypeEnum,
        "ProjectAssignmentSecurityAdmin.TypeEnum": ProjectAssignmentSecurityAdmin.TypeEnum,
        "ProjectAssignmentSelect.TypeEnum": ProjectAssignmentSelect.TypeEnum,
        "ProjectAssignmentWarehouseAdmin.TypeEnum": ProjectAssignmentWarehouseAdmin.TypeEnum,
        "ProjectRelation": ProjectRelation,
        "RoleAction": RoleAction,
        "RoleAssignment.TypeEnum": RoleAssignment.TypeEnum,
        "RoleAssignmentAssignee.TypeEnum": RoleAssignmentAssignee.TypeEnum,
        "RoleAssignmentOwnership.TypeEnum": RoleAssignmentOwnership.TypeEnum,
        "S3Credential.CredentialTypeEnum": S3Credential.CredentialTypeEnum,
        "S3CredentialAccessKey.CredentialTypeEnum": S3CredentialAccessKey.CredentialTypeEnum,
        "S3Flavor": S3Flavor,
        "ServerAction": ServerAction,
        "ServerAssignment.TypeEnum": ServerAssignment.TypeEnum,
        "ServerAssignmentGlobalAdmin.TypeEnum": ServerAssignmentGlobalAdmin.TypeEnum,
        "ServerRelation": ServerRelation,
        "StorageCredential.CredentialTypeEnum": StorageCredential.CredentialTypeEnum,
        "StorageCredential.TypeEnum": StorageCredential.TypeEnum,
        "StorageCredentialAz.TypeEnum": StorageCredentialAz.TypeEnum,
        "StorageCredentialGcs.TypeEnum": StorageCredentialGcs.TypeEnum,
        "StorageCredentialS3.TypeEnum": StorageCredentialS3.TypeEnum,
        "StorageProfile.TypeEnum": StorageProfile.TypeEnum,
        "StorageProfileAzdls.TypeEnum": StorageProfileAzdls.TypeEnum,
        "StorageProfileGcs.TypeEnum": StorageProfileGcs.TypeEnum,
        "StorageProfileS3.TypeEnum": StorageProfileS3.TypeEnum,
        "TableAction": TableAction,
        "TableAssignment.TypeEnum": TableAssignment.TypeEnum,
        "TableAssignmentCreate.TypeEnum": TableAssignmentCreate.TypeEnum,
        "TableAssignmentDescribe.TypeEnum": TableAssignmentDescribe.TypeEnum,
        "TableAssignmentManageGrants.TypeEnum": TableAssignmentManageGrants.TypeEnum,
        "TableAssignmentOwnership.TypeEnum": TableAssignmentOwnership.TypeEnum,
        "TableAssignmentPassGrants.TypeEnum": TableAssignmentPassGrants.TypeEnum,
        "TableAssignmentSelect.TypeEnum": TableAssignmentSelect.TypeEnum,
        "TableRelation": TableRelation,
        "TabularDeleteProfile.TypeEnum": TabularDeleteProfile.TypeEnum,
        "TabularDeleteProfileHard.TypeEnum": TabularDeleteProfileHard.TypeEnum,
        "TabularDeleteProfileSoft.TypeEnum": TabularDeleteProfileSoft.TypeEnum,
        "TabularType": TabularType,
        "UserLastUpdatedWith": UserLastUpdatedWith,
        "UserType": UserType,
        "ViewAction": ViewAction,
        "ViewAssignment.TypeEnum": ViewAssignment.TypeEnum,
        "ViewAssignmentDescribe.TypeEnum": ViewAssignmentDescribe.TypeEnum,
        "ViewAssignmentManageGrants.TypeEnum": ViewAssignmentManageGrants.TypeEnum,
        "ViewAssignmentModify.TypeEnum": ViewAssignmentModify.TypeEnum,
        "ViewAssignmentOwnership.TypeEnum": ViewAssignmentOwnership.TypeEnum,
        "ViewAssignmentPassGrants.TypeEnum": ViewAssignmentPassGrants.TypeEnum,
        "ViewRelation": ViewRelation,
        "WarehouseAction": WarehouseAction,
        "WarehouseAssignment.TypeEnum": WarehouseAssignment.TypeEnum,
        "WarehouseAssignmentCreate.TypeEnum": WarehouseAssignmentCreate.TypeEnum,
        "WarehouseAssignmentDescribe.TypeEnum": WarehouseAssignmentDescribe.TypeEnum,
        "WarehouseAssignmentManageGrants.TypeEnum": WarehouseAssignmentManageGrants.TypeEnum,
        "WarehouseAssignmentModify.TypeEnum": WarehouseAssignmentModify.TypeEnum,
        "WarehouseAssignmentOwnership.TypeEnum": WarehouseAssignmentOwnership.TypeEnum,
        "WarehouseAssignmentPassGrants.TypeEnum": WarehouseAssignmentPassGrants.TypeEnum,
        "WarehouseAssignmentSelect.TypeEnum": WarehouseAssignmentSelect.TypeEnum,
        "WarehouseRelation": WarehouseRelation,
        "WarehouseStatus": WarehouseStatus,
}

let typeMap: {[index: string]: any} = {
    "AzCredential": AzCredential,
    "AzCredentialClientCredentials": AzCredentialClientCredentials,
    "AzdlsProfile": AzdlsProfile,
    "BootstrapRequest": BootstrapRequest,
    "CreateProjectRequest": CreateProjectRequest,
    "CreateProjectResponse": CreateProjectResponse,
    "CreateRoleRequest": CreateRoleRequest,
    "CreateUserRequest": CreateUserRequest,
    "CreateWarehouseRequest": CreateWarehouseRequest,
    "CreateWarehouseResponse": CreateWarehouseResponse,
    "DeletedTabularResponse": DeletedTabularResponse,
    "GcsCredential": GcsCredential,
    "GcsCredentialServiceAccountKey": GcsCredentialServiceAccountKey,
    "GcsProfile": GcsProfile,
    "GcsServiceKey": GcsServiceKey,
    "GetNamespaceAccessResponse": GetNamespaceAccessResponse,
    "GetNamespaceAssignmentsResponse": GetNamespaceAssignmentsResponse,
    "GetNamespaceResponse": GetNamespaceResponse,
    "GetProjectAccessResponse": GetProjectAccessResponse,
    "GetProjectAssignmentsResponse": GetProjectAssignmentsResponse,
    "GetProjectResponse": GetProjectResponse,
    "GetRoleAccessResponse": GetRoleAccessResponse,
    "GetRoleAssignmentsResponse": GetRoleAssignmentsResponse,
    "GetServerAccessResponse": GetServerAccessResponse,
    "GetServerAssignmentsResponse": GetServerAssignmentsResponse,
    "GetTableAccessResponse": GetTableAccessResponse,
    "GetTableAssignmentsResponse": GetTableAssignmentsResponse,
    "GetViewAccessResponse": GetViewAccessResponse,
    "GetViewAssignmentsResponse": GetViewAssignmentsResponse,
    "GetWarehouseAccessResponse": GetWarehouseAccessResponse,
    "GetWarehouseAssignmentsResponse": GetWarehouseAssignmentsResponse,
    "GetWarehouseResponse": GetWarehouseResponse,
    "ListDeletedTabularsResponse": ListDeletedTabularsResponse,
    "ListProjectsResponse": ListProjectsResponse,
    "ListRolesResponse": ListRolesResponse,
    "ListUsersResponse": ListUsersResponse,
    "ListWarehousesRequest": ListWarehousesRequest,
    "ListWarehousesResponse": ListWarehousesResponse,
    "NamespaceAssignment": NamespaceAssignment,
    "NamespaceAssignmentCreate": NamespaceAssignmentCreate,
    "NamespaceAssignmentDescribe": NamespaceAssignmentDescribe,
    "NamespaceAssignmentManageGrants": NamespaceAssignmentManageGrants,
    "NamespaceAssignmentModify": NamespaceAssignmentModify,
    "NamespaceAssignmentOwnership": NamespaceAssignmentOwnership,
    "NamespaceAssignmentPassGrants": NamespaceAssignmentPassGrants,
    "NamespaceAssignmentSelect": NamespaceAssignmentSelect,
    "ProjectAssignment": ProjectAssignment,
    "ProjectAssignmentCreate": ProjectAssignmentCreate,
    "ProjectAssignmentDescribe": ProjectAssignmentDescribe,
    "ProjectAssignmentModify": ProjectAssignmentModify,
    "ProjectAssignmentProjectAdmin": ProjectAssignmentProjectAdmin,
    "ProjectAssignmentRoleCreator": ProjectAssignmentRoleCreator,
    "ProjectAssignmentSecurityAdmin": ProjectAssignmentSecurityAdmin,
    "ProjectAssignmentSelect": ProjectAssignmentSelect,
    "ProjectAssignmentWarehouseAdmin": ProjectAssignmentWarehouseAdmin,
    "RenameProjectRequest": RenameProjectRequest,
    "RenameWarehouseRequest": RenameWarehouseRequest,
    "Role": Role,
    "RoleAssignment": RoleAssignment,
    "RoleAssignmentAssignee": RoleAssignmentAssignee,
    "RoleAssignmentOwnership": RoleAssignmentOwnership,
    "S3Credential": S3Credential,
    "S3CredentialAccessKey": S3CredentialAccessKey,
    "S3Profile": S3Profile,
    "SearchRoleRequest": SearchRoleRequest,
    "SearchRoleResponse": SearchRoleResponse,
    "SearchUser": SearchUser,
    "SearchUserRequest": SearchUserRequest,
    "SearchUserResponse": SearchUserResponse,
    "ServerAssignment": ServerAssignment,
    "ServerAssignmentGlobalAdmin": ServerAssignmentGlobalAdmin,
    "ServerInfo": ServerInfo,
    "SetManagedAccessRequest": SetManagedAccessRequest,
    "StorageCredential": StorageCredential,
    "StorageCredentialAz": StorageCredentialAz,
    "StorageCredentialGcs": StorageCredentialGcs,
    "StorageCredentialS3": StorageCredentialS3,
    "StorageProfile": StorageProfile,
    "StorageProfileAzdls": StorageProfileAzdls,
    "StorageProfileGcs": StorageProfileGcs,
    "StorageProfileS3": StorageProfileS3,
    "TableAssignment": TableAssignment,
    "TableAssignmentCreate": TableAssignmentCreate,
    "TableAssignmentDescribe": TableAssignmentDescribe,
    "TableAssignmentManageGrants": TableAssignmentManageGrants,
    "TableAssignmentOwnership": TableAssignmentOwnership,
    "TableAssignmentPassGrants": TableAssignmentPassGrants,
    "TableAssignmentSelect": TableAssignmentSelect,
    "TabularDeleteProfile": TabularDeleteProfile,
    "TabularDeleteProfileHard": TabularDeleteProfileHard,
    "TabularDeleteProfileSoft": TabularDeleteProfileSoft,
    "UpdateNamespaceAssignmentsRequest": UpdateNamespaceAssignmentsRequest,
    "UpdateProjectAssignmentsRequest": UpdateProjectAssignmentsRequest,
    "UpdateRoleAssignmentsRequest": UpdateRoleAssignmentsRequest,
    "UpdateRoleRequest": UpdateRoleRequest,
    "UpdateServerAssignmentsRequest": UpdateServerAssignmentsRequest,
    "UpdateTableAssignmentsRequest": UpdateTableAssignmentsRequest,
    "UpdateUserRequest": UpdateUserRequest,
    "UpdateViewAssignmentsRequest": UpdateViewAssignmentsRequest,
    "UpdateWarehouseAssignmentsRequest": UpdateWarehouseAssignmentsRequest,
    "UpdateWarehouseCredentialRequest": UpdateWarehouseCredentialRequest,
    "UpdateWarehouseDeleteProfileRequest": UpdateWarehouseDeleteProfileRequest,
    "UpdateWarehouseStorageRequest": UpdateWarehouseStorageRequest,
    "User": User,
    "UserOrRole": UserOrRole,
    "UserOrRoleRole": UserOrRoleRole,
    "UserOrRoleUser": UserOrRoleUser,
    "ViewAssignment": ViewAssignment,
    "ViewAssignmentDescribe": ViewAssignmentDescribe,
    "ViewAssignmentManageGrants": ViewAssignmentManageGrants,
    "ViewAssignmentModify": ViewAssignmentModify,
    "ViewAssignmentOwnership": ViewAssignmentOwnership,
    "ViewAssignmentPassGrants": ViewAssignmentPassGrants,
    "WarehouseAssignment": WarehouseAssignment,
    "WarehouseAssignmentCreate": WarehouseAssignmentCreate,
    "WarehouseAssignmentDescribe": WarehouseAssignmentDescribe,
    "WarehouseAssignmentManageGrants": WarehouseAssignmentManageGrants,
    "WarehouseAssignmentModify": WarehouseAssignmentModify,
    "WarehouseAssignmentOwnership": WarehouseAssignmentOwnership,
    "WarehouseAssignmentPassGrants": WarehouseAssignmentPassGrants,
    "WarehouseAssignmentSelect": WarehouseAssignmentSelect,
}

// Check if a string starts with another string without using es6 features
function startsWith(str: string, match: string): boolean {
    return str.substring(0, match.length) === match;
}

// Check if a string ends with another string without using es6 features
function endsWith(str: string, match: string): boolean {
    return str.length >= match.length && str.substring(str.length - match.length) === match;
}

const nullableSuffix = " | null";
const optionalSuffix = " | undefined";
const arrayPrefix = "Array<";
const arraySuffix = ">";
const mapPrefix = "{ [key: string]: ";
const mapSuffix = "; }";

export class ObjectSerializer {
    public static findCorrectType(data: any, expectedType: string) {
        if (data == undefined) {
            return expectedType;
        } else if (primitives.indexOf(expectedType.toLowerCase()) !== -1) {
            return expectedType;
        } else if (expectedType === "Date") {
            return expectedType;
        } else {
            if (enumsMap[expectedType]) {
                return expectedType;
            }

            if (!typeMap[expectedType]) {
                return expectedType; // w/e we don't know the type
            }

            // Check the discriminator
            let discriminatorProperty = typeMap[expectedType].discriminator;
            if (discriminatorProperty == null) {
                return expectedType; // the type does not have a discriminator. use it.
            } else {
                if (data[discriminatorProperty]) {
                    var discriminatorType = data[discriminatorProperty];
                    if(typeMap[discriminatorType]){
                        return discriminatorType; // use the type given in the discriminator
                    } else {
                        return expectedType; // discriminator did not map to a type
                    }
                } else {
                    return expectedType; // discriminator was not present (or an empty string)
                }
            }
        }
    }

    public static serialize(data: any, type: string): any {
        if (data == undefined) {
            return data;
        } else if (primitives.indexOf(type.toLowerCase()) !== -1) {
            return data;
        } else if (endsWith(type, nullableSuffix)) {
            let subType: string = type.slice(0, -nullableSuffix.length); // Type | null => Type
            return ObjectSerializer.serialize(data, subType);
        } else if (endsWith(type, optionalSuffix)) {
            let subType: string = type.slice(0, -optionalSuffix.length); // Type | undefined => Type
            return ObjectSerializer.serialize(data, subType);
        } else if (startsWith(type, arrayPrefix)) {
            let subType: string = type.slice(arrayPrefix.length, -arraySuffix.length); // Array<Type> => Type
            let transformedData: any[] = [];
            for (let index = 0; index < data.length; index++) {
                let datum = data[index];
                transformedData.push(ObjectSerializer.serialize(datum, subType));
            }
            return transformedData;
        } else if (startsWith(type, mapPrefix)) {
            let subType: string = type.slice(mapPrefix.length, -mapSuffix.length); // { [key: string]: Type; } => Type
            let transformedData: { [key: string]: any } = {};
            for (let key in data) {
                transformedData[key] = ObjectSerializer.serialize(
                    data[key],
                    subType,
                );
            }
            return transformedData;
        } else if (type === "Date") {
            return data.toISOString();
        } else {
            if (enumsMap[type]) {
                return data;
            }
            if (!typeMap[type]) { // in case we dont know the type
                return data;
            }

            // Get the actual type of this object
            type = this.findCorrectType(data, type);

            // get the map for the correct type.
            let attributeTypes = typeMap[type].getAttributeTypeMap();
            let instance: {[index: string]: any} = {};
            for (let index = 0; index < attributeTypes.length; index++) {
                let attributeType = attributeTypes[index];
                instance[attributeType.baseName] = ObjectSerializer.serialize(data[attributeType.name], attributeType.type);
            }
            return instance;
        }
    }

    public static deserialize(data: any, type: string): any {
        // polymorphism may change the actual type.
        type = ObjectSerializer.findCorrectType(data, type);
        if (data == undefined) {
            return data;
        } else if (primitives.indexOf(type.toLowerCase()) !== -1) {
            return data;
        } else if (endsWith(type, nullableSuffix)) {
            let subType: string = type.slice(0, -nullableSuffix.length); // Type | null => Type
            return ObjectSerializer.deserialize(data, subType);
        } else if (endsWith(type, optionalSuffix)) {
            let subType: string = type.slice(0, -optionalSuffix.length); // Type | undefined => Type
            return ObjectSerializer.deserialize(data, subType);
        } else if (startsWith(type, arrayPrefix)) {
            let subType: string = type.slice(arrayPrefix.length, -arraySuffix.length); // Array<Type> => Type
            let transformedData: any[] = [];
            for (let index = 0; index < data.length; index++) {
                let datum = data[index];
                transformedData.push(ObjectSerializer.deserialize(datum, subType));
            }
            return transformedData;
        } else if (startsWith(type, mapPrefix)) {
            let subType: string = type.slice(mapPrefix.length, -mapSuffix.length); // { [key: string]: Type; } => Type
            let transformedData: { [key: string]: any } = {};
            for (let key in data) {
                transformedData[key] = ObjectSerializer.deserialize(
                    data[key],
                    subType,
                );
            }
            return transformedData;
        } else if (type === "Date") {
            return new Date(data);
        } else {
            if (enumsMap[type]) {// is Enum
                return data;
            }

            if (!typeMap[type]) { // dont know the type
                return data;
            }
            let instance = new typeMap[type]();
            let attributeTypes = typeMap[type].getAttributeTypeMap();
            for (let index = 0; index < attributeTypes.length; index++) {
                let attributeType = attributeTypes[index];
                instance[attributeType.name] = ObjectSerializer.deserialize(data[attributeType.baseName], attributeType.type);
            }
            return instance;
        }
    }
}

export interface Authentication {
    /**
    * Apply authentication settings to header and query params.
    */
    applyToRequest(requestOptions: localVarRequest.Options): Promise<void> | void;
}

export class HttpBasicAuth implements Authentication {
    public username: string = '';
    public password: string = '';

    applyToRequest(requestOptions: localVarRequest.Options): void {
        requestOptions.auth = {
            username: this.username, password: this.password
        }
    }
}

export class HttpBearerAuth implements Authentication {
    public accessToken: string | (() => string) = '';

    applyToRequest(requestOptions: localVarRequest.Options): void {
        if (requestOptions && requestOptions.headers) {
            const accessToken = typeof this.accessToken === 'function'
                            ? this.accessToken()
                            : this.accessToken;
            requestOptions.headers["Authorization"] = "Bearer " + accessToken;
        }
    }
}

export class ApiKeyAuth implements Authentication {
    public apiKey: string = '';

    constructor(private location: string, private paramName: string) {
    }

    applyToRequest(requestOptions: localVarRequest.Options): void {
        if (this.location == "query") {
            (<any>requestOptions.qs)[this.paramName] = this.apiKey;
        } else if (this.location == "header" && requestOptions && requestOptions.headers) {
            requestOptions.headers[this.paramName] = this.apiKey;
        } else if (this.location == 'cookie' && requestOptions && requestOptions.headers) {
            if (requestOptions.headers['Cookie']) {
                requestOptions.headers['Cookie'] += '; ' + this.paramName + '=' + encodeURIComponent(this.apiKey);
            }
            else {
                requestOptions.headers['Cookie'] = this.paramName + '=' + encodeURIComponent(this.apiKey);
            }
        }
    }
}

export class OAuth implements Authentication {
    public accessToken: string = '';

    applyToRequest(requestOptions: localVarRequest.Options): void {
        if (requestOptions && requestOptions.headers) {
            requestOptions.headers["Authorization"] = "Bearer " + this.accessToken;
        }
    }
}

export class VoidAuth implements Authentication {
    public username: string = '';
    public password: string = '';

    applyToRequest(_: localVarRequest.Options): void {
        // Do nothing
    }
}

export type Interceptor = (requestOptions: localVarRequest.Options) => (Promise<void> | void);
