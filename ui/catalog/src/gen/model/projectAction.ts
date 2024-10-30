/**
 * iceberg-catalog
 * Implementation of the Iceberg REST Catalog server. 
 *
 * The version of the OpenAPI document: 0.4.2
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */

import { RequestFile } from './models';

export enum ProjectAction {
    CreateWarehouse = <any> 'create_warehouse',
    Delete = <any> 'delete',
    Rename = <any> 'rename',
    ListWarehouses = <any> 'list_warehouses',
    CreateRole = <any> 'create_role',
    ListRoles = <any> 'list_roles',
    SearchRoles = <any> 'search_roles',
    ReadAssignments = <any> 'read_assignments',
    GrantRoleCreator = <any> 'grant_role_creator',
    GrantCreate = <any> 'grant_create',
    GrantDescribe = <any> 'grant_describe',
    GrantModify = <any> 'grant_modify',
    GrantSelect = <any> 'grant_select',
    GrantProjectAdmin = <any> 'grant_project_admin',
    GrantSecurityAdmin = <any> 'grant_security_admin',
    GrantWarehouseAdmin = <any> 'grant_warehouse_admin'
}
