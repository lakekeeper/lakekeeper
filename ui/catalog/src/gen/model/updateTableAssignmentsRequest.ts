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
import { TableAssignment } from './tableAssignment';

export class UpdateTableAssignmentsRequest {
    'deletes'?: Array<TableAssignment>;
    'writes'?: Array<TableAssignment>;

    static discriminator: string | undefined = undefined;

    static attributeTypeMap: Array<{name: string, baseName: string, type: string}> = [
        {
            "name": "deletes",
            "baseName": "deletes",
            "type": "Array<TableAssignment>"
        },
        {
            "name": "writes",
            "baseName": "writes",
            "type": "Array<TableAssignment>"
        }    ];

    static getAttributeTypeMap() {
        return UpdateTableAssignmentsRequest.attributeTypeMap;
    }
}
