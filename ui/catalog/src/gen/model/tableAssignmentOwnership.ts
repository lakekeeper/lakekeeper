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

export class TableAssignmentOwnership {
    'user': string;
    'role': string;
    'type': TableAssignmentOwnership.TypeEnum;

    static discriminator: string | undefined = undefined;

    static attributeTypeMap: Array<{name: string, baseName: string, type: string}> = [
        {
            "name": "user",
            "baseName": "user",
            "type": "string"
        },
        {
            "name": "role",
            "baseName": "role",
            "type": "string"
        },
        {
            "name": "type",
            "baseName": "type",
            "type": "TableAssignmentOwnership.TypeEnum"
        }    ];

    static getAttributeTypeMap() {
        return TableAssignmentOwnership.attributeTypeMap;
    }
}

export namespace TableAssignmentOwnership {
    export enum TypeEnum {
        Ownership = <any> 'ownership'
    }
}
