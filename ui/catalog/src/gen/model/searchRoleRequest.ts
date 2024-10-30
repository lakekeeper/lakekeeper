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

export class SearchRoleRequest {
    'projectId'?: string | null;
    /**
    * Search string for fuzzy search. Length is truncated to 64 characters.
    */
    'search': string;

    static discriminator: string | undefined = undefined;

    static attributeTypeMap: Array<{name: string, baseName: string, type: string}> = [
        {
            "name": "projectId",
            "baseName": "project-id",
            "type": "string"
        },
        {
            "name": "search",
            "baseName": "search",
            "type": "string"
        }    ];

    static getAttributeTypeMap() {
        return SearchRoleRequest.attributeTypeMap;
    }
}

