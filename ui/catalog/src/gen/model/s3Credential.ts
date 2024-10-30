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
import { S3CredentialAccessKey } from './s3CredentialAccessKey';

export class S3Credential {
    'awsAccessKeyId': string;
    'awsSecretAccessKey': string;
    'credentialType': S3Credential.CredentialTypeEnum;

    static discriminator: string | undefined = "credentialType";

    static attributeTypeMap: Array<{name: string, baseName: string, type: string}> = [
        {
            "name": "awsAccessKeyId",
            "baseName": "aws-access-key-id",
            "type": "string"
        },
        {
            "name": "awsSecretAccessKey",
            "baseName": "aws-secret-access-key",
            "type": "string"
        },
        {
            "name": "credentialType",
            "baseName": "credential-type",
            "type": "S3Credential.CredentialTypeEnum"
        }    ];

    static getAttributeTypeMap() {
        return S3Credential.attributeTypeMap;
    }
}

export namespace S3Credential {
    export enum CredentialTypeEnum {
        AccessKey = <any> 'access-key'
    }
}
