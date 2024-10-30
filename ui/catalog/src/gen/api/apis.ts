export * from './permissionsApi';
import { PermissionsApi } from './permissionsApi';
export * from './projectApi';
import { ProjectApi } from './projectApi';
export * from './roleApi';
import { RoleApi } from './roleApi';
export * from './serverApi';
import { ServerApi } from './serverApi';
export * from './userApi';
import { UserApi } from './userApi';
export * from './warehouseApi';
import { WarehouseApi } from './warehouseApi';
import * as http from 'http';

export class HttpError extends Error {
    constructor (public response: http.IncomingMessage, public body: any, public statusCode?: number) {
        super('HTTP request failed');
        this.name = 'HttpError';
    }
}

export { RequestFile } from '../model/models';

export const APIS = [PermissionsApi, ProjectApi, RoleApi, ServerApi, UserApi, WarehouseApi];
