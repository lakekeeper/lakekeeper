# Authorization

## Overview

Authentication verifies *who* you are, while authorization determines *what* you can do.

Authorization can only be enabled if Authentication is enabled. Please check the [Authentication Docs](./authentication.md) for more information.

Lakekeeper currently supports the following Authorizers:

* **AllowAll**: A simple authorizer that allows all requests. This is mainly intended for development and testing purposes.
* **OpenFGA**: A fine-grained authorization system based on the CNCF project [OpenFGA](https://openfga.dev). OpenFGA requires an additional OpenFGA service to be deployed (this is included in our self-contained examples and our helm charts). See the [Authorization with OpenFGA](./authorization-openfga.md) guide for details.
* **Cedar**<span class="lkp"></span>: An enterprise-grade policy-based authorization system based on [Cedar](https://cedarpolicy.com). The Cedar authorizer is built into Lakekeeper and requires no additional external services. See the [Authorization with Cedar](./authorization-cedar.md) guide for details.
* **Custom**: Lakekeeper supports custom authorizers via the `Authorizer` trait.

Check the [Authorization Configuration](./configuration.md#authorization) for setup details.
