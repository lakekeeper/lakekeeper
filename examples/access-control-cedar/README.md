## Access Control with Cedar Policy Engine

This example demonstrates how to use Lakekeeper Plus with Cedar policy engine for fine-grained access control.

The example includes two pre-configured users:
* **Peter** (admin) - has full access to all resources
* **Anna** (limited user) - initially has no access, used to demonstrate table-level authorization

The example contains:
* **Lakekeeper Plus** with Cedar authorization backend
* **Trino** SQL query engine with OAuth2 authentication
* **Jupyter** with built-in Spark for interactive data exploration
* **Keycloak** as OpenID Connect Identity Provider
* **PostgreSQL** for catalog metadata storage
* **MinIO** for object storage
* **Nginx** reverse proxy for HTTPS (required by Trino OAuth2)

### Prerequisites

1. **License Key**: You need a valid Lakekeeper Plus license key. 
   
   **Get a Free 30-Day Trial License**: Visit [https://vakamo.com/trial](https://vakamo.com/trial) to generate a free trial license.
   
   The `lakekeeper.sh` script will prompt you for your license key on first run and save it automatically.

2. **Cedar Policies**: The example includes a policy file in the `policies/` directory:
   - `policies.cedar`: Cedar policy definitions for users

### Running the Example

**Option 1: Using the lakekeeper script (recommended)**

```bash
cd examples/access-control-cedar
./lakekeeper.sh
```

The script will:
1. Automatically detect your host IP address (required for MinIO access from both containers and browser)
2. Prompt you for your license key on first run and save it to `.env`
3. Start MinIO standalone on the host network
4. Start all other services via docker compose

**To run in detached mode:**

```bash
./lakekeeper.sh up -d
```

**To update your license key:**

```bash
./lakekeeper.sh up --new-license
```

**Note**: The script automatically detects your host IP address to enable MinIO access from:
- Docker/Podman containers (for Lakekeeper storage operations)
- Your browser (for DuckDB table previews)

If automatic detection fails, the script will prompt you to enter your IP address manually.

**Option 2: Manual setup with docker compose**

If you prefer to run `docker compose` directly, you need to create the `.env` file first:

```bash
cd examples/access-control-cedar
cp .env.example .env
# Edit .env and set LAKEKEEPER__LICENSE__KEY
docker compose up
```

**Important**: Docker Compose will fail to start if `LAKEKEEPER__LICENSE__KEY` is not set in your `.env` file.

### Stopping the Services

```bash
./lakekeeper.sh down
```

Or with docker compose:

```bash
docker compose down
```

### Available Services

Once all services are up, you can access:

* **Jupyter Notebooks**: [http://localhost:8888](http://localhost:8888) - Start here for examples
* **Trino UI**: [https://localhost/ui/](https://localhost/ui/) - SQL query interface
* **Lakekeeper UI**: [http://localhost:8181](http://localhost:8181) - Catalog management interface
* **Keycloak UI**: [http://localhost:30080](http://localhost:30080) - Identity provider admin
* **Swagger API**: [http://localhost:8181/swagger-ui/](http://localhost:8181/swagger-ui/) - REST API documentation

### Getting Started

1. **Bootstrap the catalog**: Open Jupyter at [http://localhost:8888](http://localhost:8888) and run `01-Bootstrap.ipynb` to set up the initial admin user, create the demo warehouse, and create sample tables.

2. **Login credentials**: After bootstrapping, two users are available:
   - **Peter** (admin): username `peter`, password `iceberg` - has full access
   - **Anna** (test user): username `anna`, password `iceberg` - initially has no access

3. **Access the UIs**:
   - **Trino UI**: [https://localhost/ui/](https://localhost/ui/) - Use incognito/another browser to test different users
   - **Lakekeeper UI**: [http://localhost:8181](http://localhost:8181)
   - **Keycloak Admin**: [http://localhost:30080](http://localhost:30080) (admin/admin)

### Testing Authorization

This example demonstrates Cedar's table-level access control. Follow the notebooks in order:

**Step-by-step workflow:**

1. **Bootstrap** (`01-Bootstrap.ipynb`): Initialize Lakekeeper, create the warehouse, and register Peter as admin.

2. **Setup Trino** (`02-Trino-Preparation.ipynb`): Create the Trino catalog connection (run as Peter).

3. **Create Tables** (`03-Create-Tables.ipynb`): Peter creates the `finance.revenue` and `finance.products` tables and verifies he can query them.

4. **Grant Anna Access** (`04-Grant-Anna-Access.ipynb`): Add a Cedar policy granting Anna read access to only the `products` table (not `revenue`).

5. **Test Anna's Access** (`05-Test-Anna-Access.ipynb`): Anna can query `products` but cannot access `revenue`, demonstrating table-level authorization.

**Testing in Trino UI:**

To test different users in the browser:
- Login to Trino UI at [https://localhost/ui/](https://localhost/ui/) as Peter
- Open an incognito window or different browser to login as Anna (username: `anna`, password: `iceberg`)
- Try querying tables to verify Peter has full access while Anna can only access `products`

**Note**: Use incognito mode or a different browser for simultaneous multi-user testing, as each browser session maintains its own OAuth authentication.

### Cedar Policies

This example uses Cedar policies for authorization. The policies are stored in a Docker volume shared between containers, eliminating file permission issues.

**Policy Files**:
- Initial policies are copied from `policies/` directory on startup
- Policies are shared between Jupyter and Lakekeeper containers
- Edit policies directly in the notebooks (see `04-Grant-Anna-Access.ipynb` for an example)

**Auto-Reload**: Lakekeeper polls policy files every 5 seconds - changes are applied automatically without restart!

**How it works**:
- Peter's admin policy is defined in `policies/policies.cedar` and loaded on startup
- Anna's policy is added programmatically in `04-Grant-Anna-Access.ipynb`
- Changes to policy files are detected within 5-6 seconds automatically
- No service restart needed when modifying policies!

### Configuration

The example uses the following Lakekeeper configuration:
- **Authorization Backend**: Cedar
- **Policy Source**: Local file (`/policies/policies.cedar`)
- **OpenID Connect**: Keycloak at `http://keycloak:8080/realms/iceberg`
- **Storage**: MinIO S3-compatible storage
- **Database**: PostgreSQL for metadata

### Troubleshooting

**License Key Error**: If you see an error about `LAKEKEEPER__LICENSE__KEY`, make sure you've created a `.env` file with your license key:
```bash
LAKEKEEPER__LICENSE__KEY=eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSJ9...
```

**Policy Errors**: Check the lakekeeper logs for Cedar policy validation errors:
```bash
docker compose logs lakekeeper
```

### Cleanup

To stop and remove all containers:
```bash
docker compose down
```

To also remove volumes (warning: this deletes all data):
```bash
docker compose down -v
```
