---
description: "Choose and configure a Lakekeeper Authorizer — OpenFGA or Cedar — and learn how grants, privileges and roles control access to catalog objects."
---

# Authorization

Authentication verifies *who* you are, while authorization determines *what* you can do. Authorization can only be enabled if Authentication is enabled — see the [Authentication docs](./authentication.md).

## Choose an authorizer

Lakekeeper delegates every access decision to one configured **Authorizer**. This is the first decision to make: it determines how permissions are expressed, who changes them, and what day-to-day administration looks like.

| | [OpenFGA](./authorization-openfga.md) | [Cedar](./authorization-cedar.md)<span class="lkp"></span> |
|---|---|---|
| Availability | Open source | Lakekeeper Plus |
| Extra service to run | Yes — an OpenFGA deployment with its own database | No, built in |
| How permissions are expressed | Relationships between principals and objects, stored as data | Policies you author and deploy |
| Who changes them | Admins **and** object owners, at runtime, through the UI or API | Whoever can deploy the policy source |
| Conditions on attributes | No | Yes — time, tags, request attributes |
| Grants API | Full vocabulary | Planned for 0.14 |
| Changing your mind later | You can switch **to** OpenFGA on a running deployment | Switching away generally needs a new Lakekeeper instance |

Two further authorizers exist for narrower purposes. **AllowAll** permits every request and is meant for development and testing only — it records grants faithfully but enforces nothing. **Custom** lets you implement the `Authorizer` trait yourself; see [Customize](./customize.md).

Neither engine expresses row filters or column masks. Both decide whether a principal may perform an action on an object; filtering rows or columns *within* an object is not something Lakekeeper enforces.

Configuration for each is in the [Authorization configuration](./configuration.md#authorization) reference.

## What to read next

- **Evaluating Lakekeeper?** Read the page for the authorizer you are leaning towards, and stop there.
- **Setting one up?** The same page — each carries its own model, roles and configuration.
- **Need Alice to read a table?** Under OpenFGA, use the UI — or the [Grants API](./grants.md) if you are automating it — and note that object owners can hand out access to their own objects. Under Cedar, access comes from your policy source, so change that instead.
- **Operating the deployment?** See [Instance Admins](./instance-admins.md) for administrative access that does not depend on the authorizer being healthy.

## Grants, privileges and roles

Three words are used consistently across the authorizers and the API:

- A **privilege** is the name of a capability — `select`, `modify`. Which privileges exist is defined by your authorizer.
- A **grant** gives one privilege on one resource to one principal: *Alice may `select` on this warehouse*.
- A **principal** is a user or a **role**. Granting to a role once and then managing its membership is how you keep the number of grants manageable. Where role membership comes from — an identity provider, or Lakekeeper itself — depends on your setup; see [Configuration](./configuration.md).

### Direct grants are not effective permissions

A grant recorded on a resource is not the whole answer to "what can Alice do here". Your authorizer's model decides what a grant *reaches*: whether `select` implies `describe`, and whether a warehouse grant covers the tables inside it. Role membership and inheritance are resolved when a request is decided, not stored as extra grants.

So a listing of grants on a table shows what was recorded *there*, for *that* principal. To ask what a principal may effectively do, use the per-resource `.../actions` endpoints or `POST /management/v1/action/batch-check`.
