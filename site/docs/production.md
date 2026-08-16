---
title: "Production"
---

# Running Lakekeeper in Production

Lakekeeper is the heart of your data platform. The open-source catalog is built to run in production — it holds no local state, scales horizontally behind a load balancer, and upgrades stay online while database migrations run. Most teams reach production on the OSS edition alone.

For regulated industries and teams that want the catalog operated *for* them — permission-as-code, autonomous maintenance, and enterprise support — **Lakekeeper+** <span class="lkp"></span> is the production-grade path, backed by [Vakamo](https://vakamo.com).

## Production checklist (open source)

Everything you need to run Lakekeeper OSS safely is covered in the docs. In short:

- **High-availability Postgres** as the catalog backend, with separate read/write URLs and regular, tested backups.
- **Authentication** enabled through your existing OpenID provider (or Kubernetes auth).
- **Authorization** configured for your access model.
- **Horizontal scaling** with multiple instances — use the [Helm chart](https://github.com/lakekeeper/lakekeeper-charts/tree/main/charts/lakekeeper), which ships readiness/liveness probes and autoscaling.
- **TLS termination** at a reverse proxy or ingress — Lakekeeper does not terminate connections itself.
- **Monitoring & observability** wired to your stack.

See the full [Production Checklist](./docs/nightly/docs/production.md) for the complete, up-to-date list.

## Lakekeeper+: the production-grade path <span class="lkp"></span>

Lakekeeper+ builds on the open-source catalog with the capabilities regulated and large-scale platforms need, developed and supported by the team behind Lakekeeper.

<div class="grid cards" markdown>

- :material-file-lock: &nbsp; __Permission-as-code with Cedar__

    ---

    Express access policy as versioned, reviewable [Cedar](./docs/nightly/docs/authorization-cedar.md) policies — RBAC, ABAC, and property-based access in one model. Every decision is inspectable, with a per-decision policy trace and audit log, so you can prove *why* access was granted. Built for **regulated industries** where access must be governed, testable, and provable.

- :material-robot: &nbsp; __Autonomous maintenance__

    ---

    Keep query performance high and storage costs low without operating maintenance jobs yourself. [Table maintenance](./docs/nightly/docs/table-maintenance.md) — expiring snapshots and removing orphan files — runs automatically and adaptively, scheduling itself per table based on how fast reclaimable data builds up.

- :material-account-key: &nbsp; __Enterprise identity & governance__

    ---

    Resolve roles from Okta, Microsoft Entra ID, and LDAP; gate access with an external admission check; protect provider-synced roles from drift. Structured audit logs make the catalog auditable end to end.

- :material-lifebuoy: &nbsp; __Enterprise support & LTS__

    ---

    Commercial support for self-hosted and managed deployments, plus hardened Long-Term Support release lines — so you can run Lakekeeper at the heart of your platform with confidence.

</div>

## Get Lakekeeper+

Talk to the team at Vakamo about running Lakekeeper in production — self-hosted or managed. We help you understand which edition fits, and get you there.

<div class="lkplus-cta">
  <a href="https://forms.zohopublic.com/supportvak1/form/Contactus/formperma/lTpraap5Nwq1DckVGakAqP0NPo1qWTu3JxPZ9bP07CQ" target="_blank" rel="noopener noreferrer" id="lkplus-contact-btn" class="md-button md-button--primary" aria-expanded="false" aria-controls="lkplus-contact-form" data-goatcounter-click="vakamo-cta-production-primary" data-goatcounter-title="Production page — Talk to us">Talk to us about Lakekeeper+</a>
  <a href="/about/enterprise-release-notes/" class="md-button" data-goatcounter-click="production-plus-release-notes" data-goatcounter-title="Production page — Plus release notes">See Plus release notes</a>
</div>

<small>Opens a secure contact form hosted by Zoho on behalf of Vakamo; submitting it shares your details with Vakamo.</small>

<div id="lkplus-contact-form" hidden></div>

<script>
  (function () {
    var btn = document.getElementById("lkplus-contact-btn");
    var box = document.getElementById("lkplus-contact-form");
    if (!btn || !box) return;
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      if (box.dataset.loaded) { box.hidden = false; box.scrollIntoView({ behavior: "smooth", block: "nearest" }); return; }
      box.dataset.loaded = "1";
      var f = document.createElement("iframe");
      f.src = btn.href;
      f.setAttribute("aria-label", "Contact Vakamo about Lakekeeper+");
      f.setAttribute("frameborder", "0");
      f.setAttribute("loading", "lazy");
      f.style.cssText = "width:100%;height:760px;border:none;border-radius:12px;margin-top:1rem;";
      box.appendChild(f);
      box.hidden = false;
      btn.setAttribute("aria-expanded", "true");
      box.scrollIntoView({ behavior: "smooth", block: "nearest" });
    });
  })();
</script>

Prefer to chat with the community first? [Join us on Discord](https://discord.gg/jkAGG8p93B).
