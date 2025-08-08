## Messaging Domains, Connections, and Topics

This document summarizes the provided registries in `NDW/Messaging/go`.

### Domains
- **DomainA** (ID 1): NATS.io (2.7.2)
- **DomainB** (ID 2): Kafka (disabled)
- **DomainC** (ID 4): Solace (disabled)
- **Aeron Messaging** (ID 3): Aeron (disabled)

### DomainA (NATS.io)

Connections:
- **NATSConn1** (enabled)
  - URL: `nats://localhost:4222`
  - Topics:
    - `ACME.Orders` (PubKey: `ACME.Orders`, SubKey: `ACME.Orders`)
    - `ACME.Invoices` (PubKey: `ACME.Invoices`, SubKey: `ACME.Invoices`)
- **NATSConn2** (enabled)
  - URL: `nats://localhost:4222`
  - Topics:
    - `ACME.Shipments` (PubKey: `ACME.Shipments`, SubKey: `ACME.Shipments`)
- **NATSConnNews** (disabled by default)
  - JetStream examples with subjects under `News.*` (enable to use):
    - `NEWS_Sports_ALL`: SubKey `News.Sports.>` (wildcard), JS options set
    - `NEWS_Sports_Football`: `News.Sports.Football`
    - `NEWS_Sports_College_Football`: `News.Sports.CollegeFootball`
    - `NEWS_MOVIES_ALL`: `News.Archive.Movies.>` (wildcard)
    - `NEWS_Movies_Romance`: `News.Archive.Movies.Romance`
    - `NEWS_Movies_Westerns`: `News.Archive.Movies.Westerns`

### Application topic sets

Example app configs referencing DomainA:

- `APP_pubsub.JSON` (Async NATS)
  - `DomainA^NATSConn1^ACME.Orders` (Publish/Subscribe)
  - `DomainA^NATSConn1^ACME.Invoices` (Publish/Subscribe)
  - `DomainA^NATSConn2^ACME.Shipments` (Publish/Subscribe)

- `APP_syncsub.JSON` (Synchronous poll)
  - Same topics as above with `SubscribeType: NATSPoll`

- `APP_JS_PUSH.JSON` (JetStream push)
  - `NEWS_Sports_ALL` (Subscribe)
  - `NEWS_Sports_Football` (Publish/Subscribe)
  - `NEWS_Sports_College_Football` (Publish/Subscribe)

- `APP_JS_POLL.JSON` (JetStream pull)
  - `NEWS_MOVIES_ALL` (Subscribe)
  - `NEWS_Movies_Romance` (Publish/Subscribe)
  - `NEWS_Movies_Westerns` (Publish/Subscribe)

### Notes
- Enable JetStream connection/topics in the main registry to use JS examples.
- Subject wildcards `>` are supported on SubKey where noted.
- Limits and vendor options are set via `TopicOptions` and `VendorTopicOptions` in the registry.