# Approval Service

`approval` is an explicitly installed event-sourced business service. It owns
the lifecycle and audit facts for human approval requests. Requests,
resolutions, cancellations, expirations, UI notifications, and Capability
results all travel through durable Runtime messages; the service never calls a
Capability service or user-interface object directly.

The mount requires an `interaction` dependency. An optional `scheduler`
dependency may send durable `approval.expire` commands. Resolver identity is
taken from the trusted message `From` and `UserID` fields, not from payload
claims.
