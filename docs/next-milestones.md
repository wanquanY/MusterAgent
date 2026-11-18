# Device execution

The cloud worker, reference API, scheduler and projector run against the
PostgreSQL store. The placement contracts already distinguish a run on the
current device from a run on a selected paired device.

End-to-end device execution still needs a durable local store, a transport that
authenticates assignments and a device worker that can resume a fenced attempt.
The host supplies device identity, membership proof and operating-system
keystore access. Device tools must be bound to the selected device and its
authorized workspace. Desktop and mobile hosts should exercise the same
placement, cancellation, recovery and replay contracts as the cloud worker.

See the [wire contracts](integration/wire-contracts.md) and
[work item and media contracts](integration/work-items-and-media.md).
