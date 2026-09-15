# Regional OSS reads with a shared Master

The default replica selector prefers MEMORY over LOCAL_DISK. With a WAN
Master and a remote memory replica this may take the data path across regions
and exceed the durable read fence lease. Increasing `default_kv_lease_ttl`
does not override the durable protocol's lease bound.

For the Mint sidecar's ordinary blob reads, set
`MC_STORE_REQUIRED_OFFLOAD_ENDPOINT` in the sidecar process environment to
the exact `LocalDiskDescriptor.transport_endpoint` advertised by its regional
provider. This is the provider's offload RPC endpoint, **not** its Master
address, transfer-engine port, IPC port, or placement host identity.

When set, ordinary replica selection requires a COMPLETE LOCAL_DISK replica
at that exact endpoint, even when memory replicas exist. An absent provider,
wrong port, or an object not yet discovered by that provider yields no usable
replica; the selector does not silently fall back to remote memory. Allow
provider discovery to converge before retrying. The default empty setting
retains the existing selection behavior. The setting is read once per process;
restart the sidecar with the updated endpoint after a provider port change.

This is a data routing option, not an authorization boundary. It does not
change the C ABI, give the sidecar OSS credentials, bypass Master discovery,
extend read leases, or disable durable-delete fencing. Specialized APIs that
explicitly request memory sessions are outside this ordinary selector mode.
Configure it only for clients whose reads use ordinary replica selection.

Deployments should record the provider's actual endpoint, process/library
hashes, and the option in their runtime manifest. Acceptance requires source
online and source offline reads, a fresh destination cache, matching object
hashes, and deletion safety tests. A successful cold read alone does not prove
that the source-online memory path is safe.
