Configuration Framework Service Patch Notes
--

About
---
Performance issues have been noted with the Configuration Framework Service (CFS) operator
at scale after extended use. CFS defines a Kubernetes Custom Resource Definition (CRD)
for handling state changes in a way that makes CFS a first class Kubernetes deployment
citizen, e.g., it is possible to query Kubernetes directly for information about CFS
Sessions:
| kubectl get cfs -n services

Larger systems tend to accumulate larger numbers of CFS sessions more quickly because
more CFS sessions are required to configure a larger number of nodes.

Additional Background
---
The CFS Operator is a stand alone deployment that operates on changes to CFS instances
from within its Kubernetes CRD. It is responsible for setting status changes and scheduling
new CFS Sessions when Kubernetes indicates there is a change.

Kubernetes does not have a robust event filtering API for these events, and as a result,
the event buffer queue can become overloaded; this can lead to a loss of event reporting. This is
especially true for Synthetic events -- events that have happened in the past but are replayed
to newly connecting clients.

Changes in a future release automatically remove completed CFS sessions after a defined period
of time and employ additional reconciliation logic with the reported Kubernetes state.

Symptoms
---
When the K8s event queue becomes overloaded, stale events can no longer be read from
Kubernetes directly, and Kubernetes returns a HTTP/500 error; usually this is visible from
the CFS operator pod directly and looks like this:

kubernetes.client.rest.ApiException: (500)
Reason: Internal Server Error
HTTP response headers: HTTPHeaderDict({'Content-Type': 'application/json', 'Date': 'Wed, 12 Aug 2020 10:04:12 GMT', 'Content-Length': '186'})
HTTP response body: b'{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"resourceVersion: Invalid value: \\"None\\": strconv.ParseUint: parsing \\"None\\": invalid syntax","code":500}\n'

Most commonly, these instances will prevent the cfs-operator from starting (or restarting
during a migration) in a timely manner.

Mitigation
---
In order to get around this, old CFS instances should be removed from your system manually:

  for name in $(cray cfs sessions list --format json | jq -r '.[] | select(.name | startswith("batcher")) | .name'); do cray cfs sessions delete $name; done

This removes all CFS sessions that have been scheduled by the batcher implementation, and,
as a result, all associated CFS pods with ansible logs are also removed. It may be necessary
to restart the cfs-operator deployment so that it can be re-instated cleanly once again:

  kubectl -n services rollout restart deployment cray-cfs-operator

