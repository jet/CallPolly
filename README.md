# CallPolly

[_Polly_](https://github.com/App-vNext/Polly) is a focused library _and a rich set of documentation, samples and discussions_ around various resilience patterns for .NET.

Look no further for a mature, well thought out set of abstractions with a responsive community.

----

**_All this is presently under heavy development, please excuse the lack of roadmap for a bit longer..._**

CallPolly wraps Polly to provide:
- parsing and validation of a suite of rules in a declarative form (presently json, but being able to maintain the inputs in YAML is on the wishlist)
- a ruleset interpreter that applies the rules in a consistent fashion

Using CallPolly will _eventually_ enable one to:
- wrap calls with minimal invasive code changes
- use [Open Tracing](http://opentracing.io/)-based Distributed Tracing systems and dashboards to analyze the effects of the configured Call Policy as applied
- iteratively adjust those policies across a set of systems

## Taster: example policy

See the [acceptance tests](https://github.com/jet/CallPolly/blob/master/tests/CallPolly.Acceptance/Orchestration.fs#L142) for behavior implied by this configuration:
```
{ "services": {

"ingres": {
    "calls": {
        "api-a": "quick",
        "api-b": "slow"
    },
    "defaultPolicy": null,
    "policies": {
        "quick": [
            { "rule": "Cutoff",     "timeoutMs": 1000, "slaMs": 500 }
        ],
        "slow": [
            { "rule": "Cutoff",     "timeoutMs": 10000, "slaMs": 5000 }
        ]
    }
},
"upstreamA": {
    "calls": {
        "Call1": "looser",
        "Call2": "default",
        "CallBroken": "defaultBroken"
    },
    "defaultPolicy": null,
    "policies": {
        "default": [
            { "rule": "Limit",      "maxParallel": 10, "maxQueue": 3 }
        ],
        "looser": [
            { "rule": "Limit",      "maxParallel": 100, "maxQueue": 300 }
        ],
        "defaultBroken": [
            { "rule": "Isolate" }
        ]
    }
},
"upstreamB": {
    "calls": {
        "Call1": "default",
    },
    "defaultPolicy": null,
    "policies": {
        "default": [
            { "rule": "Limit",      "maxParallel": 2, "maxQueue": 8 },
            { "rule": "Break",      "windowS": 5, "minRequests": 10, "failPct": 20, "breakS": 1 }
        ]
    }
}

}
```