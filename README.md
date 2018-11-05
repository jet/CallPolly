# CallPolly

:raised_hands: [**`Polly`**](https://github.com/App-vNext/Polly) is a focused .NET library _and a rich set of documentation, samples and discussions_ around various system resilience patterns.

Look no further for a mature, well thought out set of abstractions with a responsive community. While we think CallPolly brings a lot of value, there's equally a very good chance your needs can be fulfilled by using [Polly directly via its `PolicyRegistry` mechanism](https://github.com/App-vNext/Polly/wiki/PolicyRegistry).

----

_Please raise GitHub issues for any questions specific to CallPolly so others can benefit from the discussion._

## Goals

CallPolly wraps Polly to provide:
- parsing and validation of a suite of policies, composed of rules in a declarative form (presently json, but being able to maintain the inputs in YAML and other formats is a potential avenue)
- a policy interpreter that applies the rules in a consistent fashion
- carefully curated metrics and logging output so it can feed into your Distributed Tracing solution, whatever it is to understand whether it's Doing What You Mean
- the ability to iteratively refine the policies with low risk

## Non-goals

- low level policy implementations should live elsewhere (see [CONTRIBUTION notes](#contribution-notes)) - _[e.g., `BulkheadMulti` needs to move out](https://github.com/App-vNext/Polly/issues/507)_
- the core CallPolly library facilitates, but should never bind _directly_ to any specific log or metrics emission sink

# Elements

The library is delivered as two `net461`/`netstandard2.0` multi-targeted assemblies:-

## `CallPolly.Core`
- extends [`Polly`](https://github.com/App-vNext/Polly) with a [`BulkheadMulti`](https://github.com/jet/CallPolly/blob/master/src/CallPolly.Core/BulkheadMulti.fs) primitive
- In `CallPolly.Events`, defines Key events and metrics that are emitted to the [`Serilog`](https://github.com/serilog/serilog) logger.
- In `CallPolly.Governor`, defines how rules are applied.
- In `CallPolly.Policy`, defines policy lookups, and how live-updating policies is managed.

## `CallPolly`

- reads policies in a standardized format entitled `ServicePolicy` using `Jet.JsonNet.Converters`
- maps these inputs to `CallPolly`'s inputs

For reasons of code clarity and performance, a core secondary dependency is [`Serilog`](https://github.com/serilog/serilog); the pervasiveness and low dependency nature of Serilog and the [practical unlimited interop with other loggers/targets/sinks](https://github.com/serilog/serilog/wiki/Provided-Sinks) is considered enough of a win to make this a hard dependency _e.g., if your logger is NLog, it's 2 lines of code to [forward to it](https://www.nuget.org/packages/serilog.sinks.nlog) with minimal perf cost over CallPolly binding to that directly_.

# Dependencies

The `CallPolly.Core` library
- extends [`Polly`](https://github.com/App-vNext/Polly) 
- logs to [`Serilog`](https://github.com/serilog/serilog).
- references [`Newtonsoft.Json`](https://github.com/JamesNK/Newtonsoft.Json) solely to tweak the renditions of the Policy State renditions from `DumpState` (reference is to `>= v11.0.2` is for reasons of having a clean set of dependencies for the `netstandard2.0` variant).

The `CallPolly` library:
- reads policies using [`Jet.JsonNet.Converters`](https://github.com/jet/Jet.JsonNet.Converters) and [`Newtonsoft.Json`](https://github.com/JamesNK/Newtonsoft.Json)
- feeds those into `PolicyBuilder` in `CallPolly.Core`.

Being written in F#, there's a dependency on `FSharp.Core` (v4.5 for `netstandard2.0`, or anything >= `3.1.2.5` / F# 3.1 as present in VS 2012 if you're targeting `net461`).

The tests use [`xUnit.net`](https://github.com/xunit/xunit), [`FSCheck.xUnit`](https://github.com/fscheck/FsCheck), [`Unquote`](https://github.com/SwensenSoftware/unquote) and [`Serilog.Sinks.Seq`](https://github.com/serilog/serilog-sinks-seq) (to view, see https://getseq.net, which provides a free single user license for clearer insight into log traces).

The acceptance tests add a reliance on [`Newtonsoft.Json`](https://github.com/JamesNK/Newtonsoft.Json).

# CONTRIBUTION notes

The goal of CallPolly is to act as a library providing a backbone for applications to apply rich and diverse policies in as Simple a way as possible; [Easiness](https://www.infoq.com/presentations/Simple-Made-Easy) is not the goal. A resilience framework cannot afford to introduce risk to the stability of the system it's trying to _enhance_ the resilience of, so way above normal attention to test coverage of all kinds is a given.

TL;DR while there is a high bar for adding complexity to this library, we're extremely open to making it do the right thing so it works well with any requirement an application's resilience strategy demands _but please open an Issue and have a discussion first to ensure any proposed extension can be merged quickly and smoothly without 😰_.

In service of this, the assumption is that most extensions to CallPolly should live outside the library itself. Based on this, the questions any additional feature needs to answer emphatically to get over the [-100 points for a new feature barrier](https://technet.microsoft.com/en-us/library/dn167709.aspx?f=255&MSPPError=-2147217396) are:
- can a small tweak to Polly enable the goal to be fulfilled and hence benefit lots more users?
- is the facility being proposed general enough to warrant at least exploring figuring out how to use Polly to achieve that end directly?
- has the proposed feature [proved itself broadly applicable](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming)) ?

=> **"can we make CallPolly help you achieve that without making it more complex for everyone else?**

# Building
 ```
# verify the integrity of the repo wrt being able to build/pack/test
./build.ps1
```

# Taster: example policy

Yes, there should be a real README with real examples; we'll get there :sweat_smile:

See the [acceptance tests](https://github.com/jet/CallPolly/blob/master/tests/CallPolly.Acceptance/Scenarios.fs) for behavior implied by this configuration:
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
            { "rule": "Cutoff", "timeoutMs": 1000, "slaMs": 500 }
        ],
        "slow": [
            { "rule": "Cutoff", "timeoutMs": 10000, "slaMs": 5000 }
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
            { "rule": "Limit",  "maxParallel": 10, "maxQueue": 3 }
        ],
        "looser": [
            { "rule": "Limit",  "maxParallel": 100, "maxQueue": 300 }
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
            { "rule": "Limit",  "maxParallel": 2, "maxQueue": 8 },
            { "rule": "Break",  "windowS": 5, "minRequests": 10, "failPct": 20, "breakS": 1 },
            { "rule": "Uri",    "base": "https://upstreamb" },
            { "rule": "Log",    "req": "Always", "res": "Always" }
        ]
    }
},
"upstreamC": {
    "calls": {},
    "defaultPolicy": "default",
    "policies": {
        "default": [
            { "rule": "Limit",  "maxParallel": 10, "maxQueue": 20 },
            { "rule": "LimitBy","maxParallel": 2, "maxQueue": 4, "tag": "clientIp" },
            { "rule": "LimitBy","maxParallel": 2, "maxQueue": 4, "tag": "clientDomain" },
            { "rule": "LimitBy","maxParallel": 2, "maxQueue": 4, "tag": "clientType" }
        ]
    }
}

}
```