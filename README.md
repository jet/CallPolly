# CallPolly

[Polly](https://github.com/App-vNext/Polly) is a focused library _and a rich set of documentation, samples and discussions_ around various resilience patterns for .NET.

Look no further for a mature, well thought out set of abstractions with a responsive community.

----

**_Presently under heavy development, please excuse the lack of roadmap or proper examples for bit._**

CallPolly wraps Polly to provide a:
- parsing and validation of a suite of rules in a declarative form (presently json, but being able to maintain the inputs in YAML is on the wishlist)
- a ruleset interpreter that applies the rules in a consistent fashion

Using CallPolly will _eventually_ enable one to:
- wrap calls with minimal invasive code changes
- use [Open Tracing](http://opentracing.io/)-based Distributed Tracing systems and dashboards to analyze the effects of the configured Call Policy as applied
- iteratively adjust those policies across a set of systems