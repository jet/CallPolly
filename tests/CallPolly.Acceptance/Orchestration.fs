module CallPolly.Acceptance.Tests

open CallPolly
open Swensen.Unquote
open System
open Xunit

[<AutoOpen>]
module Helpers =
    let ms x = TimeSpan.FromMilliseconds (float x)
    let s x = TimeSpan.FromSeconds (float x)
    let between min max (value : float) = value >= min && value <= max

let policy = """
{
    "services": {
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
                "Call1": "default",
                "Call2": "default",
                "CallBroken": "defaultBroken"
            },
            "defaultPolicy": null,
            "policies": {
                "default": [
                    { "rule": "Limit",      "maxParallel": 1, "maxQueue": 3 }
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
                    { "rule": "Limit",      "maxParallel": 2, "maxQueue": 0 },
                    { "rule": "Break",      "windowS": 5, "minRequests": 10, "failPct": 20, "breakS": 1 }
                ]
            }
        }
    }
}
"""

type Act =
    | Succeed
    | ThrowTimeout
    | DelayMs of ms:int
    | DelayS of s:int
    member this.Execute() = async {
        match this with
        | Succeed ->    return 42
        | ThrowTimeout -> return raise <| TimeoutException()
        | DelayMs x ->  do! Async.Sleep(ms x)
                        return 43
        | DelayS x ->   do! Async.Sleep(s x)
                        return 43 }

type Sut(log : Serilog.ILogger, policy: CallPolly.Rules.Policy<_,_>) =

    let run serviceName callName f = policy.Find(serviceName, callName).Execute(f)

    let _upstreamA1 (a : Act) =
        log.Information("A1")
        a.Execute()
    let upstreamA1 d = run "upstreamA" "Call1" <| _upstreamA1 d

    let _upstreamA2 (a : Act) =
        log.Information("A2")
        a.Execute()
    let upstreamA2 d = run "upstreamA" "Call2" <| _upstreamA2 d

    let _upstreamA3 (a : Act) =
        log.Information("A3")
        a.Execute()
    let upstreamA3 d = run "upstreamA" "CallBroken" <| _upstreamA3 d

    let _upstreamB1 (a : Act) =
        log.Information("B1")
        a.Execute()
    let upstreamB1 d = run "upstreamB" "Call1" <| _upstreamB1 d

    let _apiA a1 a2 = async {
        let! a = upstreamA1 a1
        let! b = upstreamA2 a2
        return a + b
    }

    let _apiBroken = upstreamA3 Succeed

    let _apiB b1 = upstreamB1 b1
    member __.ApiQuick a1 a2 = run "ingres" "api-a" <| _apiA a1 a2
    member __.ApiSlow b1 = run "ingres" "api-b" <| _apiB b1

let (|Http200|Http500|Http503|Http504|) : Choice<int,exn> -> _ = function
    | Choice1Of2 _ -> Http200
    | Choice2Of2 (:? Polly.ExecutionRejectedException) -> Http503
    | Choice2Of2 (:? TimeoutException) -> Http504
    | Choice2Of2 _ -> Http500

let (|Status|) : Choice<int,exn> -> int = function
    | Http200 -> 200
    | Http500 -> 500
    | Http503 -> 503
    | Http504 -> 504

/// Acceptance tests illustrating the indended use of CallPolly wrt implementing flow control within an orchestration layer
type Orchestration(output : Xunit.Abstractions.ITestOutputHelper) =
    let log = LogHooks.createLogger output

    let [<Fact>] ``Cutoff can be used to cap call time when upstreams misbehave`` () = async {
        let policy = Parser.parse log policy
        let sut = Sut(log, policy)
        let! time, (Status res) = sut.ApiQuick Succeed (DelayS 5) |> Async.Catch |> Stopwatch.Time
        test <@ res = 503 && between 1. 1.2 (time.Elapsed.TotalSeconds) @>
    }