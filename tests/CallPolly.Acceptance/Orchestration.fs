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

type SerilogHelpers.LogCaptureBuffer with
    member buffer.Take() =
        let actual = [for x in buffer.Entries -> x.RenderMessage()]
        buffer.Clear()
        actual

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
                    { "rule": "Limit",      "maxParallel": 2, "maxQueue": 8 },
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
    | Composite of Act list
    member this.Execute() = async {
        match this with
        | Succeed ->        return 42
        | ThrowTimeout ->   return raise <| TimeoutException()
        | DelayMs x ->      do! Async.Sleep(ms x)
                            return 43
        | DelayS x ->       do! Async.Sleep(s x)
                            return 43
        | Composite xs ->   for x in xs do
                                do! x.Execute() |> Async.Ignore
                            return 43}

type Sut(log : Serilog.ILogger, policy: CallPolly.Rules.Policy<_,_>) =

    let run serviceName callName f = policy.Find(serviceName, callName).Execute(f)

    let _upstreamA1 (a : Act) = async {
        log.Information("A1")
        return! a.Execute() }
    let upstreamA1 a = run "upstreamA" "Call1" <| _upstreamA1 a

    let _upstreamA2 (a : Act) = async {
        log.Information("A2")
        return! a.Execute() }
    let upstreamA2 a = run "upstreamA" "Call2" <| _upstreamA2 a

    let _upstreamA3 (a : Act) = async {
        log.Information("A3")
        return! a.Execute() }
    let upstreamA3 a = run "upstreamA" "CallBroken" <| _upstreamA3 a

    let _upstreamB1 (a : Act) = async {
        log.Information("B1")
        return! a.Execute() }
    let upstreamB1 a = run "upstreamB" "Call1" <| _upstreamB1 a

    let _apiA a1 a2 = async {
        log.Information "ApiA"
        let! a = upstreamA1 a1
        let! b = upstreamA2 a2
        return a + b
    }
    let _apiB b1 = async {
        log.Information "ApiB"
        return! upstreamB1 b1
    }
    let _apiBroken = upstreamA3 Succeed
    member __.ApiOneSecondSla a1 a2 = run "ingres" "api-a" <| _apiA a1 a2
    member __.ApiTenSecondSla b1 = run "ingres" "api-b" <| _apiB b1

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
    let log, buffer = LogHooks.createLoggerWithCapture output

    let [<Fact>] ``Cutoff can be used to cap call time when upstreams misbehave`` () = async {
        let policy = Parser.parse log policy
        let sut = Sut(log, policy)
        let! time, (Status res) = sut.ApiOneSecondSla Succeed (DelayS 5) |> Async.Catch |> Stopwatch.Time
        test <@ res = 503 && between 1. 1.2 (time.Elapsed.TotalSeconds) @>
    }

    let [<Fact>] ``Upstream timeouts can be mapped to 504s`` () = async {
        let policy = Parser.parse log policy
        let sut = Sut(log, policy)
        let! Status res = sut.ApiTenSecondSla ThrowTimeout |> Async.Catch
        test <@ res = 504 @>
    }

    let [<Fact>] ``Circuit breaking base functionality`` () = async {
        let policy = Parser.parse log policy
        let sut = Sut(log, policy)
        // 10 fails put it into circuit breaking mode - let's do 9 and the step carefully
        let! res = List.replicate 9 (sut.ApiTenSecondSla ThrowTimeout |> Async.Catch) |> Async.Parallel
        test <@ res |> Seq.forall (function Status s -> s = 504) @>

        // Dump any logs we've seen to date
        let callsToUpstreamThatIsTimingOut buffer = buffer |> List.filter (fun s -> s="B1") |> List.length
        let logs = buffer.Take()
        test <@ 9 = callsToUpstreamThatIsTimingOut logs @>

        // The next one brings the minimum count up to the limit and hence will cause circuit breaking
        let! Status res = sut.ApiTenSecondSla ThrowTimeout |> Async.Catch
        test <@ res = 504 @>
        let logs = buffer.Take()
        test <@ 1 = callsToUpstreamThatIsTimingOut logs @>

        // So any further attempts will not even attempt to execute
        let! res = List.replicate 10 (sut.ApiTenSecondSla Succeed |> Async.Catch) |> Async.Parallel
        test <@ res |> Seq.forall (function Status s -> s = 503) @>
        // And we should see much reduced calls getting through
        let logs = buffer.Take()
        test <@ 1 >= callsToUpstreamThatIsTimingOut logs @>
    }

    let [<Fact>] ``Broken circuits can recover and be retriggered`` () = async {
        let policy = Parser.parse log policy
        let sut = Sut(log, policy)
        // 10 fails put it into circuit breaking mode
        let! res = List.replicate 10 (sut.ApiTenSecondSla ThrowTimeout |> Async.Catch) |> Async.Parallel
        test <@ res |> Seq.forall (function Status s -> s = 504) @>

        let callsToUpstreamThatIsTimingOut buffer = buffer |> List.filter (fun s -> s="B1") |> List.length
        let logs = buffer.Take()
        test <@ 10 = callsToUpstreamThatIsTimingOut logs @>
        // Hold off until the 1s elapses so we're back into half-open mode
        do! Async.Sleep (ms 1100)
        // Let one succeed, validating it's closed again
        let! Status res = sut.ApiTenSecondSla Succeed |> Async.Catch
        test <@ 200 = res @>
        // Feed in enough requests to meet the threshold
        let! res = List.replicate 8 (sut.ApiTenSecondSla Succeed |> Async.Catch) |> Async.Parallel
        test <@ res |> Seq.forall (function Status s -> s = 200) @>
        // Because a) we're over the baseline traffic count and b) have a high error rate within the last 5 seconds since the closing
        // three more fails will cause stuff to fail again
        let! res = List.replicate 3 (sut.ApiTenSecondSla ThrowTimeout |> Async.Catch) |> Async.Parallel
        test <@ res |> Seq.forall (function Status s -> s = 504) @>
        // And even though the upstream is happy again, we fail
        let! Status res = sut.ApiTenSecondSla Succeed |> Async.Catch
        test <@ 503 = res @>
    }