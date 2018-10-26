﻿module CallPolly.Acceptance

open CallPolly
open Swensen.Unquote
open System
open Xunit

// shims for F# < 4, can be removed if we stop supporting that
module private Seq =
    let replicate n x = seq { for i in 1..n do yield x }

[<AutoOpen>]
module Helpers =
    let ms x = TimeSpan.FromMilliseconds (float x)
    let s x = TimeSpan.FromSeconds (float x)
    let between min max (value : float) = value >= min && value <= max

    type SerilogHelpers.LogCaptureBuffer with
        member buffer.Take() =
            [ for x in buffer.TakeBatch() -> x.RenderMessage() ]

let policy = """{
    "services": {
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
}
"""

exception BadGatewayException

type Act =
    | Succeed
    | ThrowTimeout
    | BadGateway
    | DelayMs of ms:int
    | DelayS of s:int
    member this.Execute() = async {
        match this with
        | Succeed ->        return 42
        | ThrowTimeout ->   return raise <| TimeoutException()
        | BadGateway ->     return raise BadGatewayException
        | DelayMs x ->      do! Async.Sleep(ms x)
                            return 43
        | DelayS x ->       do! Async.Sleep(s x)
                            return 43 }

type Sut(log : Serilog.ILogger, policy: CallPolly.Rules.Policy<_>) =

    let run serviceName callName f = policy.Find(serviceName, callName).Execute(f)
    let runLog callLog serviceName callName f = policy.Find(serviceName, callName).Execute(f, log=callLog)
    let runWithTags serviceName callName tags f = policy.Find(serviceName, callName).Execute(f, tags=tags)

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

    let _apiA a1 a2 = async {
        log.Information "ApiA"
        let! a = upstreamA1 a1
        let! b = upstreamA2 a2
        return a + b
    }

    let _apiABroken = upstreamA3 Succeed

    let _upstreamB1 (a : Act) = async {
        log.Information("B1")
        return! a.Execute() }
    let upstreamB1 a = run "upstreamB" "Call1" <| _upstreamB1 a

    let _apiB (a : Act) = async {
        log.Information "ApiB"
        return! upstreamB1 a
    }

    let _upstreamC1 (a : Act) = async {
        log.Information("C1")
        return! a.Execute() }
    let upstreamC1 tags a = runWithTags "upstreamC" "Call1" tags <| _upstreamC1 a

    let _apiC tags (a : Act) = async {
        log.Information "ApiC"
        return! upstreamC1 tags a
    }

    member __.ApiOneSecondSla a1 a2 = run "ingres" "api-a" <| _apiA a1 a2
    member __.ApiOneSecondSlaLog callLog a1 a2 = runLog callLog "ingres" "api-a" <| _apiA a1 a2

    member __.ApiTenSecondSla b1 = run "ingres" "api-b" <| _apiB b1

    member __.ApiManualBroken = _apiABroken

    member __.ApiMulti tags (a : Act) =
        let tags = tags |> dict |> System.Collections.ObjectModel.ReadOnlyDictionary
        run "ingres" "api-a" <| _apiC tags a

let (|Http200|Http500|Http502|Http503|Http504|) : Choice<int,exn> -> _ = function
    | Choice1Of2 _ -> Http200
    | Choice2Of2 (:? Polly.ExecutionRejectedException) -> Http503
    | Choice2Of2 (:? BadGatewayException) -> Http502
    | Choice2Of2 (:? TimeoutException) -> Http504
    | Choice2Of2 _ -> Http500

let (|Status|) : Choice<int,exn> -> int = function
    | Http200 -> 200
    | Http500 -> 500
    | Http502 -> 502
    | Http503 -> 503
    | Http504 -> 504

/// Acceptance tests illustrating the intended use of CallPolly wrt implementing flow control within an orchestration layer
type Scenarios(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let [<Fact>] ``Cutoff - can be used to cap call time when upstreams misbehave`` () = async {
        let policy = Parser.parse(policy).CreatePolicy log
        let sut = Sut(log, policy)
        let! time, (Status res) = sut.ApiOneSecondSla Succeed (DelayS 5) |> Async.Catch |> Stopwatch.Time
        let entries = buffer.Take()
        test <@ res = 503
                && between 0.9 2. (let t = time.Elapsed in t.TotalSeconds)
                && between 4. 5. (float entries.Length) @> } // 1 api call, 2 call log entries, 1 cutoff event, maybe 1 delayed event

    let [<Fact>] ``CallLog - Can capture call-specific log entries isolated from overall log`` () = async {
        let callLog, callBuffer = LogHooks.createLoggerWithCapture output
        let policy = Parser.parse(policy).CreatePolicy log
        let sut = Sut(log, policy)
        let! time, (Status res) = sut.ApiOneSecondSlaLog callLog Succeed (DelayS 5) |> Async.Catch |> Stopwatch.Time
        test <@ res = 503 && between 0.9 2. (let t = time.Elapsed in t.TotalSeconds) @>
        let callEntries, statEntries = callBuffer.Take(), buffer.Take()
        test <@ between 0.9 2. (float callEntries.Length) // 1 cutoff event, maybe 1 delayed event
                && 3 = statEntries.Length @> } // 1 api call, 2 call log entries

    let [<Fact>] ``Trapping - Arbitrary Polly expressions can be used to define a failure condition`` () = async {
        let selectPolicy (cfg: CallPolly.Rules.CallConfig<Config.Http.Configuration>) =
            Polly.Policy
                .Handle<TimeoutException>()
                .Or<BadGatewayException>(fun _e ->
                    cfg.config.EffectiveUri |> Option.exists (fun (u : Uri) -> (string u).Contains "upstreamb")
                    && cfg.config.reqLog = Config.Http.LogLevel.Always)
        let policy = Parser.parse(policy).CreatePolicy(log, selectPolicy)
        let sut = Sut(log, policy)
        let! r = Seq.replicate 9 Succeed |> Seq.map sut.ApiTenSecondSla |> Async.Parallel
        test <@ r |> Array.forall ((=) 42) @>
        for x in 1..2 do
            let! Status res = sut.ApiTenSecondSla BadGateway |> Async.Catch
            test <@ res = 502 @>
        let! Status res = sut.ApiTenSecondSla ThrowTimeout |> Async.Catch
        test <@ res = 504 @>
        let! Status res = sut.ApiTenSecondSla Succeed |> Async.Catch
        test <@ res = 503 @>
    }

    let [<Fact>] ``Propagation - Upstream timeouts can be mapped to 504s`` () = async {
        let policy = Parser.parse(policy).CreatePolicy log
        let sut = Sut(log, policy)
        let! Status res = sut.ApiTenSecondSla ThrowTimeout |> Async.Catch
        test <@ res = 504 @>
    }

    let [<Fact>] ``Isolate - Manual circuit-breaking`` () = async {
        let policy = Parser.parse(policy).CreatePolicy log
        let sut = Sut(log, policy)
        let! Status res = sut.ApiManualBroken |> Async.Catch
        test <@ res = 503 @>
    }

    let [<Fact>] ``Break - Circuit breaking base functionality`` () = async {
        let policy = Parser.parse(policy).CreatePolicy log
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

    let [<Fact>] ``Break - Broken circuits can recover and be retriggered`` () = async {
        let policy = Parser.parse(policy).CreatePolicy log
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

    let [<Fact>] ``Limit - Bulkhead functionality`` () = async {
        let policy = Parser.parse(policy).CreatePolicy log
        let sut = Sut(log, policy)
        let alternateBetweenTwoUpstreams i =
            if i % 2 = 0 then sut.ApiOneSecondSla Succeed (DelayMs 100)
            else sut.ApiOneSecondSla (DelayMs 1000) Succeed
            |> Async.Catch
        let! time, res = List.init 1000 alternateBetweenTwoUpstreams |> Async.Parallel |> Stopwatch.Time
        let counts = res |> Seq.countBy (|Status|) |> Seq.sortBy fst |> List.ofSeq
        test <@ match counts with
                | [200,successCount; 503,rejectCount] -> successCount < 150 && rejectCount >= 850
                | x -> failwithf "%A" x @>
        test <@ between 0.3 2.5 (let t = time.Elapsed in t.TotalSeconds) @>
    }

    let [<Fact>] ``LimitBy - BulkheadMulti functionality`` () = async {
        let policy = Parser.parse(policy).CreatePolicy log
        let sut = Sut(log, policy)
        let act i =
            match i % 3 with
            | 0 -> sut.ApiMulti ["clientIp","A"] (DelayMs 100)
            | 1 -> sut.ApiMulti ["clientDomain","A"] (DelayMs 100)
            | _ -> sut.ApiMulti ["clientType","A"] (DelayMs 100)
            |> Async.Catch
        let! time, res = List.init 1000 act |> Async.Parallel |> Stopwatch.Time
        let counts = res |> Seq.countBy (|Status|) |> Seq.sortBy fst |> List.ofSeq
        test <@ match counts with
                | [200,successCount; 503,rejectCount] -> successCount < 40 && rejectCount >= 960
                | x -> failwithf "%A" x @>
        test <@ between 0.3 2.5 (let t = time.Elapsed in t.TotalSeconds) @>
    }

    let readDefinitions () =
        let filename = System.Environment.GetEnvironmentVariable "CALL_POLICY"
        // Uncomment as a test hack!
        //let filename = "c:\code\CALL_POLICIES"
        if filename <> null && System.IO.File.Exists filename then System.IO.File.ReadAllText filename
        else policy

    let renderAsPrettyJson x = Parser.Newtonsoft.Serialize(x, Parser.Newtonsoft.indentSettings)

    let [<Fact>] ``DumpState - Pretty print internal state dump for diagnostics using existing converters``() =
        let res = Context.Create(log, readDefinitions)
        // This should not result in any processing, but we run it as a sanity check
        // the normal use is to periodically trigger a check for new policies by running it
        for x in 1..1(*Int32.MaxValue*) do // Poor person's demo / REPL rig for watching what real changes get logged as
            if x > 1 then System.Threading.Thread.Sleep 1000
            res.CheckForChanges()
            //let w = Parser.parse(readDefinitions()).Warnings
            //if w.Length > 0 then log.Information("Warnings... {msgs}", w |> renderAsPrettyJson)
        res.DumpInternalState() |> renderAsPrettyJson |> output.WriteLine

    let [<Fact>] ``DumpWarnings - Pretty print internal state dump for diagnostics using existing converters``() =
        let source = readDefinitions ()
        Parser.parse(source).Warnings |> renderAsPrettyJson |> output.WriteLine