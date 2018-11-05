﻿(*  NB much of this test suite has been moved and/or cloned into a more focused tests in PolicyTests.fs - in line with general test pyramid thinking, that's where as many tests as possible _should_ live.
    This set of tests also preceded the implementation of Scenarios.fs, which to a large degree covers the need to validate from a service policy json
      through to a call having the correct policy behaviors applied, which should remain a higher-level end to end set of tests.
    Therefore, where coverage of a piece of logic is already addressed in either place, it's valid to remove it from here, with moving it to RulesTests being the preferred home unless it really is an end-to-end testing scenario *)
namespace CallPoly.Tests.Integration

open CallPolly
open System
open Swensen.Unquote
open Xunit

[<AutoOpen>]
module Helpers =
    let ms x = TimeSpan.FromMilliseconds (float x)
    let s x = TimeSpan.FromSeconds (float x)
    let between min max (value : int) = value >= min && value <= max
    let hasChanged (serviceName : string) (call:string) (changeLevel: ChangeLevel) updated =
        updated |> List.exists (function sn,changes -> sn = serviceName && changes |> List.contains (call,changeLevel))

module Core =
    let defs = """{ "services": { "default": {
        "calls": {
            "(defaultLog)": "defaultLog",
            "checkout": "heavy",
            "placeOrder": "heavy",
            "EstimatedDeliveryDates": "edd",
            "EstimatedDeliveryDate": "defaultBroken"
        },
        "defaultPolicy": "default",
        "policies": {
            "defaultLog": [
                { "rule": "Log",    "req": "OnlyWhenDebugEnabled", "res": "OnlyWhenDebugEnabled" }
            ],
            "default": [
                { "rule": "Include","policy": "defaultLog" },
                { "rule": "Sla",    "slaMs": 1000, "timeoutMs": 5000 }
            ],
            "edd": [
                { "rule": "Uri",    "base": "http://base/" },
                { "rule": "Include","policy": "default" }
                ],
            "limit": [
                { "rule": "Limit",  "maxParallel": 2, "maxQueue": 3, "dryRun": true }
            ],
            "limitBy": [
                { "rule": "LimitBy","maxParallel": 3, "maxQueue": 2, "tag": "clientIp", "dryRun": true }
            ],
            "break": [
                { "rule": "Break",  "windowS": 5, "minRequests": 100, "failPct": 20, "breakS": 1 }
            ],
            "defaultBroken": [
                { "rule": "Isolate" }
            ],
            "cutoff": [
                { "rule": "Cutoff", "timeoutMs": 500, "dryRun": true }
            ],
            "heavy": [
                { "rule": "Include","policy": "defaultLog" },
                { "rule": "Include","policy": "limit" },
                { "rule": "Include","policy": "limitBy" },
                { "rule": "Include","policy": "break" },
                { "rule": "Include","policy": "cutoff" },
                { "rule": "Sla",    "slaMs": 5000, "timeoutMs": 10000 },
                { "rule": "Include","policy": "defaultBroken" }
            ],
            "UnknownUnknown": [
                { "rule": "NotYetImplemented" }
            ]
        }
}}}"""

    let mkPolicy value = Parser.ParsedRule.Policy value

    let limitParsed = mkPolicy <| Config.Policy.Input.Value.Limit { maxParallel=2; maxQueue=3; dryRun=Some true }
    let limitConfig : Governor.BulkheadConfig = { dop=2; queue=3; dryRun=true }

    let limitByParsed = mkPolicy <| Config.Policy.Input.Value.LimitBy { tag="clientIp"; maxParallel=3; maxQueue=2 }
    let limitByConfig : Governor.TaggedBulkheadConfig = { tag="clientIp"; dop=3; queue=2  }

    let breakParsed = mkPolicy <| Config.Policy.Input.Value.Break { windowS = 5; minRequests = 100; failPct=20.; breakS = 1.; dryRun = None }
    let breakConfig : Governor.BreakerConfig = { window = s 5; minThroughput = 100; errorRateThreshold = 0.2; retryAfter = s 1; dryRun = false }

    let cutoffParsed = mkPolicy <| Config.Policy.Input.Value.Cutoff { timeoutMs = 500; slaMs = None; dryRun = Some true }
    let cutoffConfig : Governor.CutoffConfig = { timeout = ms 500; sla = None; dryRun = true }

    let isolateParsed = mkPolicy <| Config.Policy.Input.Value.Isolate

    let mkHttp value = Parser.ParsedRule.Http value
    let baseUri = Uri "http://base"
    let baseParsed = mkHttp <| Config.Http.Input.Value.Uri { ``base``=Some (string baseUri); path=None }
    let logParsed = mkHttp <| Config.Http.Input.Value.Log { req=Config.Http.Input.LogLevel.OnlyWhenDebugEnabled; res=Config.Http.Input.LogLevel.OnlyWhenDebugEnabled }

    let defConfig : Config.Http.Configuration =
        {   timeout = Some (s 5); sla = Some (s 1)
            ``base`` = Some baseUri; rel = None
            reqLog = Config.Http.LogLevel.OnlyWhenDebugEnabled; resLog = Config.Http.LogLevel.OnlyWhenDebugEnabled }
    let noPolicy : Governor.PolicyConfig = { isolate = false; cutoff = None; limit = None; taggedLimits = []; breaker = None; }
    let heavyConfig : Config.Http.Configuration =
        {   timeout = Some (s 10); sla = Some (s 5)
            ``base`` = None; rel = None
            reqLog = Config.Http.LogLevel.OnlyWhenDebugEnabled; resLog = Config.Http.LogLevel.OnlyWhenDebugEnabled }
    let heavyRules : Governor.PolicyConfig = { isolate = true; cutoff = Some cutoffConfig; limit = Some limitConfig; taggedLimits = [limitByConfig]; breaker = Some breakConfig }
    let mkParsedSla sla timeout = Parser.ParsedRule.Http (Config.Http.Input.Value.Sla {slaMs = sla; timeoutMs = timeout })

    /// Base tests exercising core functionality
    type Core(output : Xunit.Abstractions.ITestOutputHelper) =
        let log = LogHooks.createLogger output

        let [<Fact>] ``Policy TryFind happy path`` () =
            let res = Parser.parse(defs)
            let pol = res.CreatePolicy(log)
            let tryFindActionRules call = pol.TryFind("default",call)
            let edd = trap <@ tryFindActionRules "EstimatedDeliveryDates" |> Option.get @>
            let findRaw call = res.Raw.TryFind("default").Value.TryFind(call).Value
            let eddRaw = findRaw "EstimatedDeliveryDates"
            test <@ [baseParsed; logParsed; mkParsedSla 1000 5000] = eddRaw
                    && defConfig = edd.Config
                    && noPolicy = edd.Policy @>
            let poRaw = findRaw "placeOrder"
            let po = trap <@ tryFindActionRules "placeOrder" |> Option.get @>
            test <@ [logParsed; limitParsed; limitByParsed; breakParsed; cutoffParsed; mkParsedSla 5000 10000; isolateParsed] = poRaw
                    && heavyConfig = po.Config
                    && heavyRules = po.Policy @>
            test <@ None = tryFindActionRules "missing" @>

        let [<Fact>] ``Policy Find Happy path`` () =
            let res = Parser.parse(defs)
            let pol = res.CreatePolicy(log)
            let findRaw call = res.Raw.TryFind("default").Value.TryFind(call).Value
            let defaultLog = findRaw "(defaultLog)"
            let default_ = findRaw "(default)"
            test <@ baseParsed :: default_ = findRaw "EstimatedDeliveryDates" @>
            test <@ [isolateParsed] = findRaw "EstimatedDeliveryDate" @>
            test <@ defaultLog @ [limitParsed; limitByParsed; breakParsed; cutoffParsed; mkParsedSla 5000 10000; isolateParsed] = findRaw "placeOrder" @>
            let heavyPolicy = pol.Find("default","placeOrder")
            test <@ heavyPolicy.Policy.isolate
                    && Some breakConfig = heavyPolicy.Policy.breaker @>

        let [<Fact>] ``UpstreamPolicy Update management`` () =
            let pol = Parser.parse(defs).CreatePolicy log
            let heavyPolicy = pol.Find("default","placeOrder")
            test <@ heavyPolicy.Policy.isolate
                    && Some breakConfig = heavyPolicy.Policy.breaker @>
            let updated =
                let polsWithDifferentAdddress = defs.Replace("http://base","http://base2")
                Parser.parse(polsWithDifferentAdddress).UpdatePolicy(pol) |> List.ofSeq
            test <@ updated |> hasChanged "default" "EstimatedDeliveryDates" ChangeLevel.Configuration
                    && heavyPolicy.Policy.isolate @>
            let updated =
                let polsWithIsolateMangledAndBackToOriginalBaseAddress = defs.Replace("Isolate","isolate")
                Parser.parse(polsWithIsolateMangledAndBackToOriginalBaseAddress).UpdatePolicy pol |> List.ofSeq
            test <@ updated |> hasChanged "default" "EstimatedDeliveryDates" ChangeLevel.Configuration
                    && updated |> hasChanged "default" "placeOrder" ChangeLevel.Policy
                    && not heavyPolicy.Policy.isolate @>

type Isolate(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let expectedLimitRules : Governor.BulkheadConfig = { dop = 2; queue = 3; dryRun = true }

    let [<Fact>] ``takes precedence over, but does not conceal Break, Limit or Timeout; logging only reflects Isolate rule`` () = async {
        let res = Parser.parse Core.defs
        let pol = res.CreatePolicy log
        let ap = pol.Find("default","placeOrder")
        test <@ ap.Policy.isolate
                && Some Core.breakConfig = ap.Policy.breaker
                && Some Core.cutoffConfig = ap.Policy.cutoff
                && Some expectedLimitRules = ap.Policy.limit @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("default","placeOrder","heavy")] =! buffer.Take()
        let updated =
            let polsWithIsolateMangled = Core.defs.Replace("Isolate","isolate")
            Parser.parse(polsWithIsolateMangled).UpdatePolicy pol |> List.ofSeq
        test <@ updated |> hasChanged "default" "placeOrder" ChangeLevel.Policy
                && not ap.Policy.isolate @>
        let! result = ap.Execute(call) |> Async.Catch
        test <@ Choice1Of2 42 = result @>
        [] =! buffer.Take() }

    let isolateDefs = """{ "services": { "default": {
        "calls": {
            "nonDefault": "def"
        },
        "defaultPolicy": "def",
        "policies": {
            "def": [
                { "rule": "Isolate" }
            ]
        }
}}}"""

    let [<Fact>] ``When on, throws and logs information`` () = async {
        let pol = Parser.parse(isolateDefs).CreatePolicy log
        let ap = pol.Find("default","notFound")
        test <@ ap.Policy.isolate && None = ap.Policy.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("default","(default)","def")] =! buffer.Take() }

    let [<Fact>] ``when removed, stops intercepting processing, does not log`` () = async {
        let pol = Parser.parse(isolateDefs).CreatePolicy log
        let ap = pol.Find("default","nonDefault")
        test <@ ap.Policy.isolate && None = ap.Policy.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("default","nonDefault","def")] =! buffer.Take()
        let updated =
            let polsWithIsolateMangled = isolateDefs.Replace("Isolate","isolate")
            Parser.parse(polsWithIsolateMangled).UpdatePolicy pol |> List.ofSeq
        test <@ updated |> hasChanged "default" "nonDefault" ChangeLevel.Policy
                && not ap.Policy.isolate @>
        let! result = ap.Execute(call) |> Async.Catch
        test <@ Choice1Of2 42 = result @>
        [] =! buffer.Take() }

type Break(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let defs = """{ "services": { "default": {
        "calls": {},
        "defaultPolicy": "def",
        "policies": {
            "def": [
                { "rule": "Break", "windowS": 10, "minRequests": 2, "failPct": 50, "breakS": 1.0, "dryRun": false }
            ]
        }
}}}"""
    let dryRunDefs = defs.Replace("false","true")

    let expectedRules : Governor.BreakerConfig = { window = s 10; minThroughput = 2; errorRateThreshold = 0.5; retryAfter = TimeSpan.FromSeconds 1.; dryRun = false }

    let [<Fact>] ``applies break constraints, logging each application and status changes appropriately`` () = async {
        let pol = Parser.parse(defs).CreatePolicy log
        let ap = pol.Find("default","notFound")
        test <@ not ap.Policy.isolate
                && Some expectedRules = ap.Policy.breaker @>
        let runTimeout = async {
            let timeout = async { return raise <| TimeoutException() }
            let! result = ap.Execute(timeout) |> Async.Catch
            let ex = trap <@ match result with Choice2Of2 x -> x | x -> failwithf "Unexpected %A" x @>
            test <@ ex :? TimeoutException @> }
        let shouldBeOpen = async {
            let fail = async { return failwith "Unexpected" }
            let! result = ap.Execute(fail) |> Async.Catch
            test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.BrokenCircuitException) -> true | _ -> false @> }
        let runSuccess = async {
            let success = async { return 42 }
            let! result = ap.Execute(success)
            42 =! result }
        do! runSuccess
        do! runSuccess
        Serilog.Log.Warning("P1")
        do! runTimeout
        Serilog.Log.Warning("P2")
        do! runTimeout
        Serilog.Log.Warning("P3")
        [Call ("(default)",Breaking)] =! buffer.Take()
        do! shouldBeOpen
        [Broken ("default","(default)","def")] =! buffer.Take()
        // Waiting for 1s should have transitioned it to HalfOpen
        do! Async.Sleep (ms 1100) // 1s + 100ms fudge factor
        Serilog.Log.Warning("P4")
        do! runSuccess
        [Status ("(default)",Pending); Status ("(default)",Resetting)] =! buffer.Take()
        do! runTimeout
        [Call ("(default)",Breaking)] =! buffer.Take()
        // Changing the rules should replace the breaker with a fresh instance which has forgotten the state
        let changedRules = defs.Replace("1.0","1.1")
        Parser.parse(changedRules).UpdatePolicy pol |> ignore
        Serilog.Log.Warning("P5")
        do! runSuccess }

    let [<Fact>] ``dryRun mode prevents breaking, logs status changes appropriately`` () = async {
        let pol = Parser.parse(dryRunDefs).CreatePolicy(log)
        let ap = pol.Find("default","notFound")
        test <@ not ap.Policy.isolate
                && Some { expectedRules with dryRun = true } = ap.Policy.breaker @>
        let runTimeout = async {
            let timeout = async { return raise <| TimeoutException() }
            let! result = ap.Execute(timeout) |> Async.Catch
            test <@ match result with Choice2Of2 (:? TimeoutException) -> true | _ -> false @> }
        let runSuccess = async {
            let success = async { return 42 }
            let! result = ap.Execute(success)
            42 =! result }
        do! runSuccess
        do! runTimeout
        // 1/2 running error rate -> would break
        [Call ("(default)",BreakingDryRun)] =! buffer.Take()
        do! runSuccess
        do! runTimeout
        // 2/4 failed -> would break
        [Call ("(default)",BreakingDryRun)] =! buffer.Take()
        do! runSuccess // 2/5 failed
        do! runTimeout // 3/6 failed -> break
        [Call ("(default)",BreakingDryRun)] =! buffer.Take()
        // Changing the rules should replace the breaker with a fresh instance which has forgotten the state
        let changedRules = defs.Replace("1.0","5")
        Parser.parse(changedRules).UpdatePolicy pol |> ignore
        do! runSuccess
        do! runSuccess
        // 1/2 failed (if the timeout happened in between the two successes, it would fail)
        do! runTimeout
        [] =! buffer.Take() }

type Limit(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let defs = """{ "services": { "default": {
        "calls": {},
        "defaultPolicy": "def",
        "policies": {
            "def": [
                { "rule": "Limit", "maxParallel": 2, "maxQueue": 3, "dryRun": false }
            ]
        }
}}}"""
    let dryRunDefs = defs.Replace("false","true")
    let expectedRules : Governor.BulkheadConfig = { dop = 2; queue = 3; dryRun = false }

    let runError = async {
        do! Async.Sleep (ms 1000)
        return raise (System.TimeoutException()) }
    let runOk = async {
        do! Async.Sleep (ms 1000)
        return 42 }

    let [<Fact>] ``dryRun mode does not inhibit processing`` () = async {
        let pol = Parser.parse(dryRunDefs).CreatePolicy(log)
        let ap = pol.Find("default","any")
        test <@ not ap.Policy.isolate
                && Some { expectedRules with dryRun = true } = ap.Policy.limit @>
        let! time, results =
            seq { for x in 0..5 -> if x % 2 = 0 then runError else runOk }
            |> Seq.mapi (fun i f -> async {
                // Stagger the starts - the dryRun mode does not force any waiting so wait before we ask for the start so we
                // get it into a state where at least 1 start shows queuing would normally take place
                do! Async.Sleep(ms (200 * i))
                // Catch inside so the first throw doesnt cancel the overall execution
                return! ap.Execute f |> Async.Catch })
            |> Async.Parallel
            |> Stopwatch.Time
        let oks, errs = Choice.partition results
        test <@ 3 = Array.length oks
                && time.Elapsed < s 4 // 1000ms*2+5*200+ 1000ms fudge factor
                && 3 = Array.length errs
                && errs |> Seq.forall (fun x -> x.GetType() = typedefof<TimeoutException>) @>
        let evnts = buffer.Take()
        let queuedOrShed = function
            | Call ("(default)",QueuingDryRun) as x -> Choice1Of2 x
            | Call ("(default)",SheddingDryRun) as x -> Choice2Of2 x
            | x -> failwithf "Unexpected event %A" x
        let queued,shed = evnts |> Seq.map queuedOrShed |> Choice.partition
        test <@ 0 <= Array.length queued // should be = 3, but if there's a delay spinning up threads, sometimes fewer get queued
                && 1 = Array.length shed @> }

    let [<Fact>] ``when active, sheds load above limit`` () = async {
        let pol = Parser.parse(defs).CreatePolicy log
        let ap = pol.Find("default","any")
        test <@ not ap.Policy.isolate && Some expectedRules = ap.Policy.limit @>
        let! time, results =
            Seq.replicate 6 runOk
            // Catch inside so the first throw doesnt cancel the overall execution
            |> Seq.map(ap.Execute >> Async.Catch)
            |> Async.Parallel
            |> Stopwatch.Time
        let oks, errs = Choice.partition results
        test <@ 5 = Array.length oks
                && time.Elapsed > s 2 && time.Elapsed < s 5 // 1500ms*2+5*200+ 1000ms fudge factor
                && 1 = Array.length errs
                && match Seq.exactlyOne errs with :? Polly.Bulkhead.BulkheadRejectedException -> true | _ -> false @>
        let evnts = buffer.Take()
        let queuedOrShed = function
            | Call ("(default)",MaybeQueuing) as x -> Choice1Of3 x
            | Deferred ("default","(default)","def",[], delay) as x -> Choice2Of3 delay
            | Shed ("default","(default)","def",[]) as x -> Choice3Of3 x
            | x -> failwithf "Unexpected event %A" x
        let queued,waited,shed = evnts |> Seq.map queuedOrShed |> Choice.partition3
        let delayed = waited |> Array.filter (fun x -> x > ms 500)
        // while we'll be pretty clear about shedding, we might discard some queuing notifications depending on the scheduling, and the shed one can also look like it will be queued
        test <@ 4 >= Array.length queued
                && between 2 3 (Array.length delayed) // Typically, 3 should get delayed, but depending on scheduling, only 2 get logged as such, and we don't want a flickering test
                && 1 = Array.length shed @> }

type LimitBy(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let limitByOnly = """{ "services": { "default": {
        "calls": {},
        "defaultPolicy": "def",
        "policies": {
            "def": [
                { "rule": "LimitBy", "maxParallel": 2, "maxQueue": 3, "tag": "clientIp" },
                { "rule": "LimitBy", "maxParallel": 2, "maxQueue": 3, "tag": "clientDomain" }
            ]
        }
}}}"""
    let expectedRule : Governor.TaggedBulkheadConfig = { dop = 2; queue = 3; tag="clientIp" }
    let expectedDomainRule : Governor.TaggedBulkheadConfig = { dop = 2; queue = 3; tag="clientDomain" }

    let runOk = async {
        do! Async.Sleep (s 1)
        return 42 }

    let [<Fact>] ``Enforces queueing and sheds load above limit whenever there is contention on a tag`` () = async {
        let pol = Parser.parse(limitByOnly).CreatePolicy log
        let ap = pol.Find("default","any")
        test <@ [expectedRule; expectedDomainRule] = ap.Policy.taggedLimits @>
        let contending =
            let alternateDomainVsIpContention i =
                match i % 2 with
                | 0 -> ["clientIp","A"]
                | _ -> ["clientIp",string i; "clientDomain","B"]
            Seq.replicate 12 runOk
            |> Seq.zip (Seq.init 12 alternateDomainVsIpContention)
        let notContending =
            let alternateUnderRadar i =
                match i % 3 with
                | 0 -> "C"
                | 1 -> "D"
                | _ -> "E"
            Seq.replicate 12 runOk
            |> Seq.zip (Seq.init 12 alternateUnderRadar |> Seq.map (fun k -> ["clientIp",k]))
        let mkTags tags =
            tags |> dict |> System.Collections.ObjectModel.ReadOnlyDictionary
        let! time, results =
            Seq.append notContending contending
            // Catch inside so the first throw doesnt cancel the overall execution
            |> Seq.map(fun (tags, op) -> ap.Execute(op, tags=mkTags tags) |> Async.Catch)
            |> Async.Parallel
            |> Stopwatch.Time
        let oks, errs = Choice.partition results
        test <@ 10+12 = Array.length oks
                && time.Elapsed > s 2 && time.Elapsed < s 6
                && 2 = Array.length errs
                && errs |> Array.forall (function :? Polly.Bulkhead.BulkheadRejectedException -> true | _ -> false) @>
        let evnts = buffer.Take()
        let shed = function
            | Shed ("default","(default)","def",tags) -> Some tags
            | x -> failwithf "Unexpected event %A" x
        let shed = evnts |> Seq.choose shed |> Array.ofSeq
        test <@ 2 = Array.length shed
                && shed |> Array.exists (List.contains ("clientIp","A"))
                && shed |> Array.exists (List.contains ("clientDomain","B")) @> }

    let limitWithLimitBy = """{ "services": { "default": {
        "calls": {},
        "defaultPolicy": "def",
        "policies": {
            "def": [
                { "rule": "Limit", "maxParallel": 24, "maxQueue": 0 },
                { "rule": "LimitBy", "maxParallel": 2, "maxQueue": 3, "tag": "clientIp" }
            ]
        }
}}}"""

    let [<Fact>] ``When allied with a limit rule, sheds load above limit, and provides clear logging`` () = async {
        let pol = Parser.parse(limitWithLimitBy).CreatePolicy log
        let ap = pol.Find("default","any")
        test <@ [expectedRule] = ap.Policy.taggedLimits @>
        let contending =
            let alternateIps i =
                match i % 2 with
                | 0 -> "A"
                | _ -> "B"
            Seq.replicate 12 runOk
            |> Seq.zip (Seq.init 12 alternateIps)
        let notContending =
            let alternateUnderRadar i =
                match i % 3 with
                | 0 -> "C"
                | 1 -> "D"
                | _ -> "E"
            Seq.replicate 12 runOk
            |> Seq.zip (Seq.init 12 alternateUnderRadar)
        let mkTags tags =
            tags |> dict |> System.Collections.ObjectModel.ReadOnlyDictionary
        let! time, results =
            Seq.append notContending contending
            // Catch inside so the first throw doesnt cancel the overall execution
            |> Seq.map(fun (ip, op) -> ap.Execute(op, tags=mkTags ["clientIp",ip]) |> Async.Catch)
            |> Async.Parallel
            |> Stopwatch.Time
        let oks, errs = Choice.partition results
        test <@ 10+12 = Array.length oks
                && time.Elapsed > s 2 && time.Elapsed < s 6
                && 2 = Array.length errs
                && errs |> Array.forall (function :? Polly.Bulkhead.BulkheadRejectedException -> true | _ -> false) @>
        let evnts = buffer.Take()
        let queuedOrShed = function
            | Call ("(default)",MaybeQueuing) as x -> Choice1Of3 x
            | Deferred ("default","(default)","def",[tag],delay) as x -> Choice2Of3 (tag,delay)
            | Shed ("default","(default)","def",[tag]) as x -> Choice3Of3 (tag,x)
            | x -> failwithf "Unexpected event %A" x
        let queued,waited,shed = evnts |> Seq.map queuedOrShed |> Choice.partition3
        let delayed = waited |> Array.filter (fun (_tag,x) -> x > ms 500)
        test <@ 0 = Array.length queued // We're not triggering any queueing on the 'Limit' rule
                && between 9 (24-10) (Array.length delayed) // Typically, 3+3+1+1+1 should get delayed, but depending on scheduling, some won't get logged as such, and we don't want a flickering test
                && ['A'..'E'] |> Seq.forall (fun expected -> delayed |> Seq.exists (fun (tag,_delay) -> tag = ("clientIp",string expected)))
                && 2 = Array.length shed @> }

type Cutoff(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let defs = """{ "services": { "default": {
        "calls": {},
        "defaultPolicy": "default",
        "policies": {
            "default": [
                { "rule": "Cutoff", "timeoutMs": 1000, "slaMs": 500, "dryRun": false }
            ]
        }
}}}"""
    let dryRunDefs = defs.Replace("false","true")
    let expectedRules : Governor.CutoffConfig = { timeout = ms 1000; sla = Some (ms 500); dryRun = false }

    // Replacing sleep with Async.SleepWrong below demonstrates what a failure to honor cancellation will result in
    // (and cause the tests to fail, as the general case is that we expect a computation to honor cancelation requests
    //  cooperatively, as F# async code generally does)
    // We *could* provide a facility to abort immediately even if the task is not cooperating, but that would mean
    //   every timeout leaves an orphaned thread, which is a very risky behavior to offer
    // See https://github.com/App-vNext/Polly/wiki/Timeout, and grep for Pessimistic in the impl for more details
    let sleep : TimeSpan -> Async<unit> = Async.Sleep

    let runError (duration : TimeSpan) = async {
        do! sleep duration
        return raise (TimeoutException()) }
    let runOk (duration : TimeSpan) = async {
        do! sleep duration
        return 42 }

    let [<Fact>] ``dryRun mode does not inhibit processing`` () = async {
        let pol = Parser.parse(dryRunDefs).CreatePolicy(log)
        let ap = pol.Find("default","any")
        test <@ not ap.Policy.isolate
                && Some { expectedRules with dryRun = true } = ap.Policy.cutoff @>
        let! time, results =
            [0 ; 501 ; 2001; 2001; 501; 0]
            |> Seq.mapi (fun i duration -> (if i % 2 = 0 then runError else runOk) (ms duration))
            |> Seq.map (ap.Execute >> Async.Catch)
            |> Async.Parallel
            |> Stopwatch.Time
        let oks, errs = Choice.partition results
        test <@ 3 = Array.length oks
                && time.Elapsed >= ms 2001 && time.Elapsed < ms 5501 // 2001ms*2+ 1.5s fudge factor
                && 3 = Array.length errs
                && errs |> Seq.forall (fun x -> x.GetType() = typeof<TimeoutException>) @>
        let evnts = buffer.Take()
        let breachedOrCanceled = function
            | Breached ("default","(default)",_d) as x -> Choice1Of2 x
            | Call ("(default)",CanceledDryRun _d) as x -> Choice2Of2 x
            | x -> failwithf "Unexpected event %A" x
        let breached,wouldBeCancelled = evnts |> Seq.map breachedOrCanceled |> Choice.partition
        // even the zero delay ones could in extreme circumstances end up with wierd timing effects
        test <@ let breached,wouldBeCancelled = Array.length breached, Array.length wouldBeCancelled
                between 0 4 <| breached // should be = 2, but we'll settle for this weaker assertion
                && between 1 6 <| wouldBeCancelled
                && between 1 6 <| breached + wouldBeCancelled @> }

    let [<Fact>] ``when active, cooperatively cancels requests exceeding the cutoff duration`` () = async {
        let pol = Parser.parse(defs).CreatePolicy log
        let ap = pol.Find("default","any")
        test <@ not ap.Policy.isolate
                && Some expectedRules = ap.Policy.cutoff @>
        let! time, results =
            [0 ; 501 ; 2501; 2501; 501; 0]
            |> Seq.mapi (fun i duration -> (if i % 2 = 0 then runError else runOk) (ms duration))
            // Catch inside so the first throw doesnt cancel the overall execution
            |> Seq.map(ap.Execute >> Async.Catch)
            |> Async.Parallel
            |> Stopwatch.Time
        let oks, errs = Choice.partition results
        let errsWhere p = Seq.where p errs |> Seq.length
        test <@ time.Elapsed >= s 1 && time.Elapsed < ms 1500 // 1000ms timeout + 500ms fudge factor
                && between 1 3 <| Array.length oks // lower bound should be 2 but in extreme circumstances
                && between 1 3 <| errsWhere (fun x -> x :? TimeoutException)
                && between 2 4 <| errsWhere (fun x -> x :? Polly.Timeout.TimeoutRejectedException) @>
        let evnts = buffer.Take()
        let breachedOrCanceled = function
            | Breached ("default","(default)",_d) as x -> Choice1Of2 x
            | Canceled ("default","(default)",_d) as x -> Choice2Of2 x
            | x -> failwithf "Unexpected event %A" x
        let breached,canceled = evnts |> Seq.map breachedOrCanceled |> Choice.partition
        // even the zero delay ones could in extreme circumstances end up with wierd timing effects
        test <@ between 0 4 (Array.length breached) // Should be = 2, but it all depends on the thread pool, and we don't want a flickering test
                && between 2 4 (Array.length canceled) // Should be = 2
                && Array.length canceled = errsWhere (fun x -> x :? Polly.Timeout.TimeoutRejectedException)  @> }