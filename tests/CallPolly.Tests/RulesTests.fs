namespace CallPoly.Tests.Rules

open CallPolly
open CallPolly.Rules
open System
open Swensen.Unquote
open Xunit

[<AutoOpen>]
module Helpers =
    let ms x = TimeSpan.FromMilliseconds (float x)
    let s x = TimeSpan.FromSeconds (float x)
    let between min max (value : int) = value >= min && value <= max
    let hasChanged (serviceName : string) (call:string) (changeLevel:Rules.ChangeLevel) updated =
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
                { "rule": "Log",        "req": "OnlyWhenDebugEnabled", "res": "OnlyWhenDebugEnabled" }
            ],
            "default": [
                { "rule": "Include",    "policy": "defaultLog" },
                { "rule": "Sla",        "slaMs": 1000, "timeoutMs": 5000 }
            ],
            "edd": [
                { "rule": "Uri",        "base": "http://base" },
                { "rule": "Include",    "policy": "default" }
                ],
            "limit": [
                { "rule": "Limit",      "maxParallel": 2, "maxQueue": 3, "dryRun": true }
            ],
            "break": [
                { "rule": "Break",      "windowS": 5, "minRequests": 100, "failPct": 20, "breakS": 1 }
            ],
            "defaultBroken": [
                { "rule": "Isolate" }
            ],
            "cutoff": [
                { "rule": "Cutoff",     "timeoutMs": 500, "dryRun": true }
            ],
            "heavy": [
                { "rule": "Include",    "policy": "defaultLog" },
                { "rule": "Include",    "policy": "limit" },
                { "rule": "Include",    "policy": "break" },
                { "rule": "Include",    "policy": "cutoff" },
                { "rule": "Sla",        "slaMs": 5000, "timeoutMs": 10000 },
                { "rule": "Include",    "policy": "defaultBroken" }
            ],
            "UnknownUnknown": [
                { "rule": "NotYetImplemented" }
            ]
        }
}}}"""

    let baseUri = Uri "http://base"
    let baseRule = Parser.ActionRule.BaseUri baseUri

    let logRule = Parser.ActionRule.Log (Parser.LogMode.OnlyWhenDebugEnabled, Parser.LogMode.OnlyWhenDebugEnabled)
    let limitConfig : Rules.BulkheadConfig = { dop=2; queue=3; dryRun=true }
    let limitRule = Parser.ActionRule.Limit limitConfig
    let breakConfig : Rules.BreakerConfig = { window = s 5; minThroughput = 100; errorRateThreshold = 0.2; retryAfter = s 1; dryRun = false }
    let breakRule = Parser.ActionRule.Break breakConfig
    let cutoffConfig : Rules.CutoffConfig = { timeout = ms 500; sla = None; dryRun = true }
    let cutoffRule = Parser.ActionRule.Cutoff cutoffConfig
    let defConfig : Parser.CallConfiguration * Uri option =
        {   timeout = Some (s 5); sla = Some (s 1)
            ``base`` = Some baseUri; rel = None
            reqLog = Parser.LogMode.OnlyWhenDebugEnabled; resLog = Parser.LogMode.OnlyWhenDebugEnabled },
        Some baseUri
    let noRules = { isolate = false; cutoff = None; limit = None; breaker = None; }
    let heavyConfig : Parser.CallConfiguration * Uri option =
        {   timeout = Some (s 10); sla = Some (s 5)
            ``base`` = None; rel = None
            reqLog = Parser.LogMode.OnlyWhenDebugEnabled; resLog = Parser.LogMode.OnlyWhenDebugEnabled },
        None
    let heavyRules = { isolate = true; cutoff = Some cutoffConfig; limit = Some limitConfig; breaker = Some breakConfig }

    /// Base tests exercising core functionality
    type Core(output : Xunit.Abstractions.ITestOutputHelper) =
        let log = LogHooks.createLogger output

        let [<Fact>] ``Policy TryFind happy path`` () =
            let pol = Parser.parse log defs
            let tryFindActionRules call =
                pol.TryFind("default",call)
                |> Option.map (fun x -> x.Raw, x.Config, x.Policy)
            let eddRaw, eddCfg, eddRules = trap <@ tryFindActionRules "EstimatedDeliveryDates" |> Option.get @>
            test <@ [baseRule; logRule; Parser.ActionRule.Sla (ms 1000, ms 5000)] = eddRaw
                    && defConfig = eddCfg
                    && noRules = eddRules @>
            let poRaw, poCfg, poRules = trap <@ tryFindActionRules "placeOrder" |> Option.get @>
            test <@ [logRule; limitRule; breakRule; cutoffRule; Parser.ActionRule.Sla (ms 5000, ms 10000); Parser.ActionRule.Isolate] = poRaw
                    && heavyConfig = poCfg
                    && heavyRules = poRules @>
            test <@ None = tryFindActionRules "missing" @>

        let [<Fact>] ``Policy Find Happy path`` () =
            let pol = Parser.parse log defs
            let defaultLog = pol.Find("default","(defaultLog)").Raw
            let default_ = pol.Find("default","shouldDefault").Raw
            let findActionRules call = pol.Find("default",call).Raw
            test <@ baseRule :: default_ = findActionRules "EstimatedDeliveryDates" @>
            test <@ [Parser.ActionRule.Isolate] = findActionRules "EstimatedDeliveryDate" @>
            test <@ defaultLog @ [limitRule; breakRule; cutoffRule; Parser.ActionRule.Sla (ms 5000, ms 10000); Parser.ActionRule.Isolate] = findActionRules "placeOrder" @>
            test <@ default_ = findActionRules "unknown" @>
            let heavyPolicy = pol.Find("default","placeOrder")
            test <@ heavyPolicy.Policy.isolate
                    && Some breakConfig = heavyPolicy.Policy.breaker @>

        let [<Fact>] ``UpstreamPolicy Update management`` () =
            let pol = Parser.parse log defs
            let heavyPolicy = pol.Find("default","placeOrder")
            test <@ heavyPolicy.Policy.isolate
                    && Some breakConfig = heavyPolicy.Policy.breaker @>
            let updated =
                let polsWithDifferentAdddress = defs.Replace("http://base","http://base2")
                pol |> Parser.updateFrom polsWithDifferentAdddress |> List.ofSeq
            test <@ updated |> hasChanged "default" "EstimatedDeliveryDates" Rules.ChangeLevel.Configuration
                    && heavyPolicy.Policy.isolate @>
            let updated =
                let polsWithIsolateMangledAndBackToOriginalBaseAddress = defs.Replace("Isolate","isolate")
                pol |> Parser.updateFrom polsWithIsolateMangledAndBackToOriginalBaseAddress |> List.ofSeq
            test <@ updated |> hasChanged "default" "EstimatedDeliveryDates" Rules.ChangeLevel.Configuration
                    && updated |> hasChanged "default" "placeOrder" Rules.ChangeLevel.Policy
                    && not heavyPolicy.Policy.isolate @>

[<AutoOpen>]
module SerilogExtractors =
    open Serilog.Events

    let (|CallPollyEvent|_|) (logEvent : LogEvent) : CallPolly.Events.Event option =
        match logEvent.Properties.TryGetValue CallPolly.Events.Constants.EventPropertyName with
        | true, SerilogScalar (:? CallPolly.Events.Event as e) -> Some e
        | _ -> None
    type StatusEvent = Pending|Resetting
    type CallEvent = Breaking|BreakingDryRun|MaybeQueuing|SheddingDryRun|QueuingDryRun|CanceledDryRun of TimeSpan
    type LogEvent =
        | Isolated of service: string * call: string * policy: string
        | Broken of service: string * call: string * policy: string
        | Deferred of service: string * call: string * policy: string * duration: TimeSpan
        | Shed of service: string * call: string * policy: string
        | Breached of service: string * call: string * duration: TimeSpan
        | Canceled of service: string * call: string * duration: TimeSpan
        | Status of string * StatusEvent
        | Call of string * CallEvent
        | Other of string
    let classify = function
        | CallPollyEvent (Events.Event.Isolated(_service,eCall))
            & HasProp "service" (SerilogString service)
            & HasProp "call" (SerilogString call)
            & HasProp "policy" (SerilogString policy)
            when eCall = call ->
                Isolated (service,call,policy)
        | CallPollyEvent (Events.Event.Broken (_service,eCall,_config))
            & HasProp "service" (SerilogString service)
            & HasProp "call" (SerilogString call)
            & HasProp "policy" (SerilogString policy)
            when eCall = call ->
                Broken (service,call,policy)
        | TemplateContains "Pending Reopen" & HasProp "call" (SerilogString an) -> Status(an, Pending)
        | TemplateContains "Reset" & HasProp "call" (SerilogString an) -> Status(an, Resetting)
        | TemplateContains "Circuit Breaking " & HasProp "call" (SerilogString an) -> Call(an, Breaking)
        | TemplateContains "Circuit DRYRUN Breaking " & HasProp "call" (SerilogString an) -> Call(an, BreakingDryRun)
        | TemplateContains "Bulkhead Queuing likely for " & HasProp "call" (SerilogString an) -> Call(an, MaybeQueuing)
        | TemplateContains "Bulkhead DRYRUN Queuing " & HasProp "call" (SerilogString an) -> Call(an, QueuingDryRun)
        | TemplateContains "Bulkhead DRYRUN Shedding " & HasProp "call" (SerilogString an) -> Call(an, SheddingDryRun)
        | CallPollyEvent (Events.Event.Deferred (_service,eCall,eInterval))
            & HasProp "service" (SerilogString service)
            & HasProp "call" (SerilogString call)
            & HasProp "policy" (SerilogString policy)
            when eCall = call ->
                Deferred (service,call,policy, eInterval.Elapsed)
        | CallPollyEvent (Events.Event.Shed (_service,eCall,_config))
            & HasProp "service" (SerilogString service)
            & HasProp "call" (SerilogString call)
            & HasProp "policy" (SerilogString policy)
            when eCall = call ->
                Shed (service,call,policy)
        | CallPollyEvent (Events.Event.Breached (_service,eCall,_sla,interval))
            & HasProp "service" (SerilogString service)
            & HasProp "call" (SerilogString call)
            & HasProp "policy" (SerilogString _policy)
            when eCall = call ->
                Breached (service,call, interval.Elapsed)
        | CallPollyEvent (Events.Event.Canceled (_service,eCall,_config,interval))
            & HasProp "service" (SerilogString service)
            & HasProp "call" (SerilogString call)
            & HasProp "policy" (SerilogString _policy)
            when eCall = call ->
                Canceled (service,call, interval.Elapsed)
        | TemplateContains "Cutoff DRYRUN " & TemplateContains "cancelation would have been requested on "
            & HasProp "call" (SerilogString an)
            & HasProp "policy" (SerilogString _policy)
            & HasProp "durationMs" (SerilogScalar (:? float as duration)) ->
                Call(an, CanceledDryRun(TimeSpan.FromMilliseconds duration))
        | x -> Other (dumpEvent x)
    type SerilogHelpers.LogCaptureBuffer with
        member buffer.Take() =
            let actual = [for x in buffer.Entries -> classify x]
            buffer.Clear()
            actual

type Isolate(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let expectedLimitRules : Rules.BulkheadConfig = { dop = 2; queue = 3; dryRun = true }

    let [<Fact>] ``takes precedence over, but does not conceal Break, Limit or Timeout; logging only reflects Isolate rule`` () = async {
        let pol = Parser.parse log Core.defs
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
            pol |> Parser.updateFrom polsWithIsolateMangled |> List.ofSeq
        test <@ updated |> hasChanged "default" "placeOrder" Rules.ChangeLevel.Policy
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
                { "rule": "Isolate"  }
            ]
        }
}}}"""

    let [<Fact>] ``When on, throws and logs information`` () = async {
        let pol = Parser.parse log isolateDefs
        let ap = pol.Find("default","notFound")
        test <@ ap.Policy.isolate && None = ap.Policy.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("default","(default)","def")] =! buffer.Take() }

    let [<Fact>] ``when removed, stops intercepting processing, does not log`` () = async {
        let pol = Parser.parse log isolateDefs
        let ap = pol.Find("default","nonDefault")
        test <@ ap.Policy.isolate && None = ap.Policy.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("default","nonDefault","def")] =! buffer.Take()
        let updated =
            let polsWithIsolateMangled = isolateDefs.Replace("Isolate","isolate")
            pol |> Parser.updateFrom polsWithIsolateMangled |> List.ofSeq
        test <@ updated |> hasChanged "default" "nonDefault" Rules.ChangeLevel.Policy
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
                { "rule": "Break", "windowS": 5, "minRequests": 2, "failPct": 50, "breakS": .5, "dryRun": false }
            ]
        }
}}}"""
    let dryRunDefs = defs.Replace("false","true")

    let expectedRules : Rules.BreakerConfig = { window = s 5; minThroughput = 2; errorRateThreshold = 0.5; retryAfter = TimeSpan.FromSeconds 0.5; dryRun = false }

    let [<Fact>] ``applies break constraints, logging each application and status changes appropriately`` () = async {
        let pol = Parser.parse log defs
        let ap = pol.Find("default","notFound")
        test <@ not ap.Policy.isolate
                && Some expectedRules = ap.Policy.breaker @>
        let runTimeout = async {
            let timeout = async { return raise <| TimeoutException() }
            let! result = ap.Execute(timeout) |> Async.Catch
            test <@ match result with Choice2Of2 (:? TimeoutException) -> true | _ -> false @> }
        let shouldBeOpen = async {
            let fail = async { return failwith "Unexpected" }
            let! result = ap.Execute(fail) |> Async.Catch
            test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.BrokenCircuitException) -> true | _ -> false @>
            [Broken ("default","(default)","def")] =! buffer.Take() }
        let runSuccess = async {
            let success = async { return 42 }
            let! result = ap.Execute(success)
            42 =! result }
        do! runSuccess
        do! runTimeout
        [Call ("(default)",Breaking)] =! buffer.Take()
        do! shouldBeOpen
        // Waiting for 1s should have transitioned it to HalfOpen
        do! Async.Sleep (s 1)
        do! runSuccess
        [Status ("(default)",Pending); Status ("(default)",Resetting)] =! buffer.Take()
        do! runTimeout
        [Call ("(default)",Breaking)] =! buffer.Take()
        do! shouldBeOpen
        // Changing the rules should replace the breaker with a fresh instance which has forgotten the state
        let changedRules = defs.Replace(".5","5")
        pol |> Parser.updateFrom changedRules |> ignore
        do! runSuccess }

    let [<Fact>] ``dryRun mode prevents breaking, logs status changes appropriately`` () = async {
        let pol = Parser.parse log dryRunDefs
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
        let changedRules = defs.Replace(".5","5")
        pol |> Parser.updateFrom changedRules |> ignore
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
    let expectedRules : Rules.BulkheadConfig = { dop = 2; queue = 3; dryRun = false }

    let runError = async {
        do! Async.Sleep (s 1)
        return raise (System.TimeoutException()) }
    let runOk = async {
        do! Async.Sleep (s 1)
        return 42 }

    let [<Fact>] ``dryRun mode does not inhibit processing`` () = async {
        let pol = Parser.parse log dryRunDefs
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
                && time.Elapsed < ms 2500 // 1s+5*200+ 500ms fudge factor
                && 3 = Array.length errs
                && errs |> Seq.forall (fun x -> x.GetType() = typedefof<TimeoutException>) @>
        let evnts = buffer.Take()
        let queuedOrShed = function
            | Call ("(default)",QueuingDryRun) as x -> Choice1Of2 x
            | Call ("(default)",SheddingDryRun) as x -> Choice2Of2 x
            | x -> failwithf "Unexpected event %A" x
        let queued,shed = evnts |> Seq.map queuedOrShed |> Choice.partition
        test <@ 1 <= Array.length queued // should be = 3, but if there's a delay spinning up threads, sometimes fewer get queued
                && 1 = Array.length shed @> }

    let [<Fact>] ``when active, sheds load above limit`` () = async {
        let pol = Parser.parse log defs
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
                && time.Elapsed > s 2 && time.Elapsed < s 4
                && 1 = Array.length errs
                && match Seq.exactlyOne errs with :? Polly.Bulkhead.BulkheadRejectedException -> true | _ -> false @>
        let evnts = buffer.Take()
        let queuedOrShed = function
            | Call ("(default)",MaybeQueuing) as x -> Choice1Of3 x
            | Deferred ("default","(default)","def",delay) as x -> Choice2Of3 delay
            | Shed ("default","(default)","def") as x -> Choice3Of3 x
            | x -> failwithf "Unexpected event %A" x
        let queued,waited,shed = evnts |> Seq.map queuedOrShed |> Choice.partition3
        let delayed = waited |> Array.filter (fun x -> x > ms 500)
        // while we'll be pretty clear about shedding, we might discard some queuing notifications depending on the scheduling, and the shed one can also look like it will be queued
        test <@ 4 >= Array.length queued
                && between 2 3 (Array.length delayed) // Typically, 3 should get delayed, but depending on scheduling, only 2 get logged as such, and we don't want a flickering test
                && 1 = Array.length shed @> }

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
    let expectedRules : Rules.CutoffConfig = { timeout = ms 1000; sla = Some (ms 500); dryRun = false }

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
        let pol = Parser.parse log dryRunDefs
        let ap = pol.Find("default","any")
        test <@ not ap.Policy.isolate
                && Some { expectedRules with dryRun = true } = ap.Policy.cutoff @>
        let r = Random()
        let! time, results =
            [0 ; 501 ; 1501; 1501; 501; 0]
            |> Seq.mapi (fun i duration -> (if i % 2 = 0 then runError else runOk) (ms duration))
            |> Seq.map (ap.Execute >> Async.Catch)
            |> Async.Parallel
            |> Stopwatch.Time
        let oks, errs = Choice.partition results
        test <@ 3 = Array.length oks
                && time.Elapsed >= ms 1501 && time.Elapsed < ms 2001 // 1501ms+ 500ms fudge factor
                && 3 = Array.length errs
                && errs |> Seq.forall (fun x -> x.GetType() = typeof<TimeoutException>) @>
        let evnts = buffer.Take()
        let breachedOrCanceled = function
            | Breached ("default","(default)",_d) as x -> Choice1Of2 x
            | Call ("(default)",CanceledDryRun _d) as x -> Choice2Of2 x
            | x -> failwithf "Unexpected event %A" x
        let breached,wouldBeCancelled = evnts |> Seq.map breachedOrCanceled |> Choice.partition
        // even the zero delay ones could in extreme circumstances end up with wierd timing effects
        test <@ between 1 4 <| Array.length breached // should be = 2, but we'll settle for this weaker assertion
                && between 1 4 <| Array.length wouldBeCancelled @> } // should be = 2, but we'll settle for this weaker assertion

    let [<Fact>] ``when active, cooperatively cancels requests exceeding the cutoff duration`` () = async {
        let pol = Parser.parse log defs
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
        test <@ between 1 4 (Array.length breached) // Should be = 2, but it all depends on the thread pool, and we don't want a flickering test
                && between 2 4 (Array.length canceled) // Should be = 2
                && Array.length canceled = errsWhere (fun x -> x :? Polly.Timeout.TimeoutRejectedException)  @> }