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

module Core =
    let pols = """{
      "defaultLog": [
        { "ruleName": "Log",        "req": "OnlyWhenDebugEnabled", "res": "OnlyWhenDebugEnabled" }
      ],
      "default": [
        { "ruleName": "Include",    "policyName": "defaultLog" },
        { "ruleName": "Sla",        "slaMs": 1000, "timeoutMs": 5000 }
      ],
      "edd": [
        { "ruleName": "Uri",        "base": "http://base" },
        { "ruleName": "Include",    "policyName": "default" }
      ],
      "break": [
        { "ruleName": "Break",      "windowS": 5, "minRequests": 100, "failPct": 20, "breakS": 1 }
      ],
      "defaultBroken": [
        { "ruleName": "Isolate" }
      ],
      "heavy": [
        { "ruleName": "Include",    "policyName": "defaultLog" },
        { "ruleName": "Include",    "policyName": "break" },
        { "ruleName": "Sla",        "slaMs": 5000, "timeoutMs": 10000 },
        { "ruleName": "Include",    "policyName": "defaultBroken" }
      ],
      "UnknownUnknown": [
        { "ruleName": "NotYetImplemented" }
      ]
    }"""

    let map = """{
      "(default)": "default",
      "(defaultLog)": "defaultLog",
      "checkout": "heavy",
      "placeOrder": "heavy",
      "EstimatedDeliveryDates": "edd",
      "EstimatedDeliveryDate": "defaultBroken"
    }"""

    let baseUri = Rules.ActionRule.BaseUri (Uri "http://base")

    let logRule = Rules.ActionRule.Log (Rules.LogMode.OnlyWhenDebugEnabled, Rules.LogMode.OnlyWhenDebugEnabled)
    let breakConfig : Rules.BreakerConfig = { window = s 5; minThroughput = 100; errorRateThreshold = 0.2; retryAfter = s 1; dryRun = false }
    let breakRule = Rules.ActionRule.Break breakConfig

    /// Base tests exercising core functionality
    type Core(output : Xunit.Abstractions.ITestOutputHelper) =
        let log = LogHooks.createLogger output

        let [<Fact>] ``UpstreamPolicyWithoutDefault happy path`` () =
            let pol = Parser.parseUpstreamPolicyWithoutDefault log pols map
            let tryFindActionRules actionName = pol.TryFind actionName |> Option.map (fun x -> x.ActionRules)
            test <@ Some [ baseUri; logRule; Rules.ActionRule.Sla (ms 1000, ms 5000)] = tryFindActionRules "EstimatedDeliveryDates" @>
            test <@ Some [ logRule; breakRule; Rules.ActionRule.Sla (ms 5000, ms 10000); Rules.ActionRule.Isolate] = tryFindActionRules "placeOrder" @>
            test <@ None = pol.TryFind "missing" @>

        let [<Fact>] ``UpstreamPolicy Happy path`` () =
            let pol = Parser.parseUpstreamPolicy log pols map
            let defaultLog = pol.Find("(defaultLog)").ActionRules
            let default_ = pol.Find("shouldDefault").ActionRules
            let findActionRules actionName = pol.Find(actionName).ActionRules
            test <@ baseUri :: default_ = findActionRules "EstimatedDeliveryDates" @>
            test <@ [Rules.ActionRule.Isolate] = findActionRules "EstimatedDeliveryDate" @>
            test <@ defaultLog @ [breakRule; Rules.ActionRule.Sla (ms 5000, ms 10000); Rules.ActionRule.Isolate] = findActionRules "placeOrder" @>
            test <@ default_ = findActionRules "unknown" @>
            let heavyPolicy = pol.Find "placeOrder"
            test <@ heavyPolicy.PolicyConfig.isolate
                    && Some breakConfig = heavyPolicy.PolicyConfig.breaker @>

        let [<Fact>] ``UpstreamPolicy Update management`` () =
            let pol = Parser.parseUpstreamPolicy log pols map
            let heavyPolicy = pol.Find "placeOrder"
            test <@ heavyPolicy.PolicyConfig.isolate
                    && Some breakConfig = heavyPolicy.PolicyConfig.breaker @>
            let updated =
                let polsWithDifferentAdddress = pols.Replace("http://base","http://base2")
                pol |> Parser.updateFrom polsWithDifferentAdddress map |> List.ofSeq
            test <@ updated |> List.contains ("EstimatedDeliveryDates",Rules.ChangeLevel.CallConfigurationOnly)
                    && heavyPolicy.PolicyConfig.isolate @>
            let updated =
                let polsWithIsolateMangledAndBackToOriginalBaseAddress = pols.Replace("Isolate","isolate")
                pol |> Parser.updateFrom polsWithIsolateMangledAndBackToOriginalBaseAddress map |> List.ofSeq
            test <@ updated |> List.contains ("EstimatedDeliveryDates",Rules.ChangeLevel.CallConfigurationOnly)
                    && updated |> List.contains ("placeOrder",Rules.ChangeLevel.ConfigAndPolicy)
                    && not heavyPolicy.PolicyConfig.isolate @>

[<AutoOpen>]
module SerilogExtractors =
    open Serilog.Events

    let (|CallPollyEvent|_|) (logEvent : LogEvent) : CallPolly.Events.Event option =
        match logEvent.Properties.TryGetValue CallPolly.Events.Constants.EventPropertyName with
        | true, SerilogScalar (:? CallPolly.Events.Event as e) -> Some e
        | _ -> None
    type StatusEvent = Pending|Resetting
    type CallEvent = Breaking|BreakingDryRun
    type LogEvent =
        | Isolated of policy: string * action: string
        | Broken of policy: string * action: string
        | Status of string * StatusEvent
        | Call of string * CallEvent
        | Other of string
    let classify = function
        | CallPollyEvent (Events.Event.Isolated eAction)
            & HasProp "actionName" (SerilogString action)
            & HasProp "policy" (SerilogString policy)
            when eAction = action ->
                Isolated (policy, action)
        | CallPollyEvent (Events.Event.Broken eAction)
            & HasProp "actionName" (SerilogString action)
            & HasProp "policy" (SerilogString policy)
            when eAction = action ->
                Broken (policy, action)
        | TemplateContains "Pending Reopen" & HasProp "actionName" (SerilogString an) -> Status(an, Pending)
        | TemplateContains "Reset" & HasProp "actionName" (SerilogString an) -> Status(an, Resetting)
        | TemplateContains "Circuit Breaking " & HasProp "actionName" (SerilogString an) -> Call(an, Breaking)
        | TemplateContains "Circuit DRYRUN Breaking " & HasProp "actionName" (SerilogString an) -> Call(an, BreakingDryRun)
        | x -> Other (dumpEvent x)
    type SerilogHelpers.LogCaptureBuffer with
        member buffer.Take() =
            let actual = [for x in buffer.Entries -> classify x]
            buffer.Clear()
            actual

type Isolate(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let [<Fact>] ``takes precedence over, but does not conceal Break and Limit; logging only reflects Isolate rule`` () = async {
        let pol = Parser.parseUpstreamPolicy log Core.pols Core.map
        let ap = pol.Find "placeOrder"
        test <@ ap.PolicyConfig.isolate
                && Some Core.breakConfig = ap.PolicyConfig.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("heavy","placeOrder")] =! buffer.Take()
        let updated =
            let polsWithIsolateMangled = Core.pols.Replace("Isolate","isolate")
            pol |> Parser.updateFrom polsWithIsolateMangled Core.map |> List.ofSeq
        test <@ updated |> List.contains ("placeOrder",Rules.ChangeLevel.ConfigAndPolicy)
                && not ap.PolicyConfig.isolate @>
        let! result = ap.Execute(call) |> Async.Catch
        test <@ Choice1Of2 42 = result @>
        [] =! buffer.Take() }

    let isolatePols = """{
      "default": [
        { "ruleName": "Isolate"  }
      ]
    }"""
    let isolateMap = """{
      "(default)": "default",
      "nonDefault": "default"
    }"""

    let [<Fact>] ``When on, throws and logs information`` () = async {
        let pol = Parser.parseUpstreamPolicy log isolatePols isolateMap
        let ap = pol.Find "notFound"
        test <@ ap.PolicyConfig.isolate && None = ap.PolicyConfig.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("default","(default)")] =! buffer.Take() }

    let [<Fact>] ``when removed, stops intercepting processing, does not log`` () = async {
        let pol = Parser.parseUpstreamPolicy log isolatePols isolateMap
        let ap = pol.Find "nonDefault"
        test <@ ap.PolicyConfig.isolate && None = ap.PolicyConfig.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        [Isolated ("default","nonDefault")] =! buffer.Take()
        let updated =
            let polsWithIsolateMangled = isolatePols.Replace("Isolate","isolate")
            pol |> Parser.updateFrom polsWithIsolateMangled isolateMap |> List.ofSeq
        test <@ updated |> List.contains ("(default)",Rules.ChangeLevel.ConfigAndPolicy)
                && not ap.PolicyConfig.isolate @>
        let! result = ap.Execute(call) |> Async.Catch
        test <@ Choice1Of2 42 = result @>
        [] =! buffer.Take() }

type Break(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let pols = """{
      "default": [
        {
          "ruleName": "Break",
          "windowS": 5,
          "minRequests": 2,
          "failPct": 50,
          "breakS": .5,
          "dryRun": false
        }
      ]
    }"""
    let dryRunPols = pols.Replace("false","true")

    let expectedRules : Rules.BreakerConfig = { window = s 5; minThroughput = 2; errorRateThreshold = 0.5; retryAfter = TimeSpan.FromSeconds 0.5; dryRun = false }

    let map = """{
      "(default)": "default",
    }"""

    let [<Fact>] ``applies break constraints, logging each application and status changes appropriately`` () = async {
        let pol = Parser.parseUpstreamPolicy log pols map
        let ap = pol.Find "notFound"
        test <@ not ap.PolicyConfig.isolate
                && Some expectedRules = ap.PolicyConfig.breaker @>
        let runTimeout = async {
            let timeout = async { return raise <| TimeoutException() }
            let! result = ap.Execute(timeout) |> Async.Catch
            test <@ match result with Choice2Of2 (:? TimeoutException) -> true | _ -> false @> }
        let shouldBeOpen = async {
            let fail = async { return failwith "Unexpected" }
            let! result = ap.Execute(fail) |> Async.Catch
            test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.BrokenCircuitException) -> true | _ -> false @>
            [Broken ("default","(default)")] =! buffer.Take() }
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
        let changedRules = pols.Replace(".5","5")
        pol |> Parser.updateFrom changedRules map |> ignore
        do! shouldBeOpen }

    let [<Fact>] ``dryRun mode prevents breaking, logs status changes appropriately`` () = async {
        let pol = Parser.parseUpstreamPolicy log dryRunPols map
        let ap = pol.Find "notFound"
        test <@ not ap.PolicyConfig.isolate
                && Some { expectedRules with dryRun = true } = ap.PolicyConfig.breaker @>
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
        let changedRules = pols.Replace(".5","5")
        pol |> Parser.updateFrom changedRules map |> ignore
        do! runSuccess
        do! runSuccess
        // 1/2 failed (if the timeout happened in between the two successes, it would fail)
        do! runTimeout
        [] =! buffer.Take() }
