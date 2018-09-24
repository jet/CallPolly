module CallPoly.Tests.Rules

open CallPolly
open System
open Swensen.Unquote
open Xunit
open CallPolly.Rules

let pols = """{
  "defaultLog": [
    {
      "ruleName": "Log",
      "req": "OnlyWhenDebugEnabled",
      "res": "OnlyWhenDebugEnabled"
    }
  ],
  "break": [
    {
      "ruleName": "Break",
      "windowS": 5,
      "minRequests": 100,
      "failPct": 20,
      "breakS": 1
    }
  ],
  "default": [
    {
      "ruleName": "Include",
      "policyName": "defaultLog"
    },
    {
      "ruleName": "Sla",
      "slaMs": 1000,
      "timeoutMs": 5000
    }
  ],
  "edd": [
    {
      "ruleName": "Uri",
      "base": "http://base"
    },
    {
      "ruleName": "Include",
      "policyName": "default"
    }
  ],
  "heavy": [
    {
      "ruleName": "Include",
      "policyName": "defaultLog"
    },
    {
      "ruleName": "Include",
      "policyName": "break"
    },
    {
      "ruleName": "Sla",
      "slaMs": 5000,
      "timeoutMs": 10000
    },
    {
      "ruleName": "Include",
      "policyName": "defaultBroken"
    }
  ],
  "defaultBroken": [
    {
      "ruleName": "Isolate"
    }
  ],
  "UnknownUnknown": [
    {
      "ruleName": "NotYetImplemented"
    }
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

let ms x = TimeSpan.FromMilliseconds (float x)
let s x = TimeSpan.FromSeconds (float x)

let baseUri = Rules.ActionRule.BaseUri (Uri "http://base")

let logRule = Rules.ActionRule.Log (Rules.LogMode.OnlyWhenDebugEnabled, Rules.LogMode.OnlyWhenDebugEnabled)
let breakConfig : Rules.BreakerConfig = { window = s 5; minThroughput = 100; errorRateThreshold = 0.2; retryAfter = s 1 }
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
    let (|Isolated|Other|) = function
        | CallPollyEvent (Events.Event.Isolated ePolicy)
            & HasProp "action" (SerilogString action)
            & HasProp "policy" (SerilogString policy)
            when ePolicy = policy ->
                Isolated (sprintf "Isolated %s %s" policy action)
        | x -> Other x

type SerilogHelpers.LogCaptureBuffer with
    member buffer.VerifyActionIsolatedLogged policy action =
        let expected, unexpected = Seq.map (|Isolated|Other|) buffer.Entries |> Choice.partition
        test <@ let dumped = Array.map dumpEvent unexpected
                [ sprintf "Isolated %s %s" policy action ] = List.ofArray expected
                && Array.isEmpty dumped @>
        buffer.Clear()

    member buffer.VerifyNothingLogged () = test <@ Seq.isEmpty <| Seq.map dumpEvent buffer.Entries @>

type Isolate(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let pols = """{
      "default": [
        { "ruleName": "Isolate"  }
      ]
    }"""
    let map = """{
      "(default)": "default",
      "nonDefault": "default"
    }"""

    let [<Fact>] ``When on, throws and logs information`` () = async {
        let pol = Parser.parseUpstreamPolicy log pols map
        let actionName = "exampleAction"
        let ap = pol.Find actionName
        test <@ ap.PolicyConfig.isolate && None = ap.PolicyConfig.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(actionName,call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        buffer.VerifyActionIsolatedLogged "default" actionName }

    let [<Fact>] ``when removed, stops intercepting processing, does not log`` () = async {
        let pol = Parser.parseUpstreamPolicy log pols map
        let actionName = "nonDefault"
        let ap = pol.Find actionName
        test <@ ap.PolicyConfig.isolate && None = ap.PolicyConfig.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(actionName, call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        buffer.VerifyActionIsolatedLogged "default" actionName
        let updated =
            let polsWithIsolateMangled = pols.Replace("Isolate","isolate")
            pol |> Parser.updateFrom polsWithIsolateMangled map |> List.ofSeq
        test <@ updated |> List.contains ("(default)",Rules.ChangeLevel.ConfigAndPolicy)
                && not ap.PolicyConfig.isolate @>
        let! result = ap.Execute(actionName, call) |> Async.Catch
        test <@ Choice1Of2 42 = result @>
        buffer.VerifyNothingLogged() }

type Break(output : Xunit.Abstractions.ITestOutputHelper) =
    let log, buffer = LogHooks.createLoggerWithCapture output

    let [<Fact>] ``Isolate facility takes precedence over, but does not conceal Break; logging only reflects Isolate rule`` () = async {
        let pol = Parser.parseUpstreamPolicy log pols map
        let actionName = "placeOrder"
        let ap = pol.Find actionName
        test <@ ap.PolicyConfig.isolate
                && Some breakConfig = ap.PolicyConfig.breaker @>
        let call = async { return 42 }
        let! result = ap.Execute(actionName, call) |> Async.Catch
        test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.IsolatedCircuitException) -> true | _ -> false @>
        buffer.VerifyActionIsolatedLogged "heavy" actionName
        let updated =
            let polsWithIsolateMangled = pols.Replace("Isolate","isolate")
            pol |> Parser.updateFrom polsWithIsolateMangled map |> List.ofSeq
        test <@ updated |> List.contains ("placeOrder",Rules.ChangeLevel.ConfigAndPolicy)
                && not ap.PolicyConfig.isolate @>
        let! result = ap.Execute(actionName, call) |> Async.Catch
        test <@ Choice1Of2 42 = result @>
        buffer.VerifyNothingLogged() }

    let pols = """{
      "default": [
        {
          "ruleName": "Break",
          "windowS": 5,
          "minRequests": 2,
          "failPct": 50,
          "breakS": .5
        }
      ]
    }"""

    let expectedRules : Rules.BreakerConfig = { window = s 5; minThroughput = 2; errorRateThreshold = 0.5; retryAfter = TimeSpan.FromSeconds 0.5 }

    let map = """{
      "(default)": "default",
    }"""

    let [<Fact>] ``applies break constraints`` () = async {
        let pol = Parser.parseUpstreamPolicy log pols map
        let ap = pol.Find "any"
        test <@ not ap.PolicyConfig.isolate
                && Some expectedRules = ap.PolicyConfig.breaker @>
        let executeCallYieldingTimeout = async {
            let timeout = async { return raise <| TimeoutException() }
            let! result = ap.Execute("action",timeout) |> Async.Catch
            test <@ match result with Choice2Of2 (:? TimeoutException) -> true | _ -> false @> }
        let shouldBeOpen = async {
            let fail = async { return failwith "Unexpected" }
            let! result = ap.Execute("action",fail) |> Async.Catch
            test <@ match result with Choice2Of2 (:? Polly.CircuitBreaker.BrokenCircuitException) -> true | _ -> false @> }
        let runSuccess = async {
            let success = async { return 42 }
            let! result = ap.Execute("action",success)
            42 =! result }
        do! runSuccess
        do! executeCallYieldingTimeout
        do! shouldBeOpen
        // Waiting for 1s should have transitioned it to HalfOpen
        do! Async.Sleep (s 1)
        do! runSuccess
        do! executeCallYieldingTimeout
        do! shouldBeOpen
        // Changing the rules should replace the breaker with a fresh instance which has forgotten the state
        let changedRules = pols.Replace(".5","5")
        pol |> Parser.updateFrom changedRules map |> ignore
        do! shouldBeOpen }