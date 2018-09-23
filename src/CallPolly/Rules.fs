module CallPolly.Rules

open Polly
open Polly.CircuitBreaker
open System
open System.Collections.Generic
open System.Collections.Concurrent

[<RequireQualifiedAccess>]
type LogMode =
    | Always
    | Never
    | OnlyWhenDebugEnabled

type BreakerConfig = { window: TimeSpan; minThroughput: int; errorRateThreshold: float; retryAfter: TimeSpan }
type PolicyConfig = { isolate: bool; breaker: BreakerConfig option }
type CallConfiguration =
    {   timeout: TimeSpan option; sla: TimeSpan option
        ``base``: Uri option; rel: Uri option
        reqLog: LogMode; resLog: LogMode }

[<RequireQualifiedAccess>]
type ActionRule =
    | BaseUri of Uri: Uri
    | RelUri of Uri: Uri
    | Sla of sla: TimeSpan * timeout: TimeSpan
    | Log of req: LogMode * res: LogMode
    | Break of BreakerConfig
    | Isolate

let inferPolicy : ActionRule seq -> PolicyConfig =
    let folder s = function
        | ActionRule.Isolate -> { s with isolate = true }
        | ActionRule.Break breakerConfig -> { s with breaker = Some breakerConfig }
        // Covered by inferConfig
        | ActionRule.BaseUri _ | ActionRule.RelUri _ | ActionRule.Sla _ | ActionRule.Log _ -> s
    Seq.fold folder { isolate = false; breaker = None }
let inferConfig xs: CallConfiguration * Uri option =
    let folder s = function
        | ActionRule.BaseUri uri -> { s with ``base`` = Some uri }
        | ActionRule.RelUri uri -> { s with rel = Some uri }
        | ActionRule.Sla (sla=sla; timeout=t) -> { s with sla = Some sla; timeout = Some t }
        | ActionRule.Log (req=reqLevel; res=resLevel) -> { s with reqLog = reqLevel; resLog = resLevel }
        // Covered by inferPolicy
        | ActionRule.Isolate | ActionRule.Break _ -> s
    let def =
        {   reqLog = LogMode.Never; resLog = LogMode.Never
            timeout = None; sla = None
            ``base`` = None; rel = None }
    let config = Seq.fold folder def xs
    let effectiveAddress =
        match config.``base``, config.rel with
        | None, u | u, None -> u
        | Some b, Some r -> Uri(b,r) |> Some
    config, effectiveAddress

let log = Serilog.Log.ForContext(Serilog.Core.Constants.SourceContextPropertyName, "CallPolicy")

/// Translates a PolicyConfig's rules to a Polly IAsyncPolicy instance that gets held in the ActionPolicy
module private PollyMapper =
    let logBreak (exn : exn) (timespan: TimeSpan) (context: Context) =
        log.Warning(exn, "Circuit Breaking for {action} for {duration}", context.PolicyKey, timespan)
    let logReset (context : Context) =
        log.Information("Circuit Reset for {action}", context.PolicyKey)
    let ofPolicyConfig (key : string) (config : PolicyConfig) : IAsyncPolicy option =
        let logHalfOpen () =
            log.Information("Circuit Pending Reopen for {action}", key)
        let maybeCb : CircuitBreakerPolicy option =
            let maybeIsolate isolate (cb : CircuitBreakerPolicy)  =
                if isolate then cb.Isolate() else ()
                cb
            match config.breaker with
            | None when config.isolate ->
                Policy
                    .Handle<TimeoutException>()
                    .CircuitBreakerAsync(1, TimeSpan.MaxValue)
                |> maybeIsolate true
                |> Some
            | None ->
                None
            | Some bc ->
                Policy
                    .Handle<TimeoutException>()
                    .AdvancedCircuitBreakerAsync(
                        failureThreshold = bc.errorRateThreshold,
                        samplingDuration = bc.window,
                        minimumThroughput = bc.minThroughput,
                        durationOfBreak = bc.retryAfter,
                        onBreak = Action<_,_,_> logBreak,
                        onReset = Action<_>(logReset),
                        onHalfOpen = logHalfOpen )
                |> maybeIsolate config.isolate
                |> Some
        maybeCb |> Option.map (fun cb -> cb.WithPolicyKey(key) :> _)

type [<RequireQualifiedAccess>] ChangeLevel = Added |  CallConfigurationOnly | ConfigAndPolicy
type ActionPolicy(key, actionRules) =
    let mutable actionRules = actionRules
    let mutable policyConfig = inferPolicy actionRules
    let mutable maybePolly : IAsyncPolicy option = None
    let applyPolicyConfig newConfig =
        policyConfig <- newConfig
        maybePolly <- PollyMapper.ofPolicyConfig key policyConfig
    do applyPolicyConfig policyConfig
    member __.ActionRules = actionRules
    member __.PolicyConfig : PolicyConfig = policyConfig
    member __.CallConfig : CallConfiguration * Uri option =
        inferConfig actionRules
    member __.TryUpdateFrom updated =
        if actionRules = updated then
            None
        else
            actionRules <- updated
            match inferPolicy updated with
            | newPol when newPol = policyConfig ->
                Some ChangeLevel.CallConfigurationOnly
            | newPol ->
                applyPolicyConfig newPol
                Some ChangeLevel.ConfigAndPolicy
    member __.Execute(inner : Async<'t>) =
        match maybePolly with
        | None -> inner
        | Some polly -> async {
            let! ct = Async.CancellationToken
            let startInnerTask () = Async.StartAsTask(inner, cancellationToken=ct)
            return! polly.ExecuteAsync(startInnerTask) |> Async.AwaitTaskCorrect }

type UpstreamPolicyWithoutDefault private(makeKey, map: ConcurrentDictionary<string,ActionPolicy>) =
    static let mkActionPolicy makeKey action rules = ActionPolicy(makeKey action, rules)

    /// Searches for the Policy associated with Action name`
    member __.TryFind name : ActionPolicy option =
        match map.TryGetValue name with
        | true, rules -> Some rules
        | false, _ -> None

    /// Attempts to parse
    /// - Policies: mapping of policy name to arrays of Rules, defined by the json in `policiesJson`
    /// - Map: the mappings of Action names to items in the Policies, defined by the json in `mapJson`
    /// Throws if policiesJson or mapJson fail to adhere to correct Json structure
    static member Parse(makeKey, rulesMap : (string * ActionRule list) seq) =
        let inputs = seq { for action,rules in rulesMap -> KeyValuePair(action, mkActionPolicy makeKey action rules) }
        UpstreamPolicyWithoutDefault(makeKey, ConcurrentDictionary inputs)
    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(rulesMap : (string * ActionRule list) seq) =
        let rulesMap = rulesMap |> Seq.cache
        seq { for action,rules in rulesMap do
                let mutable changeLevel = None
                let add action =
                    changeLevel <- Some ChangeLevel.Added
                    mkActionPolicy makeKey action rules
                let tryUpdate (current : ActionPolicy) =
                    changeLevel <- current.TryUpdateFrom rules
                    current
                map.AddOrUpdate(action, (fun action -> add action), fun _ current -> tryUpdate current) |> ignore
                match changeLevel with
                | None -> ()
                | Some changeLevel -> yield action, changeLevel }
    /// Facilitates dumping for diagnostics
    member __.InternalState = map

/// Defines a Policy that includes a fallback policy for Actions without a specific rule
type UpstreamPolicy private(inner: UpstreamPolicyWithoutDefault, defaultPolicy: ActionPolicy) =
    /// Determines the Rules to be applied for the Action `name`
    /// Throws InvalidOperationException if no rule found for `name` and no mapping provided for a `(default)` action
    member __.Find action : ActionPolicy =
        match inner.TryFind action with
        | Some actionPolicy -> actionPolicy
        | None -> defaultPolicy

    /// Attempts to parse as per a normal Policy, throws if a ruleset entitled `defaultLabel` is not present in the set
    static member Parse(makeKey, rulesMap : (string * ActionRule list) seq, defaultName) =
        let inner = UpstreamPolicyWithoutDefault.Parse(makeKey, rulesMap)
        let def =
           match inner.TryFind defaultName with
            | None -> invalidOp (sprintf "Could not find a default policy entitled '%s'" defaultName)
            | Some defPol -> defPol
        UpstreamPolicy(inner, def)
    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom rulesMap = inner.UpdateFrom rulesMap
    /// Facilitates dumping for diagnostics
    member __.InternalState = inner.InternalState