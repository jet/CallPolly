module CallPolly.Rules

open Polly
open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks

[<RequireQualifiedAccess>]
type LogMode =
    | Always
    | Never
    | OnlyWhenDebugEnabled

type BreakerConfig = { window: TimeSpan; minThroughput: int; errorRateThreshold: float; retryAfter: TimeSpan; dryRun: bool }
type BulkheadConfig = { dop: int; queue: int; dryRun: bool }
type PolicyConfig = { isolate: bool; limit: BulkheadConfig option; breaker: BreakerConfig option }
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
    | Limit of BulkheadConfig
    | Isolate

let inferPolicy : ActionRule seq -> PolicyConfig =
    let folder s = function
        | ActionRule.Isolate -> { s with isolate = true }
        | ActionRule.Break breakerConfig -> { s with breaker = Some breakerConfig }
        | ActionRule.Limit bulkheadConfig -> { s with limit = Some bulkheadConfig }
        // Covered by inferConfig
        | ActionRule.BaseUri _ | ActionRule.RelUri _ | ActionRule.Sla _ | ActionRule.Log _ -> s
    Seq.fold folder { isolate = false; limit = None; breaker = None }
let inferConfig xs: CallConfiguration * Uri option =
    let folder s = function
        | ActionRule.BaseUri uri -> { s with ``base`` = Some uri }
        | ActionRule.RelUri uri -> { s with rel = Some uri }
        | ActionRule.Sla (sla=sla; timeout=t) -> { s with sla = Some sla; timeout = Some t }
        | ActionRule.Log (req=reqLevel; res=resLevel) -> { s with reqLog = reqLevel; resLog = resLevel }
        // Covered by inferPolicy
        | ActionRule.Isolate | ActionRule.Limit _ | ActionRule.Break _ -> s
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

module ContextKeys =
    let [<Literal>] action = "action"

/// Translates a PolicyConfig's rules to a Polly IAsyncPolicy instance that gets held in the ActionPolicy
type ActionGovernor(log: Serilog.ILogger, actionName : string, policyName: string, config : PolicyConfig) =
    let logBreaking (exn : exn) (timespan: TimeSpan) =
        match config with
        | { isolate = true } -> ()
        | { breaker = Some { dryRun = true; retryAfter = t } } -> log |> Events.Log.breakingDryRun exn actionName t
        | _ -> log |> Events.Log.breaking exn actionName timespan
    let logHalfOpen () = log |> Events.Log.halfOpen actionName
    let logReset () = log |> Events.Log.reset actionName
    let logQueuing() = log |> Events.Log.queuing actionName
    let logDeferral interval concurrencyLimit = log |> Events.Log.deferral policyName actionName interval concurrencyLimit
    let logShedding config = log |> Events.Log.shedding policyName actionName config
    let logSheddingDryRun () = log |> Events.Log.sheddingDryRun actionName
    let logQueuingDryRun () = log |> Events.Log.queuingDryRun actionName
    let maybeCb : CircuitBreaker.CircuitBreakerPolicy option =
        let maybeIsolate isolate (cb : CircuitBreaker.CircuitBreakerPolicy) =
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
                    // using technique in https://github.com/App-vNext/Polly/issues/496#issuecomment-420183946 to trigger an inert break
                    durationOfBreak = (if bc.dryRun then TimeSpan.Zero else bc.retryAfter),
                    onBreak = Action<_,_> logBreaking,
                    // durationOfBreak above will cause a break to immediately be followed by a halfOpen and reset - we want to cut off those messages at source
                    onReset = Action (if bc.dryRun then ignore else logReset),
                    onHalfOpen = (if bc.dryRun then ignore else logHalfOpen) )
                |> maybeIsolate config.isolate
            |> Some
    let maybeBulkhead : Bulkhead.BulkheadPolicy option =
        match config.limit with
        | None -> None
        | Some limit ->
            let logRejection (_: Context) : Task = logShedding limit ; Task.CompletedTask
            let effectiveLimit = if limit.dryRun then Int32.MaxValue else limit.dop // https://github.com/App-vNext/Polly/issues/496#issuecomment-420183946
            let bhp = Policy.BulkheadAsync(maxParallelization = effectiveLimit, maxQueuingActions = limit.queue, onBulkheadRejectedAsync = logRejection)
            Some bhp
    let asyncPolicy : IAsyncPolicy option =
        match maybeBulkhead, maybeCb with
        | Some bh, Some cb -> Policy.WrapAsync(bh, cb) :> IAsyncPolicy |> Some
        | Some bh, None -> bh :> IAsyncPolicy |> Some
        | None, Some cb -> cb :> IAsyncPolicy |> Some
        | None, None -> None
    /// This Exception filter is intended to perform logging as a side-effect without entering a catch and/or having to be re-raised, hence wierd impl
    let (|LogWhenIsolatedFilter|_|) (ex : exn) =
        match ex with
        | :? Polly.CircuitBreaker.IsolatedCircuitException -> Events.Log.actionIsolated log policyName actionName; None
        | :? Polly.CircuitBreaker.BrokenCircuitException -> Events.Log.actionBroken log policyName actionName config.breaker.Value; None
        | _ when true -> None
        | _ -> invalidOp "not possible"; Some () // Compiler gets too clever if we never return Some
    /// Execute and/or an invocation of a function with the relevant policy applied
    member __.Execute(inner : Async<'a>) : Async<'a> =
        match asyncPolicy with
        | None -> inner
        | Some polly -> async {
            let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            let mutable wasFull = false
            // NB This logging is on a best-effort basis - obviously the guard here has an implied race condition
            // queing might not be needed and/or the request might instead be shed
            match config, maybeBulkhead with
            | { limit = Some { dryRun = false } }, Some bh ->
                if bh.BulkheadAvailableCount = 0 && bh.QueueAvailableCount <> 0 then
                    logQueuing ()
            // As we'll be let straight through unencumbered in dryRun mode, but might be in a race to start, note whether queuing was aleady in effect
            | { limit = Some ({ dryRun = true } as limit) }, Some bh ->
                let activeCount = Int32.MaxValue - bh.BulkheadAvailableCount
                if activeCount > limit.dop then wasFull <- true
            | _ -> ()

            let! ct = Async.CancellationToken
            let startInnerTask _ctx =
                // As we'll be let straight through unencumbered in dryRun mode, we do the checks at the point where we're being scheduled to run
                match config, maybeBulkhead with
                | { limit = Some ({ dryRun = true } as limit) }, Some bh ->
                    let activeCount = Int32.MaxValue - bh.BulkheadAvailableCount
                    if activeCount > limit.dop + limit.queue then logSheddingDryRun ()
                    elif activeCount > limit.dop && wasFull then logQueuingDryRun ()
                | { limit = Some ({ dryRun = false } as limit) }, _ ->
                    let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
                    let interval = Events.StopwatchInterval(startTicks, endTicks)
                    if interval.Elapsed.TotalMilliseconds > 1. then
                        logDeferral interval limit.dop
                | _ -> ()

                Async.StartAsTask(inner, cancellationToken=ct)
            // TODO find/add a cleaner way to use the Polly API to log when the event fires due to the the circuit being Isolated
            try return! polly.ExecuteAsync startInnerTask |> Async.AwaitTaskCorrect
            with LogWhenIsolatedFilter -> return! invalidOp "not possible; Filter always returns None" }

type [<RequireQualifiedAccess>] ChangeLevel = Added |  CallConfigurationOnly | ConfigAndPolicy
type ActionPolicy(log, actionName, policyName, actionRules) =
    let makePolicyConfig rules = inferPolicy rules
    let mutable actionRules, policyConfig = actionRules, makePolicyConfig actionRules
    let makeGoverner () = ActionGovernor(log,  actionName, policyName, policyConfig)
    let mutable governor : ActionGovernor = makeGoverner()
    let updateConfig newConfig =
        policyConfig <- newConfig
        governor <- makeGoverner()

    member __.ActionRules = actionRules
    member __.PolicyConfig : PolicyConfig = policyConfig
    member __.CallConfig : CallConfiguration * Uri option =
        inferConfig actionRules
    member __.TryUpdateFrom rules =
        if actionRules = rules then
            None
        else
            actionRules <- rules
            match makePolicyConfig rules with
            | newPol when newPol = policyConfig ->
                Some ChangeLevel.CallConfigurationOnly
            | newPol ->
                updateConfig newPol
                Some ChangeLevel.ConfigAndPolicy
    member __.Execute(inner : Async<'t>) =
       governor.Execute inner

type UpstreamPolicyWithoutDefault private(log : Serilog.ILogger, map: ConcurrentDictionary<string,ActionPolicy>) =
    static let mkActionPolicy log actionName policyName rules = ActionPolicy(log, actionName, policyName, rules)

    /// Searches for the Policy associated with Action name`
    member __.TryFind actionName : ActionPolicy option =
        match map.TryGetValue actionName with
        | true, rules -> Some rules
        | false, _ -> None

    /// Attempts to parse
    /// - Policies: mapping of policy name to arrays of Rules, defined by the json in `policiesJson`
    /// - Map: the mappings of Action names to items in the Policies, defined by the json in `mapJson`
    /// Throws if policiesJson or mapJson fail to adhere to correct Json structure
    static member Parse(log, rulesMap : (string * string * ActionRule list) seq) =
        let inputs = seq { for actionName, policyName, rules in rulesMap -> KeyValuePair(actionName, mkActionPolicy log actionName policyName rules) }
        UpstreamPolicyWithoutDefault(log, ConcurrentDictionary inputs)

    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(rulesMap : (string * string * ActionRule list) seq) =
        let rulesMap = rulesMap |> Seq.cache
        seq { for actionName, policyName, rules in rulesMap do
                let mutable changeLevel = None
                let add policyName =
                    changeLevel <- Some ChangeLevel.Added
                    mkActionPolicy log actionName policyName rules
                let tryUpdate (current : ActionPolicy) =
                    changeLevel <- current.TryUpdateFrom rules
                    current
                map.AddOrUpdate(actionName, (fun _actionName -> add policyName), fun _ current -> tryUpdate current) |> ignore
                match changeLevel with
                | None -> ()
                | Some changeLevel -> yield actionName, changeLevel }
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
    static member Parse(log, rulesMap : (string * string * ActionRule list) seq, defaultName) =
        let inner = UpstreamPolicyWithoutDefault.Parse(log, rulesMap)
        let def =
           match inner.TryFind defaultName with
            | None -> invalidOp (sprintf "Could not find a default policy entitled '%s'" defaultName)
            | Some defPol -> defPol
        UpstreamPolicy(inner, def)
    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom rulesMap = inner.UpdateFrom rulesMap
    /// Facilitates dumping for diagnostics
    member __.InternalState = inner.InternalState