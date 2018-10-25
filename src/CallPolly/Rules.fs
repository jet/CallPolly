module CallPolly.Rules

open Polly
open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

// shims for F# < 4, can be removed if we stop supporting that
module private Option =
    let toNullable = function Some x -> Nullable x | None -> Nullable ()

type BreakerConfig = { window: TimeSpan; minThroughput: int; errorRateThreshold: float; retryAfter: TimeSpan; dryRun: bool }
type BulkheadConfig = { dop: int; queue: int; dryRun: bool }
type CutoffConfig = { timeout: TimeSpan; sla: TimeSpan option; dryRun: bool }
type PolicyConfig =
    {   // Prettify DumpState diagnostic output - this structure is not roundtripped but isolated circuits stand out better when rare
        [<Newtonsoft.Json.JsonProperty(DefaultValueHandling=Newtonsoft.Json.DefaultValueHandling.Ignore)>]
        isolate: bool
        cutoff: CutoffConfig option; limit: BulkheadConfig option; breaker: BreakerConfig option }

type GovernorState = { circuitState : string option; bulkheadAvailable : int option; queueAvailable : int option }

type Polly.Context with
    member __.Log = __.Item("log") :?> Serilog.ILogger

/// Translates a PolicyConfig's rules to a Polly IAsyncPolicy instance that gets held in the ActionPolicy
type Governor
    (   stateLog: Serilog.ILogger, buildFailurePolicy: unit -> Polly.PolicyBuilder,
        serviceName: string, callName: string, policyName: string, config: PolicyConfig) =
    let logBreach log sla interval = log |> Events.Log.cutoffSlaBreached (serviceName, callName, policyName) sla interval
    let logTimeout log (config: CutoffConfig) interval =
        let cfg = config.dryRun, ({ timeout = config.timeout; sla = Option.toNullable config.sla } : Events.CutoffParams)
        log |> Events.Log.cutoffTimeout (serviceName, callName, policyName) cfg interval
    let maybeCutoff: Timeout.TimeoutPolicy option =
        match config.cutoff with
        // NB this is Optimistic (aka correct ;) https://github.com/App-vNext/Polly/wiki/Timeout mode for a reason
        // (i.e. giving up on work early while it still consumes a thread, assisting a bug or adversary to DOS you)
        // In the general case, it's always possible to write an async expression that honors cancellation
        // If anyone tries to insist on using Pessimistic mode, this should instead by accomplished by explicitly having
        // the inner computation cut the orphan work adrift in the place where the code requires such questionable semantics
        | Some cutoff when not cutoff.dryRun ->
            stateLog.Debug("Establishing TimeoutAsync for {service:l}-{call:l}: {timeout}", serviceName, callName, cutoff.timeout)
            Some <| Policy.TimeoutAsync(cutoff.timeout)
        | _ -> None
    let logQueuing log = log |> Events.Log.queuing (serviceName, callName)
    let logDeferral log interval concurrencyLimit = log |> Events.Log.deferral (serviceName, callName, policyName) interval concurrencyLimit
    let logShedding log config = log |> Events.Log.shedding (serviceName, callName, policyName) config
    let logSheddingDryRun log = log |> Events.Log.sheddingDryRun (serviceName, callName)
    let logQueuingDryRun log = log |> Events.Log.queuingDryRun (serviceName, callName)
    let maybeBulkhead : Bulkhead.BulkheadPolicy option =
        match config.limit with
        | None -> None
        | Some limit ->
            let logRejection (c: Context) : Task = logShedding c.Log { dop = limit.dop; queue = limit.queue } ; Task.CompletedTask
            let effectiveLimit = if limit.dryRun then Int32.MaxValue else limit.dop // https://github.com/App-vNext/Polly/issues/496#issuecomment-420183946
            stateLog.Debug("Establishing BulkheadAsync for {service:l}-{call:l} {effectiveLimit}+{queue}", serviceName, callName, effectiveLimit, limit.queue)
            Some <| Policy.BulkheadAsync(maxParallelization = effectiveLimit, maxQueuingActions = limit.queue, onBulkheadRejectedAsync = logRejection)
    let logBreaking (exn : exn) (timespan: TimeSpan) =
        match config with
        | { isolate = true } -> ()
        | { breaker = Some { dryRun = true; retryAfter = t } } -> stateLog |> Events.Log.breakingDryRun exn (serviceName, callName) t
        | _ -> stateLog |> Events.Log.breaking exn (serviceName, callName) timespan
    let logHalfOpen () = stateLog |> Events.Log.halfOpen (serviceName, callName)
    let logReset () = stateLog |> Events.Log.reset (serviceName, callName)
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
            stateLog.Debug("Establishing AdvancedCircuitBreakerAsync for {service:l}-{call:l}", serviceName, callName)
            buildFailurePolicy()
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
    let asyncPolicy : IAsyncPolicy option =
        [|  match maybeCutoff with Some x -> yield x :> IAsyncPolicy | _ -> ()
            match maybeBulkhead with Some x -> yield x :> IAsyncPolicy | _ -> ()
            match maybeCb with Some x -> yield x :> IAsyncPolicy | _ -> () |]
        |> function
            | [||] ->
                match config.cutoff with
                | Some { dryRun=true } -> Policy.NoOpAsync() :> IAsyncPolicy |> Some
                | _ -> None
            | [|x|] -> Some x
            | xs -> Policy.WrapAsync(xs) :> IAsyncPolicy |> Some
    /// This Exception filter is intended to perform logging as a side-effect without entering a catch and/or having to be re-raised, hence wierd impl
    let (|LogWhenRejectedFilter|_|) log (processingInterval : Lazy<Events.StopwatchInterval>) (ex : exn) =
        match ex with
        | :? Polly.CircuitBreaker.IsolatedCircuitException ->
            log |> Events.Log.actionIsolated (serviceName, callName, policyName)
            None
        | :? Polly.CircuitBreaker.BrokenCircuitException ->
            let config : Events.BreakerParams =
                // TODO figure out why/how this can happen
                match config.breaker with
                | Some c -> { window = c.window; minThroughput = c.minThroughput; errorRateThreshold = c.errorRateThreshold }
                | None -> Unchecked.defaultof<_>
            log |> Events.Log.actionBroken (serviceName, callName, policyName) config
            None
        | :? Polly.Timeout.TimeoutRejectedException as e ->
            logTimeout log config.cutoff.Value <| processingInterval.Force()
            None
        | _ when true ->
            None
        | _ ->
            invalidOp "not possible"
            Some () // Compiler gets too clever if we never return Some

    /// Execute and/or log failures regarding invocation of a function with the relevant policy applied
    member __.Execute(inner : Async<'a>, ?log) : Async<'a> =
        let callLog = defaultArg log stateLog
        match asyncPolicy with
        | None ->
            callLog.Debug("Policy Execute Raw {service:l}-{call:l}", serviceName, callName)
            inner
        | Some polly -> async {
            let mutable wasFull = false
            // NB This logging is on a best-effort basis - obviously the guard here has an implied race condition
            // queing might not be needed and/or the request might instead be shed
            match config, maybeBulkhead with
            | { limit = Some { dryRun = false } }, Some bh ->
                if bh.BulkheadAvailableCount = 0 && bh.QueueAvailableCount <> 0 then
                    logQueuing callLog
            // As we'll be let straight through unencumbered in dryRun mode, but might be in a race to start, note whether queuing was aleady in effect
            | { limit = Some ({ dryRun = true } as limit) }, Some bh ->
                let activeCount = Int32.MaxValue - bh.BulkheadAvailableCount
                if activeCount > limit.dop then wasFull <- true
            | _ -> ()

            let startTicks = Stopwatch.GetTimestamp()
            let jitProcessingInterval =
                lazy
                    let processingCompletedTicks = Stopwatch.GetTimestamp()
                    Events.StopwatchInterval(startTicks, processingCompletedTicks)

            let startInnerTask (_: Context) (pollyCt: CancellationToken) =
                // As we'll be let straight through unencumbered in dryRun mode, we do the checks at the point where we're being scheduled to run
                match config, maybeBulkhead with
                | { limit = Some ({ dryRun = true } as limit) }, Some bh ->
                    let activeCount = Int32.MaxValue - bh.BulkheadAvailableCount
                    if activeCount > limit.dop + limit.queue then logSheddingDryRun callLog
                    elif activeCount > limit.dop && wasFull then logQueuingDryRun callLog
                | { limit = Some ({ dryRun = false } as limit) }, _ ->
                    let commenceProcessingTicks = Stopwatch.GetTimestamp()
                    let deferralInterval = Events.StopwatchInterval(startTicks, commenceProcessingTicks)
                    if (let e = deferralInterval.Elapsed in e.TotalMilliseconds) > 1. then
                        logDeferral callLog deferralInterval limit.dop
                | _ -> ()

                // sic - cancellation of the inner computation needs to be controlled by Polly's chain of handlers
                // for example, if a cutoff is configured, it's Polly that will be requesting the cancellation
                Async.StartAsTask(inner, cancellationToken=pollyCt)
            let execute = async {
                let! ct = Async.CancellationToken // Grab async cancellation token of this `Execute` call, so cancellation gets propagated into the Polly [wrap]
                callLog.Debug("Policy Execute Inner {service:l}-{call:l}", serviceName, callName)
                let ctx = Seq.singleton ("log", box callLog) |> dict
                try return! polly.ExecuteAsync(startInnerTask, ctx, ct) |> Async.AwaitTaskCorrect
                // TODO find/add a cleaner way to use the Polly API to log when the event fires due to the the circuit being Isolated/Broken
                with LogWhenRejectedFilter callLog jitProcessingInterval -> return! invalidOp "not possible; Filter always returns None" }
            match config.cutoff with
            | None | Some { sla=None; dryRun=false } ->
                return! execute
            | Some ({ timeout=timeout; sla=sla; dryRun = dryRun } as cutoffConfig)->
                try return! execute
                finally
                    if not jitProcessingInterval.IsValueCreated then
                        let processingInterval = jitProcessingInterval.Force()
                        let elapsed = processingInterval.Elapsed
                        stateLog.Debug("Policy Executed in {elapsedMs} {service:l}-{call:l}", elapsed.TotalMilliseconds, serviceName, callName)
                        if not config.isolate then
                            match sla with
                            | _ when elapsed > timeout && dryRun -> logTimeout callLog cutoffConfig processingInterval
                            | Some sla when elapsed > sla -> logBreach callLog sla processingInterval
                            | _ -> () }

    /// Diagnostic state
    member __.InternalState : GovernorState =
        {   circuitState = maybeCb |> Option.map (fun cb -> string cb.CircuitState)
            bulkheadAvailable = maybeBulkhead |> Option.map (fun bh -> bh.BulkheadAvailableCount)
            queueAvailable = maybeBulkhead |> Option.map (fun bh -> bh.QueueAvailableCount) }

type [<RequireQualifiedAccess>] ChangeLevel = Added | ConfigurationAndPolicy | Configuration | Policy

type CallConfig<'TCallConfig> = { policyName: string; policy: PolicyConfig; config: 'TCallConfig }

/// Holds a policy and configuration for a Service Call, together with the state required to govern the incoming calls
type CallPolicy<'TConfig when 'TConfig: equality> (makeGoverner : CallConfig<'TConfig> -> Governor, cfg : CallConfig<'TConfig>) =
    let mutable cfg, governor = cfg, makeGoverner cfg

    /// Ingest an updated set of config values, reporting diffs, if any
    member __.TryUpdate(updated : CallConfig<'TConfig>) =
        let level =
            match updated.policy = cfg.policy, updated.config = cfg.config with
            | false, false -> Some ChangeLevel.ConfigurationAndPolicy
            | true, false -> Some ChangeLevel.Configuration
            | false, true -> Some ChangeLevel.Policy
            | true, true -> None

        match level with
        | Some ChangeLevel.ConfigurationAndPolicy | Some ChangeLevel.Policy ->
            governor <- makeGoverner updated
            cfg <- updated
        | Some ChangeLevel.Configuration ->
            cfg <- updated
        | _ -> ()

        level

    member __.Policy = cfg.policy
    member __.Config = cfg.config

    /// Execute the call, apply the policy rules
    member __.Execute(inner : Async<'t>, ?log) =
        governor.Execute(inner,?log=log)

    /// Facilitates dumping for diagnostics
    member __.InternalState =
        cfg, governor.InternalState

type ServiceConfig<'TConfig> =
    {   defaultCallName: string option
        serviceName: string
        callsMap: (string*CallConfig<'TConfig>) list }

/// Maintains a set of call policies for a service
type private ServicePolicy<'TConfig when 'TConfig: equality> private
    (   makeCallPolicy : (*callName:*) string -> CallConfig<'TConfig> -> CallPolicy<'TConfig>,
        defaultCallName : string option,
        calls: ConcurrentDictionary<(*callName:*) string,CallPolicy<'TConfig>>) =
    let mutable defaultCallName = defaultCallName

    new (makeCallPolicy, cfg: ServiceConfig<'TConfig>) =
        let inputs = seq {
            for callName,callCfg in cfg.callsMap ->
                KeyValuePair(callName, makeCallPolicy callName callCfg) }
        ServicePolicy<_>(makeCallPolicy, cfg.defaultCallName, ConcurrentDictionary inputs)

    /// Searches for the CallPolicy associated with `call`
    member __.TryFind(callName : string) =
        match calls.TryGetValue callName with
        | true, callPolicy -> Some callPolicy
        | false, _ -> None

    /// Searches for the default CallPolicy defined for this service (if any)
    member __.TryDefaultPolicy() : CallPolicy<'TConfig> option =
        match defaultCallName with
        | None -> None
        | Some def ->
            match calls.TryGetValue def with
            | true, callPolicy -> Some callPolicy
            | false, _ -> None

    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(cfg: ServiceConfig<'TConfig>) =
        let updates = [
            for callName,cfg in cfg.callsMap do
                let mutable changeLevel = None
                let createCallPolicy (callName : string) : CallPolicy<'TConfig> =
                    changeLevel <- Some ChangeLevel.Added
                    makeCallPolicy callName cfg
                let tryUpdate _ (current:CallPolicy<'TConfig>) =
                    changeLevel <- current.TryUpdate(cfg)
                    current
                calls.AddOrUpdate(callName, createCallPolicy, tryUpdate) |> ignore
                match changeLevel with
                | None -> ()
                | Some changeLevel -> yield callName, changeLevel ]
        defaultCallName <- cfg.defaultCallName
        updates

    /// Facilitates dumping for diagnostics
    member __.InternalState = seq {
        for KeyValue(callName, call) in calls ->
            callName, call.InternalState
    }

/// Maintains an application-wide set of service policies, together with their policies, configuration and policy-state
type Policy<'TConfig when 'TConfig : equality> private (makeCallPolicy, services: ConcurrentDictionary<string,ServicePolicy<'TConfig>>) =

    /// Initialize the policy from an initial set of configs
    new(log, createFailurePolicyBuilder : CallConfig<'TConfig> -> PolicyBuilder, cfgs: ((*service:*)string * ServiceConfig<'TConfig>) seq) =
        let makeGovernor serviceName callName (callConfig: CallConfig<_>) : Governor =
            Governor(log, (fun () -> createFailurePolicyBuilder callConfig), serviceName, callName, callConfig.policyName, callConfig.policy)
        let makeCallPolicy serviceName callName (callConfig: CallConfig<_>) : CallPolicy<'TConfig> =
            CallPolicy(makeGovernor serviceName callName, callConfig)
        let inputs = seq {
            for service, cfg in cfgs ->
                KeyValuePair(service, ServicePolicy<_>(makeCallPolicy cfg.serviceName, cfg)) }
        Policy<'TConfig>(makeCallPolicy, ConcurrentDictionary<_,_> inputs)

    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(cfgs: ((*callName:*) string * ServiceConfig<_>) seq) =
        let updates = ResizeArray()
        for callName, cfg in cfgs do
            let createServicePolicy _callName =
                ServicePolicy(makeCallPolicy cfg.serviceName, cfg)
            let tryUpdate (current : ServicePolicy<_>) =
                updates.Add(callName, current.UpdateFrom cfg)
                current
            services.AddOrUpdate(callName, createServicePolicy, fun _ current -> tryUpdate current) |> ignore
        updates.ToArray() |> List.ofArray

    /// Attempts to find the policy for the specified `call` in the specified `service`
    member __.TryFind(service, call) : CallPolicy<_> Option =
        match services.TryGetValue service with
        | false, _ -> None
        | true, sp ->
            match sp.TryFind call with
            | Some cp -> Some cp
            | None -> None

    /// Finds the policy for the specified `call` in the specified `service`
    /// Throws InvalidOperationException if `service` not found
    /// Throws InvalidOperationException if `call` not defined in ServicePolicy and no default CallPolicy nominated
    member __.Find(service, call) : CallPolicy<_> =
        match services.TryGetValue service with
        | false, _ -> invalidOp <| sprintf "Undefined service '%s'" service
        | true, sp ->
            match sp.TryFind call with
            | Some cp -> cp
            | None ->
                match sp.TryDefaultPolicy() with
                | None -> invalidOp <| sprintf "Service '%s' does not define a default call policy" service
                | Some dp -> dp

    /// Facilitates dumping all Service Configs for diagnostics
    member __.InternalState = seq {
        for KeyValue(serviceName, service) in services ->
            serviceName, service.InternalState }