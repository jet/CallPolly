module CallPolly.Rules

open Polly
open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

type BreakerConfig = { window: TimeSpan; minThroughput: int; errorRateThreshold: float; retryAfter: TimeSpan; dryRun: bool }
type BulkheadConfig = { dop: int; queue: int; dryRun: bool }
type CutoffConfig = { timeout: TimeSpan; sla: TimeSpan option; dryRun: bool }
type PolicyConfig = { isolate: bool; cutoff: CutoffConfig option; limit: BulkheadConfig option; breaker: BreakerConfig option }

type GovernorState =
    {   circuitState : string option
        bulkheadAvailable : int option
        queueAvailable : int option }

/// Translates a PolicyConfig's rules to a Polly IAsyncPolicy instance that gets held in the ActionPolicy
type Governor(log: Serilog.ILogger, actionName : string, policyName: string, config : PolicyConfig) =
    let logBreach sla interval = log |> Events.Log.cutoffSlaBreached policyName actionName sla interval
    let logTimeout (config: CutoffConfig) interval =
        let cfg = config.dryRun, ({ timeout = config.timeout; sla = Option.toNullable config.sla } : Events.CutoffParams)
        log |> Events.Log.cutoffTimeout policyName actionName cfg interval
    let maybeCutoff: Timeout.TimeoutPolicy option =
        match config.cutoff with
        // NB this is Optimistic (aka correct ;) https://github.com/App-vNext/Polly/wiki/Timeout mode for a reason
        // (i.e. giving up on work early while it still consumes a thread, assisting a bug or adversary to DOS you)
        // In the general case, it's always possible to write an async expression that honors cancellation
        // If anyone tries to insist on using Pessimistic mode, this should instead by accomplished by explicitly having
        // the inner computation cut the orphan work adrift in the place where the code requires such questionable semantics
        | Some cutoff when not cutoff.dryRun -> Some <| Policy.TimeoutAsync(cutoff.timeout)
        | _ -> None
    let logQueuing() = log |> Events.Log.queuing actionName
    let logDeferral interval concurrencyLimit = log |> Events.Log.deferral policyName actionName interval concurrencyLimit
    let logShedding config = log |> Events.Log.shedding policyName actionName config
    let logSheddingDryRun () = log |> Events.Log.sheddingDryRun actionName
    let logQueuingDryRun () = log |> Events.Log.queuingDryRun actionName
    let maybeBulkhead : Bulkhead.BulkheadPolicy option =
        match config.limit with
        | None -> None
        | Some limit ->
            let logRejection (_: Context) : Task = logShedding { dop = limit.dop; queue = limit.queue } ; Task.CompletedTask
            let effectiveLimit = if limit.dryRun then Int32.MaxValue else limit.dop // https://github.com/App-vNext/Polly/issues/496#issuecomment-420183946
            Some <| Policy.BulkheadAsync(maxParallelization = effectiveLimit, maxQueuingActions = limit.queue, onBulkheadRejectedAsync = logRejection)
    let logBreaking (exn : exn) (timespan: TimeSpan) =
        match config with
        | { isolate = true } -> ()
        | { breaker = Some { dryRun = true; retryAfter = t } } -> log |> Events.Log.breakingDryRun exn actionName t
        | _ -> log |> Events.Log.breaking exn actionName timespan
    let logHalfOpen () = log |> Events.Log.halfOpen actionName
    let logReset () = log |> Events.Log.reset actionName
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
    let (|LogWhenRejectedFilter|_|) (processingInterval : Lazy<Events.StopwatchInterval>) (ex : exn) =
        match ex with
        | :? Polly.CircuitBreaker.IsolatedCircuitException ->
            log |> Events.Log.actionIsolated policyName actionName
            None
        | :? Polly.CircuitBreaker.BrokenCircuitException ->
            let c = config.breaker.Value
            let config : Events.BreakerParams = { window = c.window; minThroughput = c.minThroughput; errorRateThreshold = c.errorRateThreshold }
            log |> Events.Log.actionBroken policyName actionName config
            None
        | :? Polly.Timeout.TimeoutRejectedException ->
            logTimeout config.cutoff.Value <| processingInterval.Force()
            None
        | _ when true ->
            None
        | _ ->
            invalidOp "not possible"
            Some () // Compiler gets too clever if we never return Some

    /// Execute and/or log failures regarding invocation of a function with the relevant policy applied
    member __.Execute(inner : Async<'a>) : Async<'a> =
        match asyncPolicy with
        | None -> inner
        | Some polly -> async {
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

            let startTicks = Stopwatch.GetTimestamp()
            let jitProcessingInterval =
                lazy
                    let processingCompletedTicks = Stopwatch.GetTimestamp()
                    Events.StopwatchInterval(startTicks, processingCompletedTicks)

            let startInnerTask (pollyCt: CancellationToken) =
                // As we'll be let straight through unencumbered in dryRun mode, we do the checks at the point where we're being scheduled to run
                match config, maybeBulkhead with
                | { limit = Some ({ dryRun = true } as limit) }, Some bh ->
                    let activeCount = Int32.MaxValue - bh.BulkheadAvailableCount
                    if activeCount > limit.dop + limit.queue then logSheddingDryRun ()
                    elif activeCount > limit.dop && wasFull then logQueuingDryRun ()
                | { limit = Some ({ dryRun = false } as limit) }, _ ->
                    let commenceProcessingTicks = Stopwatch.GetTimestamp()
                    let deferralInterval = Events.StopwatchInterval(startTicks, commenceProcessingTicks)
                    if deferralInterval.Elapsed.TotalMilliseconds > 1. then
                        logDeferral deferralInterval limit.dop
                | _ -> ()

                // sic - cancellation of the inner computation needs to be controlled by Polly's chain of handlers
                // for example, if a cutoff is configured, it's Polly that will be requesting the cancellation
                Async.StartAsTask(inner, cancellationToken=pollyCt)
            let execute = async {
                let! ct = Async.CancellationToken // Grab async cancellation token of this `Execute` call, so cancellation gets propagated into the Polly [wrap]
                try return! polly.ExecuteAsync(startInnerTask, ct) |> Async.AwaitTaskCorrect
                // TODO find/add a cleaner way to use the Polly API to log when the event fires due to the the circuit being Isolated/Broken
                with LogWhenRejectedFilter jitProcessingInterval -> return! invalidOp "not possible; Filter always returns None" }
            match config.cutoff with
            | None | Some { sla=None; dryRun=false } ->
                return! execute
            | Some ({ timeout=timeout; sla=sla; dryRun = dryRun } as config)->
                try return! execute
                finally
                    if not jitProcessingInterval.IsValueCreated then
                        let processingInterval = jitProcessingInterval.Force()
                        match sla, processingInterval.Elapsed with
                        | _, elapsed when elapsed > timeout && dryRun -> logTimeout config processingInterval
                        | Some sla, elapsed when elapsed > sla -> logBreach sla processingInterval
                        | _ -> () }

    /// Diagnostic state
    member __.InternalState : GovernorState =
        {   circuitState = maybeCb |> Option.map (fun cb -> string cb.CircuitState)
            bulkheadAvailable = maybeBulkhead |> Option.map (fun bh -> bh.BulkheadAvailableCount)
            queueAvailable = maybeBulkhead |> Option.map (fun bh -> bh.QueueAvailableCount) }

type [<RequireQualifiedAccess>] ChangeLevel = Added | ConfigurationAndPolicy | Configuration | Policy

type CallConfig<'TCallConfig, 'Raw> =
    {   callName: string
        policyName: string
        policyConfig: PolicyConfig
        callConfig: 'TCallConfig
        raw: 'Raw list }

/// Holds a policy and configuration for a Service Call, together with the state required to govern the incoming calls
type CallPolicy<'TConfig,'Raw when 'TConfig: equality>(log, cfg : CallConfig<'TConfig,'Raw>) =
    let makeGoverner cfg = Governor(log, cfg.callName, cfg.policyName, cfg.policyConfig)
    let mutable cfg, governor = cfg, makeGoverner cfg

    /// Ingest an updated set of config values, reporting diffs, if any
    member __.TryUpdate(updated : CallConfig<_,_>) =
        let changes =
            match updated.policyConfig = cfg.policyConfig, updated.callConfig = cfg.callConfig with
            | true, true -> Some ChangeLevel.ConfigurationAndPolicy
            | true, false -> Some ChangeLevel.Configuration
            | false, true -> Some ChangeLevel.Policy
            | false, false -> None

        match changes with
        | Some ChangeLevel.ConfigurationAndPolicy | Some ChangeLevel.Policy ->
            governor <- makeGoverner updated
            cfg <- updated
        | Some ChangeLevel.Configuration ->
            cfg <- updated
        | _ -> ()

        changes

    member __.Policy = cfg.policyConfig
    member __.Config = cfg.callConfig
    member __.Raw = cfg.raw

    /// Execute the call, apply the policy rules
    member __.Execute(inner : Async<'t>) =
        governor.Execute inner

    /// Facilitates dumping for diagnostics
    member __.InternalState =
        cfg, governor.InternalState

type ServiceConfig<'TConfig,'Raw> =
    {   defaultPolicyName: string option
        callsMap: CallConfig<'TConfig,'Raw> list }

/// Maintains a set of call policies for a service
type ServicePolicy<'TConfig,'Raw when 'TConfig: equality> private
    (   log : Serilog.ILogger,
        defaultPolicyName : string option,
        calls: ConcurrentDictionary<string,CallPolicy<'TConfig,'Raw>>) =
    let mutable defaultPolicyName = defaultPolicyName

    /// Searches for the CallPolicy associated with `call`
    member __.TryFind(callName : string) =
        match calls.TryGetValue callName with
        | true, callPolicy -> Some callPolicy
        | false, _ -> None

    /// Searches for the default CallPolicy defined for this service (if any)
    member __.TryDefaultPolicy() : CallPolicy<'TConfig,'Raw> option =
        match defaultPolicyName with
        | None -> None
        | Some def ->
            match calls.TryGetValue def with
            | true, callPolicy -> Some callPolicy
            | false, _ -> None

    /// Attempts to parse
    /// - Policies: mapping of policy name to arrays of Rules, defined by the json in `policiesJson`
    /// - Map: the mappings of Action names to items in the Policies, defined by the json in `mapJson`
    /// Throws if policiesJson or mapJson fail to adhere to correct Json structure
    static member Create(log, cfg: ServiceConfig<'TConfig,'Raw>) =
        let inputs = seq {
            for call in cfg.callsMap ->
                KeyValuePair(call.callName, CallPolicy<_,_>(log, call)) }
        ServicePolicy<_,_>(log, cfg.defaultPolicyName, ConcurrentDictionary inputs)

    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(cfg: ServiceConfig<'TConfig,'Raw>) =
        let updates = [
            for cfg in cfg.callsMap do
                let mutable changeLevel = None
                let create () =
                    changeLevel <- Some ChangeLevel.Added
                    CallPolicy(log, cfg)
                calls.AddOrUpdate(cfg.callName, (fun _callName -> create ()), (fun _ current -> changeLevel <- current.TryUpdate(cfg); current)) |> ignore
                match changeLevel with
                | None -> ()
                | Some changeLevel -> yield cfg.callName, changeLevel ]
        defaultPolicyName <- cfg.defaultPolicyName
        updates

    /// Facilitates dumping for diagnostics
    member __.InternalState = seq {
        for KeyValue(callName, call) in calls ->
            callName, call.InternalState
    }

/// Maintains an application-wide set of service policies, together with their policies, configuration and policy-state
type Policy<'TConfig,'Raw when 'TConfig : equality> private(log, services: ConcurrentDictionary<string,ServicePolicy<'TConfig,'Raw>>) =

    /// Initialize the policy from an initial set of configs
    static member Create(log, cfgs: (string * ServiceConfig<'TConfig,'Raw>) seq) =
        let inputs = seq {
            for service, cfg in cfgs ->
                KeyValuePair(service, ServicePolicy<_,_>.Create(log, cfg)) }
        Policy<'TConfig,'Raw>(log, ConcurrentDictionary<_,_> inputs)

    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(cfgs: (string * ServiceConfig<_,_>) seq) =
        let updates = ResizeArray()
        for name, cfg in cfgs do
            let create () =
                ServicePolicy<_,_>.Create(log, cfg)
            let tryUpdate (current : ServicePolicy<_,_>) =
                updates.Add(name, current.UpdateFrom cfg)
                current
            services.AddOrUpdate(name, (fun _callName -> create ()), fun _ current -> tryUpdate current) |> ignore
        updates.ToArray() |> List.ofArray

    /// Attempts to find the policy for the specified `call` in the specified `service`
    member __.TryFind(service, call) : CallPolicy<_,_> Option =
        match services.TryGetValue service with
        | false, _ -> None
        | true, sp ->
            match sp.TryFind call with
            | Some cp -> Some cp
            | None -> None

    /// Finds the policy for the specified `call` in the specified `service`
    /// Throws InvalidOperationException if `service` not found
    /// Throws InvalidOperationException if `call` not defined in ServicePolicy and no default CallPolicy nominated
    member __.Find(service, call) : CallPolicy<_,_> =
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