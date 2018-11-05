module CallPolly.Governor

open Polly
open System
open System.Collections.Generic
open System.Collections.ObjectModel
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

// shims for F# < 4, can be removed if we stop supporting that
module private Option =
    let toNullable = function Some x -> Nullable x | None -> Nullable ()

type BreakerConfig = { window: TimeSpan; minThroughput: int; errorRateThreshold: float; retryAfter: TimeSpan; dryRun: bool }
type BulkheadConfig = { dop: int; queue: int; dryRun: bool }
type TaggedBulkheadConfig = { tag: string; dop: int; queue: int }
type CutoffConfig = { timeout: TimeSpan; sla: TimeSpan option; dryRun: bool }
type PolicyConfig =
    {   // Prettify DumpState diagnostic output - this structure is not roundtripped but isolated circuits stand out better when rare
        [<Newtonsoft.Json.JsonProperty(DefaultValueHandling=Newtonsoft.Json.DefaultValueHandling.Ignore)>]
        isolate: bool
        cutoff: CutoffConfig option; limit: BulkheadConfig option; taggedLimits: TaggedBulkheadConfig list; breaker: BreakerConfig option }

type GovernorState =
    {   circuitState : string option;
        bulkheadAvailable : int option; queueAvailable : int option
        // sic - array, rendered as null so it does not get rendered where not applicable
        taggedAvailabilities : TaggedBulkheadState[] }
and TaggedBulkheadState =
    {   tag: string
        items: MultiBulkheadState list }
and MultiBulkheadState =
    {   value: string
        bulkheadAvailable : int
        queueAvailable : int }

let emptyRod : IReadOnlyDictionary<string,string> = ReadOnlyDictionary(Dictionary()) :> _

type Polly.Context with
    member __.Log = __.Item("log") :?> Serilog.ILogger
    member __.Tags : IReadOnlyDictionary<string,string> =
        match __.TryGetValue "tags" with
        | true, (:? IReadOnlyDictionary<string,string> as v) -> v
        | _ -> emptyRod

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
    let logDeferral log interval tags =
        let mkRod pairs : IReadOnlyDictionary<string,_> = System.Collections.ObjectModel.ReadOnlyDictionary(pairs |> dict) :> _
        let configLimits = mkRod <| seq {
            match config.limit with None -> () | Some limit -> yield "any", limit.dop
            for limitBy in config.taggedLimits do yield limitBy.tag, limitBy.dop }
        log |> Events.Log.deferral (serviceName, callName, policyName, configLimits) interval tags
    let logShedding log config = log |> Events.Log.shedding (serviceName, callName, policyName) config
    let logSheddingDryRun log = log |> Events.Log.sheddingDryRun (serviceName, callName)
    let logQueuingDryRun log = log |> Events.Log.queuingDryRun (serviceName, callName)
    let maybeBulkhead : Bulkhead.BulkheadPolicy option =
        match config.limit with
        | None -> None
        | Some limit ->
            let logRejection (c: Context) : Task = logShedding c.Log (c.Tags, Events.Any {dop = limit.dop; queue = limit.queue }) ; Task.CompletedTask
            let effectiveLimit = if limit.dryRun then Int32.MaxValue else limit.dop // https://github.com/App-vNext/Polly/issues/496#issuecomment-420183946
            stateLog.Debug("Establishing BulkheadAsync for {service:l}-{call:l} {effectiveLimit}+{queue}", serviceName, callName, effectiveLimit, limit.queue)
            Some <| Policy.BulkheadAsync(maxParallelization = effectiveLimit, maxQueuingActions = limit.queue, onBulkheadRejectedAsync = logRejection)
    let multiBulkheads : BulkheadMulti.BulkheadMultiAsyncPolicy list =
        [ for limitBy in config.taggedLimits ->
            let logRejection (c: Context) = logShedding c.Log (c.Tags, Events.Tagged { tag = limitBy.tag; dop = limitBy.dop; queue = limitBy.queue })
            stateLog.Debug("Establishing BulkheadAsyncMulti by {tag} for {service:l}-{call:l} {effectiveLimit}+{queue}", limitBy.tag, serviceName, callName, limitBy.dop, limitBy.queue)
            let tryGetTagValue (c: Context) =
                match c.Tags.TryGetValue limitBy.tag with
                | true, value -> Some value
                | _ -> None
            BulkheadMulti.Policy.BulkheadMultiAsync(tag = limitBy.tag, maxParallelization = limitBy.dop, maxQueuingActions = limitBy.queue, tryGetTagValue=tryGetTagValue, onBulkheadRejected = logRejection) ]
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
            for b in multiBulkheads do yield b :> IAsyncPolicy
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
        | :? Polly.Timeout.TimeoutRejectedException ->
            logTimeout log config.cutoff.Value <| processingInterval.Force()
            None
        | _ when 1=2 ->
            invalidOp "not possible"
            Some () // Compiler gets too clever if we never return Some
        | _ ->
            None

    /// Execute and/or log failures regarding invocation of a function with the relevant policy applied
    member __.Execute(inner : Async<'a>, ?log : Serilog.ILogger, ?tags: IReadOnlyDictionary<string,string>) : Async<'a> =
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
                | { limit = Some { dryRun = false } }, _ ->
                    let commenceProcessingTicks = Stopwatch.GetTimestamp()
                    let deferralInterval = Events.StopwatchInterval(startTicks, commenceProcessingTicks)
                    if (let e = deferralInterval.Elapsed in e.TotalMilliseconds) > 1. then
                        logDeferral callLog deferralInterval (defaultArg tags emptyRod)
                | _ -> ()

                // sic - cancellation of the inner computation needs to be controlled by Polly's chain of handlers
                // for example, if a cutoff is configured, it's Polly that will be requesting the cancellation
                Async.StartAsTask(inner, cancellationToken=pollyCt)
            let execute = async {
                let! ct = Async.CancellationToken // Grab async cancellation token of this `Execute` call, so cancellation gets propagated into the Polly [wrap]
                callLog.Debug("Policy Execute Inner {service:l}-{call:l}", serviceName, callName)
                let contextData = dict <| seq {
                    yield ("log", box callLog)
                    match tags with
                    | None -> ()
                    | Some (t : IReadOnlyDictionary<string,string>) -> yield "tags", box t }
                try return! polly.ExecuteAsync(startInnerTask, contextData, ct) |> Async.AwaitTaskCorrect
                // TODO find/add a cleaner way to use the Polly API to log when the event fires due to the the circuit being Isolated/Broken
                with LogWhenRejectedFilter callLog jitProcessingInterval -> return! invalidOp "not possible; Filter always returns None" }
            match config.cutoff with
            | None | Some { sla=None; dryRun=false } ->
                return! execute
            | Some ({ timeout=timeout; sla=sla; dryRun = dryRun } as cutoffConfig) ->
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
            queueAvailable = maybeBulkhead |> Option.map (fun bh -> bh.QueueAvailableCount)
            taggedAvailabilities =
                match multiBulkheads with
                | [] -> null
                | tbhs ->
                    [| for tbh in tbhs ->
                        {   tag = tbh.Tag
                            items =
                                [ for KeyValue(tagValue,bh) in tbh.DumpState() ->
                                    {   value = tagValue
                                        bulkheadAvailable = bh.BulkheadAvailableCount
                                        queueAvailable = bh.QueueAvailableCount } ] } |]}