module CallPolly.Events

open System
open System.Collections.Generic

/// Represents a time measurement of a computation that includes stopwatch tick metadata
[<NoComparison; NoEquality>]
type StopwatchInterval (startTicks : int64, endTicks : int64) =
    do if startTicks < 0L || startTicks > endTicks then invalidArg "ticks" "tick arguments do not form a valid interval."
    member __.StartTicks = startTicks
    member __.EndTicks = endTicks
    member __.Elapsed = TimeSpan.FromStopwatchTicks(endTicks - startTicks)
    override __.ToString() = string __.Elapsed

module Constants =
    let [<Literal>] EventPropertyName = "cpe"

type BreakerParams = { window: TimeSpan; minThroughput: int; errorRateThreshold: float }

[<NoComparison>]
type BulkheadParams =
    | Any of GlobalBulkheadParams
    | Tagged of TaggedBulkheadParams
and [<NoComparison>] GlobalBulkheadParams = { dop: int; queue: int }
and [<NoComparison>] TaggedBulkheadParams = { tag: string; dop: int; queue: int }

[<NoComparison; NoEquality>]
type CutoffParams = { timeout: TimeSpan; sla: Nullable<TimeSpan> }

[<NoComparison; NoEquality>]
type Event =
    | Isolated of service: string * call: string
    | Broken of service: string * call: string * config: BreakerParams
    | Deferred of service: string * call: string * tags: IReadOnlyDictionary<string,string> * interval: StopwatchInterval
    | Shed of service: string * call: string * tags: IReadOnlyDictionary<string,string> * config: BulkheadParams
    | Breached of service: string * call: string * sla: TimeSpan * interval: StopwatchInterval
    | Canceled of service: string * call: string * config: CutoffParams * interval: StopwatchInterval
    override __.ToString() = "(Metrics)"

module internal Log =
    open Serilog.Events

    // Serilog will grab [only] real Dictionaries (not IDictionary or IReadOnlyDictionary), so we need to wrap them like this for them to be captured as maps
    let private makePropertyForCapturableAsMap (rod: IReadOnlyDictionary<_,_>) : Dictionary<_,_> =
        let d = Dictionary()
        for KeyValue (k,v) in rod do d.Add(k,v)
        d

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let private forEvent (value : Event) (log : Serilog.ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty(Constants.EventPropertyName, ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

    (* Circuit breaker rejections *)

    let actionIsolated (service: string, call:string, policy:string) (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Isolated (service,call))
        lfe.Warning("Circuit Isolated for {service:l}-{call:l} based on {policy:l} policy", service, call, policy)
    let actionBroken (service: string, call:string, policy:string) (config: BreakerParams) (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Broken (service,call,config))
        lfe.Warning("Circuit Broken for {service:l}-{call:l} based on {policy:l}: {@breakerConfig}", service, call, policy, config)

    (* Circuit Breaker state transitions *)

    let breaking (exn: exn) (service: string, call:string) (timespan: TimeSpan) (log : Serilog.ILogger) =
        log.Warning(exn, "Circuit Breaking for {service:l}-{call:l} for {duration}", service, call, timespan)
    let breakingDryRun (exn: exn) (service: string, call:string) (timespan: TimeSpan) (log : Serilog.ILogger) =
        log.ForContext("dryRun",true).Warning(exn, "Circuit DRYRUN Breaking for {service:l}-{call:l} for {duration}", service, call, timespan)
    let halfOpen (service: string, call:string) (log : Serilog.ILogger) =
        log.Information("Circuit Pending Reopen for {service:l}-{call:l}", service, call)
    let reset (service: string, call:string) (log : Serilog.ILogger) =
        log.Information("Circuit Reset for {service:l}-{call:l}", service, call)

    (* Bulkhead queuing and rejections *)

    // NB this is not guaranteed to reflect actual queueing
    // - in some cases, queueing may not take place
    // - in others, the call will eventually fall victim to shedding
    let queuing (service: string, call:string) (log : Serilog.ILogger) =
        log.Information("Bulkhead Queuing likely for {service:l}-{call:l}", service, call)
    // Deferral can happen due to either a Limit or a LimitBy
    let deferral
            (service: string, call:string, policy:string, configLimits: IReadOnlyDictionary<string,int>)
            (interval: StopwatchInterval)
            (tags: IReadOnlyDictionary<string,string>)
            (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Deferred (service,call,tags,interval))
        match tags with
        | dict when dict.Count = 0 ->
            lfe.Warning("Bulkhead Delayed {service:l}-{call:l} for {timespan} due to {@maxParallel} in {policy:l}",
                service, call, interval.Elapsed, makePropertyForCapturableAsMap configLimits, policy)
        | tags ->
            lfe.Warning("Bulkhead Delayed {service:l}-{call:l} for {timespan} due to {@maxParallel} with {@tags} in {policy:l}",
                service, call, interval.Elapsed, makePropertyForCapturableAsMap configLimits, makePropertyForCapturableAsMap tags, policy)
    // Shedding can happen due to either a Limit or a LimitBy
    let shedding (service: string, call: string, policy: string) (tags: IReadOnlyDictionary<string,string>, config:BulkheadParams) (log : Serilog.ILogger) =
        let lfe = log |> forEvent (Shed (service,call,tags,config))
        let flatConfig = match config with Any a -> box a | Tagged t -> box t
        match tags with
        | dict when dict.Count = 0 ->
            lfe.Warning("Bulkhead Shedding for {service:l}-{call:l} based on {policy:l}: {@bulkheadConfig}",
                service, call, policy, flatConfig)
        | tags ->
            lfe.Warning("Bulkhead Shedding for {service:l}-{call:l} with {@tags} based on {policy:l}: {@bulkheadConfig}",
                service, call, makePropertyForCapturableAsMap tags, policy, flatConfig)
    let queuingDryRun (service: string, call:string) (log : Serilog.ILogger) =
        log.ForContext("dryRun",true).Information("Bulkhead DRYRUN Queuing for {service:l}-{call:l}", service, call)
    // Only implemented for Limit, not LimitBy
    let sheddingDryRun (service: string, call:string) (log : Serilog.ILogger) =
        log.ForContext("dryRun",true).Warning("Bulkhead DRYRUN Shedding for {service:l}-{call:l}", service, call)

    (* Cutoff violations and abandonments *)

    let cutoffSlaBreached (service: string, call:string, policy:string) (sla: TimeSpan) (interval: StopwatchInterval) (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Breached (service, call,sla,interval))
        lfe.Warning("Cutoff {service:l}-{call:l} {durationMs} breached SLA {slaMs} in {policy:l}",
            service, call, (let t = interval.Elapsed in t.TotalMilliseconds), sla.TotalMilliseconds, policy)
    let cutoffTimeout (service: string, call:string, policy:string) (dryRun, config : CutoffParams) (interval: StopwatchInterval) (log : Serilog.ILogger) =
        if dryRun then
            let lfe = log.ForContext("dryRun",true)
            lfe.Warning("Cutoff DRYRUN {service:l}-{call:l} {durationMs} (cancelation would have been requested on {timeoutMs}) in {policy:l}",
                service, call, (let t = interval.Elapsed in t.TotalMilliseconds), config.timeout.TotalMilliseconds, policy)
        else
            let lfe = log |> forEvent (Canceled (service, call,config,interval))
            lfe.Warning("Cutoff {service:l}-{call:l} {durationMs} canceled after {timeoutMs} in {policy:l}: {@cutoffConfig}",
                service, call, (let t = interval.Elapsed in t.TotalMilliseconds), config.timeout.TotalMilliseconds, policy, config)