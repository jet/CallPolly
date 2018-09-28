module CallPolly.Events

open System

/// Represents a time measurement of a computation that includes stopwatch tick metadata
[<NoEquality; NoComparison>]
type StopwatchInterval (startTicks : int64, endTicks : int64) =
    do if startTicks < 0L || startTicks > endTicks then invalidArg "ticks" "tick arguments do not form a valid interval."
    member __.StartTicks = startTicks
    member __.EndTicks = endTicks
    member __.Elapsed = TimeSpan.FromStopwatchTicks(endTicks - startTicks)
    override __.ToString() = string __.Elapsed

module Constants =
    let [<Literal>] EventPropertyName = "cp"

type BreakerParams = { window: TimeSpan; minThroughput: int; errorRateThreshold: float }
type BulkheadParams = { dop: int; queue: int }
type CutoffParams = { timeout: TimeSpan; sla: Nullable<TimeSpan> }

type Event =
    | Isolated of action: string
    | Broken of action: string * config: BreakerParams
    | Deferred of action: string * interval: StopwatchInterval
    | Shed of action: string * config: BulkheadParams
    | Breached of action: string * sla: TimeSpan * interval: StopwatchInterval
    | Canceled of action: string * config: CutoffParams * interval: StopwatchInterval
    override __.ToString() = "(Metrics)"

module internal Log =
    open Serilog.Events

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let private forEvent (value : Event) (log : Serilog.ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty(Constants.EventPropertyName, ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

    (* Circuit breaker rejections *)

    let actionIsolated policyName actionName (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Isolated actionName)
        lfe.Warning("Circuit Isolated for {actionName} based on {policy} policy", actionName, policyName)
    let actionBroken policyName actionName (config: BreakerParams) (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Broken (actionName,config))
        lfe.Warning("Circuit Broken for {actionName} based on {policy}: {@breakerConfig}", actionName, policyName, config)

    (* Circuit Breaker state transitions *)

    let breaking (exn: exn) (actionName: string) (timespan: TimeSpan) (log : Serilog.ILogger) =
        log.Warning(exn, "Circuit Breaking for {actionName} for {duration}", actionName, timespan)
    let breakingDryRun (exn: exn) (actionName: string) (timespan: TimeSpan) (log : Serilog.ILogger) =
        log.ForContext("dryRun",true).Warning(exn, "Circuit DRYRUN Breaking for {actionName} for {duration}", actionName, timespan)
    let halfOpen (actionName: string) (log : Serilog.ILogger) =
        log.Information("Circuit Pending Reopen for {actionName}", actionName)
    let reset (actionName: string) (log : Serilog.ILogger) =
        log.Information("Circuit Reset for {actionName}", actionName)

    (* Bulkhead queuing and rejections *)

    // NB this is not guaranteed to reflect actual queueing
    // - in some cases, queueing may not take place
    // - in others, the call will eventually fall victim to shedding
    let queuing (actionName: string) (log : Serilog.ILogger) =
        log.Information("Bulkhead Queuing likely for {actionName}", actionName)
    let deferral policyName actionName (interval : StopwatchInterval) (concurrencyLimit : int) (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Deferred (actionName,interval))
        lfe.Warning("Bulkhead Delayed {actionName} for {timespan} due to concurrency limit of {maxParallel} in {policy}",
            actionName, interval.Elapsed, concurrencyLimit, policyName)
    let shedding (policyName: string) (actionName: string) (config:BulkheadParams) (log : Serilog.ILogger) =
        let lfe = log |> forEvent (Shed (actionName,config))
        lfe.Warning("Bulkhead Shedding for {actionName} based on {policy}: {@bulkheadConfig}", actionName, policyName, config)
    let queuingDryRun (actionName: string) (log : Serilog.ILogger) =
        log.ForContext("dryRun",true).Information("Bulkhead DRYRUN Queuing for {actionName}", actionName)
    let sheddingDryRun (actionName: string) (log : Serilog.ILogger) =
        log.ForContext("dryRun",true).Warning("Bulkhead DRYRUN Shedding for {actionName}", actionName)

    (* Cutoff violations and abandonments *)

    let cutoffSlaBreached (policyName: string) (actionName: string) (sla: TimeSpan) (interval: StopwatchInterval) (log: Serilog.ILogger) =
        let lfe = log |> forEvent (Breached (actionName,sla,interval))
        lfe.Warning("Cutoff {actionName} {durationMs} breached SLA {slaMs} in {policy}",
            actionName, interval.Elapsed.TotalMilliseconds, sla.TotalMilliseconds, policyName)
    let cutoffTimeout (policyName: string) (actionName: string) (dryRun, config : CutoffParams) (interval: StopwatchInterval) (log : Serilog.ILogger) =
        if dryRun then
            let lfe = log.ForContext("dryRun",true)
            lfe.Warning("Cutoff DRYRUN {actionName} {durationMs} (cancelation would have been requested on {timeoutMs}) in {policy}",
                actionName, interval.Elapsed.TotalMilliseconds, config.timeout.TotalMilliseconds, policyName)
        else
            let lfe = log |> forEvent (Canceled (actionName,config,interval))
            lfe.Warning("Cutoff {actionName} {durationMs} canceled after {timeoutMs} in {policy}: {@cutoffConfig}",
                actionName, interval.Elapsed.TotalMilliseconds, config.timeout.TotalMilliseconds, policyName, config)