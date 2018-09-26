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

type Event =
    | Isolated of action: string
    | Broken of action: string
    | Deferred of action: string * interval: StopwatchInterval
    | Shed of action: string

module internal Log =
    open Serilog.Events

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let private forEvent (value : Event) (log : Serilog.ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty(Constants.EventPropertyName, ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

    (* Circuit breaker rejections *)

    let actionIsolated (log: Serilog.ILogger) policyName actionName =
        let lfe = log |> forEvent (Isolated actionName)
        lfe.Warning("Circuit Isolated for {actionName} based on {policy} policy", actionName, policyName)
    let actionBroken (log: Serilog.ILogger) policyName actionName breakerConfig =
        let lfe = log |> forEvent (Broken actionName)
        lfe.Warning("Circuit Broken for {actionName} based on {policy}: {@breakerConfig}", actionName, policyName, breakerConfig)

    (* Circuit Breaker state transitions *)

    let breaking (exn: exn) (actionName: string) (timespan: TimeSpan) (log : Serilog.ILogger) =
        log.Warning(exn, "Circuit Breaking for {actionName} for {duration}", actionName, timespan)
    let breakingDryRun (exn: exn) (actionName: string) (timespan: TimeSpan) (log : Serilog.ILogger) =
        log.Warning(exn, "Circuit DRYRUN Breaking for {actionName} for {duration}", actionName, timespan)
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
    let shedding (policyName: string) (actionName: string) bulkheadConfig (log : Serilog.ILogger) =
        let lfe = log |> forEvent (Shed actionName)
        lfe.Warning("Bulkhead Shedding for {actionName} based on {policy}: {@bulkheadConfig}", actionName, policyName, bulkheadConfig)
    let queuingDryRun (actionName: string) (log : Serilog.ILogger) =
        log.Information("Bulkhead DRYRUN Queuing for {actionName}", actionName)
    let sheddingDryRun (actionName: string) (log : Serilog.ILogger) =
        log.Warning("Bulkhead DRYRUN Shedding for {actionName}", actionName)