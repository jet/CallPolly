module CallPolly.Events

module Constants =
    let [<Literal>] EventPropertyName = "cp"

type Event =
    | Isolated of policy: string

module internal Log =
    open Serilog.Events

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let forEvent (value : Event) (log : Serilog.ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty(Constants.EventPropertyName, ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

    let actionIsolated (log: Serilog.ILogger) policyName actionName =
        let lfe = log |> forEvent (Isolated policyName)
        lfe.Information("Circuit Isolated for {action} based on {policy} policy", actionName, policyName)