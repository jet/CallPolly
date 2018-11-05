namespace CallPolly

open Serilog
open System

module private SerilogHelpers =
    let inline kv x y = System.Collections.Generic.KeyValuePair<_,_>(x,y)
    let inline lo x (*lp*) = Serilog.Events.StructureValue x
    let inline ls x (*lp seq*)= Serilog.Events.SequenceValue x
    let inline ld x (*lv,kvp(lv)*) = Serilog.Events.DictionaryValue x
    let inline lp n v(*:lo/lv/ls/ld*) = Serilog.Events.LogEventProperty(n, v)
    let inline lv (x:#obj) = Serilog.Events.ScalarValue(x)
    let forContextExplicit k v (log : ILogger) =
        let enrich (e : Serilog.Events.LogEvent) = e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(k, v))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

module private Impl =

    type Warning = { service: string; call: string; ruleText: string }

    let readAndParseCycle (log: ILogger) readDefinitions =
        let mutable current = readDefinitions ()
        let ingest () =
            let res = Parser.parse current
            if not (Array.isEmpty res.Warnings) then
                let msgs = seq { for w in res.Warnings -> { service=w.serviceName; call=w.callName; ruleText=string w.unknownRule } }
                log.ForContext("warnings", msgs).Warning("Policy definitions had {count} unrecognized rules", res.Warnings.Length)
            res
        let tryReadUpdates () =
            let updated = readDefinitions()
            if current = updated then None else

            current <- updated
            ingest () |> Some

        ingest(),tryReadUpdates

    open SerilogHelpers
    let logChanges (log: ILogger) (res: (string*(string*ChangeLevel) list) list) =
        let xs = res |> Seq.filter (function _,c -> not (List.isEmpty c)) |> Seq.sortBy (function _,c -> -List.length c) |> Seq.cache
        if not (Seq.isEmpty xs) then
            //.ForContext("changes", seq { for s,calls in xs do for c,change in calls -> sprintf "%s:%s:%O" s c change }, true )
            let changesAsDictionary calls = ld (seq { for callname,change in calls -> kv (lv callname) (lv (string change) :> _) })
            let dump = ls <| seq { for s,calls in xs -> lo [ lp "service" (lv s); lp "changes" (changesAsDictionary calls)] }
            (log |> forContextExplicit "dump" dump).Information("Updated {count} values for {services}",
                xs |> Seq.sumBy (function _service,changes -> changes.Length),
                xs |> Seq.map (function service,changes -> kv service changes.Length))

module Internal =
    /// NB aside from the naming of the top level fields within this record, it's important to note that this structure is subject to breaking changes
    // and should only be used for the purposes of diagnostic dumps
    // if there is a programmatic diagnostic you need, it should be requested and designed via an Issue
    [<NoComparison>]
    type CallPolicyInternalState =
        {   service: string; action: string; baseUri: Uri option
            config: CallConfig<Config.Http.Configuration>; state: Governor.GovernorState }

type Context private (inner : Policy<_>, logChanges, tryReadUpdates) =
    static member Create(log,readDefinitions,?createFailurePolicyBuilder) =
        let initialParse,tryReadUpdates = Impl.readAndParseCycle log readDefinitions
        let inner = initialParse.CreatePolicy(log, ?createFailurePolicyBuilder=createFailurePolicyBuilder)
        Context(inner, Impl.logChanges log, tryReadUpdates)

    member __.Policy = inner

    member __.CheckForChanges() =
        match tryReadUpdates() with
        | None -> ()
        | Some parsed ->
            let changes = parsed.UpdatePolicy(inner)
            logChanges changes

    member __.DumpInternalState(): Internal.CallPolicyInternalState seq = seq {
        for service, calls in inner.InternalState do
            for action, (cfg, state) in calls do
                yield { service=service; action=action; baseUri=cfg.config.EffectiveUri; config=cfg; state=state } }