namespace CallPolly

open Serilog
open System

module private Impl =

    type Warning = { service: string; call: string; ruleText: string }

    let readAndParseCycle (log: ILogger) readDefinitions =
        let mutable current = readDefinitions ()
        let ingest () =
            let res = Parser.parse current
            if not (List.isEmpty res.Warnings) then
                let msgs = seq { for w in res.Warnings -> { service=w.serviceName; call=w.callName; ruleText = string w.unknownRule } }
                log.ForContext("{count} Warnings", msgs, true).Warning("Definition had {count} unrecognized rules", List.length res.Warnings)
            res
        let tryReadUpdates () =
            let updated = readDefinitions()
            if current = updated then None else

            current <- updated
            ingest () |> Some

        ingest(),tryReadUpdates

    type ServicePolicyUpdate =
        {   service: string
            actionUpdates: (string * CallPolly.Rules.ChangeLevel) [] }

    let logChanges (log: ILogger) res =
        let changes =
            seq { for service, actionUpdates in res -> { service = service; actionUpdates = List.toArray actionUpdates } }
            |> Seq.distinct
            |> Seq.cache
        log
            .ForContext("dump", changes, true)
            .Information("Updated {count} values for {@services}",
                Seq.length changes,
                seq { for x in changes -> x.service, Array.length x.actionUpdates });

[<NoComparison>]
type CallPolicyInternalState =
    {   service: string; action: string; baseUri: Uri option
        config: Rules.CallConfig<Config.Http.Configuration>; state: Rules.GovernorState }

type Context private (inner : Rules.Policy<_>, logChanges, tryReadUpdates) =
    static member Create(log,createPolicyBuilder,readDefinitions) =
        let initialParse,tryReadUpdates = Impl.readAndParseCycle log readDefinitions
        let inner = initialParse.CreatePolicy(log, createPolicyBuilder)
        Context(inner, Impl.logChanges log, tryReadUpdates)

    member __.Policy = inner

    member __.CheckForChanges() =
        match tryReadUpdates() with
        | None -> ()
        | Some parsed ->
            let changes = parsed.UpdatePolicy(inner)
            logChanges changes

    member __.DumpInternalState() = seq {
        for service, actions in inner.InternalState do
            for action, (config, state) in actions do
                yield { service = service; action=action; baseUri=config.config.EffectiveUri; config=config; state=state } }