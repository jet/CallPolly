module CallPolly.Config

open Newtonsoft.Json.Converters.FSharp
open System

let (|TimeSpanMs|) ms = TimeSpan.FromMilliseconds(float ms)

module Policy =
    module Input =
        [<NoComparison; RequireQualifiedAccess>]
        type BreakInput = { windowS: int; minRequests: int; failPct: float; breakS: float; dryRun: bool option }
        [<NoComparison; RequireQualifiedAccess>]
        type LimitInput = { maxParallel: int; maxQueue: int; dryRun: bool option }
        [<NoComparison; RequireQualifiedAccess>]
        type CutoffInput = { timeoutMs: int; slaMs: int option; dryRun: bool option }

        [<NoComparison>]
        [<RequireQualifiedAccess>]
        type Value =
            | Break of BreakInput
            | Limit of LimitInput
            | Cutoff of CutoffInput
            | Isolate

    [<NoComparison>]
    [<RequireQualifiedAccess>]
    type Rule =
        | Break of Rules.BreakerConfig
        | Limit of Rules.BulkheadConfig
        | Cutoff of Rules.CutoffConfig
        | Isolate

    let private interpret: Input.Value -> Rule = function
        | Input.Value.Isolate -> Rule.Isolate
        | Input.Value.Break x ->
            Rule.Break {
                window = TimeSpan.FromSeconds (float x.windowS)
                minThroughput = x.minRequests
                errorRateThreshold = x.failPct/100.
                retryAfter = TimeSpan.FromSeconds x.breakS
                dryRun = x.dryRun |> Option.defaultValue false }
        | Input.Value.Limit x ->
            Rule.Limit {
                dop = x.maxParallel
                queue = x.maxQueue
                dryRun = x.dryRun |> Option.defaultValue false }
        | Input.Value.Cutoff ({ timeoutMs=TimeSpanMs timeout } as x) ->
            Rule.Cutoff {
                timeout = timeout
                sla = x.slaMs |> Option.map (|TimeSpanMs|)
                dryRun = x.dryRun |> Option.defaultValue false }

    let private fold : Rule seq -> Rules.PolicyConfig =
        let folder (s : Rules.PolicyConfig) = function
            | Rule.Isolate -> { s with isolate = true }
            | Rule.Break breakerConfig -> { s with breaker = Some breakerConfig }
            | Rule.Limit bulkheadConfig -> { s with limit = Some bulkheadConfig }
            | Rule.Cutoff cutoffConfig -> { s with cutoff = Some cutoffConfig }
        Seq.fold folder { isolate = false; cutoff = None; limit = None; breaker = None }
    let ofInputs xs = xs |> Seq.map interpret |> fold

module Http =
    module Input =

        [<Newtonsoft.Json.JsonConverter(typeof<TypeSafeEnumConverter>)>]
        [<NoComparison; RequireQualifiedAccess>]
        type LogLevel =
            | Always
            | Never
            | OnlyWhenDebugEnabled

        [<NoComparison; RequireQualifiedAccess>]
        type UriInput = { ``base``: string option; path: string option }
        [<NoComparison; RequireQualifiedAccess>]
        type SlaInput = { slaMs: int; timeoutMs: int }
        [<NoComparison; RequireQualifiedAccess>]
        type LogInput = { req: LogLevel; res: LogLevel }
        [<NoComparison; RequireQualifiedAccess>]
        type Value =
            | Uri of UriInput
            | Sla of SlaInput
            | Log of LogInput

    [<RequireQualifiedAccess>]
    type Log =
        | Always
        | Never
        | OnlyWhenDebugEnabled

    let toRuleLog = function
        | Input.LogLevel.Always -> Log.Always
        | Input.LogLevel.Never -> Log.Never
        | Input.LogLevel.OnlyWhenDebugEnabled -> Log.OnlyWhenDebugEnabled

    [<NoComparison>]
    [<RequireQualifiedAccess>]
    type Rule =
        | BaseUri of Uri: Uri
        | RelUri of Uri: Uri
        | Sla of sla: TimeSpan * timeout: TimeSpan
        | Log of req: Log * res: Log

    let private interpret (x: Input.Value): Rule seq = seq {
        match x with
        | Input.Value.Uri { ``base``=b; path=p } ->
            match Option.toObj b with null -> () | b -> yield Rule.BaseUri(Uri b)
            match Option.toObj p with null -> () | p -> yield Rule.RelUri(Uri(p, UriKind.Relative))
        | Input.Value.Sla { slaMs=TimeSpanMs sla; timeoutMs=TimeSpanMs timeout } -> yield Rule.Sla(sla,timeout)
        | Input.Value.Log { req=req; res=res } -> yield Rule.Log(toRuleLog req,toRuleLog res) }

    [<NoComparison>]
    type Configuration =
        {   timeout: TimeSpan option; sla: TimeSpan option
            ``base``: Uri option; rel: Uri option
            reqLog: Log; resLog: Log }

    let private fold (xs : Rule seq): Configuration * Uri option =
        let folder s = function
            | Rule.BaseUri uri -> { s with ``base`` = Some uri }
            | Rule.RelUri uri -> { s with rel = Some uri }
            | Rule.Sla (sla=sla; timeout=t) -> { s with sla = Some sla; timeout = Some t }
            | Rule.Log (req=reqLevel; res=resLevel) -> { s with reqLog = reqLevel; resLog = resLevel }
        let def =
            {   reqLog = Log.Never; resLog = Log.Never
                timeout = None; sla = None
                ``base`` = None; rel = None }
        let config = Seq.fold folder def xs
        let effectiveAddress =
            match config.``base``, config.rel with
            | None, u | u, None -> u
            | Some b, Some r -> Uri(b,r) |> Some
        config, effectiveAddress
    let ofInputs xs = xs |> Seq.collect interpret |> fold