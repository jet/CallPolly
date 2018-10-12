module CallPolly.Config

open Newtonsoft.Json
open Newtonsoft.Json.Converters.FSharp
open System

let (|TimeSpanMs|) ms = TimeSpan.FromMilliseconds(float ms)

module Policy =
    module Input =

        [<NoComparison; RequireQualifiedAccess>]
        [<JsonObject(ItemRequired=Required.Always)>]
        type BreakInput =
            {   windowS: int; minRequests: int; failPct: float; breakS: float

                [<JsonProperty(Required=Required.DisallowNull)>]
                dryRun: bool option }

        [<NoComparison; RequireQualifiedAccess>]
        [<JsonObject(ItemRequired=Required.Always)>]
        type LimitInput =
            {   maxParallel: int; maxQueue: int

                [<JsonProperty(Required=Required.DisallowNull)>]
                dryRun: bool option }

        [<NoComparison; RequireQualifiedAccess>]
        [<JsonObject(ItemRequired=Required.Always)>]
        type CutoffInput =
            {   timeoutMs: int

                [<JsonProperty(Required=Required.DisallowNull)>]
                slaMs: int option

                [<JsonProperty(Required=Required.DisallowNull)>]
                dryRun: bool option }

        [<NoComparison; RequireQualifiedAccess>]
        type Value =
            | Break of BreakInput
            | Limit of LimitInput
            | Cutoff of CutoffInput
            | Isolate

    [<NoComparison; RequireQualifiedAccess>]
    [<JsonConverter(typeof<UnionConverter>, "rule")>]
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
                dryRun = defaultArg x.dryRun false }
        | Input.Value.Limit x ->
            Rule.Limit {
                dop = x.maxParallel
                queue = x.maxQueue
                dryRun = defaultArg x.dryRun false }
        | Input.Value.Cutoff ({ timeoutMs=TimeSpanMs timeout } as x) ->
            Rule.Cutoff {
                timeout = timeout
                sla = x.slaMs |> Option.map (|TimeSpanMs|)
                dryRun = defaultArg x.dryRun false }

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

        [<NoComparison; RequireQualifiedAccess>]
        [<JsonConverter(typeof<TypeSafeEnumConverter>)>]
        type LogLevel =
            | Always
            | Never
            | OnlyWhenDebugEnabled

        [<NoComparison; RequireQualifiedAccess>]
        [<JsonObject(ItemRequired=Required.DisallowNull)>]
        type UriInput = { ``base``: string option; path: string option }

        [<NoComparison; RequireQualifiedAccess>]
        [<JsonObject(ItemRequired=Required.Always)>]
        type SlaInput = { slaMs: int; timeoutMs: int }

        [<NoComparison; RequireQualifiedAccess>]
        [<JsonObject(ItemRequired=Required.Always)>] // yes, not strictly necessary
        type LogInput = { req: LogLevel; res: LogLevel }

        [<NoComparison; RequireQualifiedAccess>]
        type Value =
            | Uri of UriInput
            | Sla of SlaInput
            | Log of LogInput

    [<RequireQualifiedAccess>]
    [<JsonConverter(typeof<TypeSafeEnumConverter>)>]
    type LogLevel =
        | Always
        | Never
        | OnlyWhenDebugEnabled

    let toRuleLog = function
        | Input.LogLevel.Always -> LogLevel.Always
        | Input.LogLevel.Never -> LogLevel.Never
        | Input.LogLevel.OnlyWhenDebugEnabled -> LogLevel.OnlyWhenDebugEnabled

    [<NoComparison; RequireQualifiedAccess>]
    [<JsonConverter(typeof<UnionConverter>, "rule")>]
    type Rule =
        | BaseUri of Uri: Uri
        | RelUri of Uri: Uri
        | Sla of sla: TimeSpan * timeout: TimeSpan
        | Log of req: LogLevel * res: LogLevel

    let private interpret (x: Input.Value): Rule seq = seq {
        match x with
        | Input.Value.Uri { ``base``=b; path=p } ->
            match b with Some null | None -> () | Some b -> yield Rule.BaseUri (Uri b)
            match p with Some null | None -> () | Some p -> yield Rule.RelUri (Uri(p, UriKind.Relative))
        | Input.Value.Sla { slaMs=TimeSpanMs sla; timeoutMs=TimeSpanMs timeout } -> yield Rule.Sla (sla,timeout)
        | Input.Value.Log { req=req; res=res } -> yield Rule.Log (toRuleLog req,toRuleLog res) }

    [<NoComparison>]
    type Configuration =
        {   timeout: TimeSpan option; sla: TimeSpan option
            ``base``: Uri option; rel: Uri option
            reqLog: LogLevel; resLog: LogLevel }
        member __.EffectiveUri : Uri option =
            match __.``base``, __.rel with
            | None, None -> None
            | None, u | u, None -> u
            | Some b, Some r ->
                let baseWithTrailingSlash =
                    let current = string b
                    if current.EndsWith "/" then b
                    else Uri(current+"/", UriKind.Absolute)
                Uri(baseWithTrailingSlash,r) |> Some

    let private fold (xs : Rule seq): Configuration =
        let folder s = function
            | Rule.BaseUri uri -> { s with ``base`` = Some uri }
            | Rule.RelUri uri -> { s with rel = Some uri }
            | Rule.Sla (sla=sla; timeout=t) -> { s with sla = Some sla; timeout = Some t }
            | Rule.Log (req=reqLevel; res=resLevel) -> { s with reqLog = reqLevel; resLog = resLevel }
        let def =
            {   reqLog = LogLevel.Never; resLog = LogLevel.Never
                timeout = None; sla = None
                ``base`` = None; rel = None }
        Seq.fold folder def xs
    let ofInputs xs = xs |> Seq.collect interpret |> fold