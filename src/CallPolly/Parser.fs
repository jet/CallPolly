module CallPolly.Parser

open CallPolly.Rules
open Newtonsoft.Json
open System
open System.Collections.Generic

let [<Literal>] defaultPolicyLabel = "(default)"

/// Wrappers for Newtonsoft.Json
type Newtonsoft() =
    static let settings =
        let tmp = Settings.CreateDefault()
        tmp.Converters.Add(Converters.OptionConverter())
        tmp

    /// <summary>
    ///     Serializes given value to a json string.
    /// </summary>
    /// <param name="value">Value to serialize.</param>
    /// <param name="indent">Use indentation when serializing json. Defaults to true.</param>
    static member Serialize<'T>(value : 'T) : string =
        JsonConvert.SerializeObject(value, settings)

    /// <summary>
    ///     Deserializes value of given type from json string.
    /// </summary>
    /// <param name="json">Json string to deserialize.</param>
    static member Deserialize<'T> (json : string) : 'T =
        JsonConvert.DeserializeObject<'T>(json, settings)

[<RequireQualifiedAccess>]
[<Newtonsoft.Json.JsonConverter(typeof<Converters.TypeSafeEnumConverter>)>]
type LogAmount =
    | Always
    | Never
    | OnlyWhenDebugEnabled
    member __.AsLogRule = __ |> function
        | LogAmount.Always -> LogMode.Always
        | LogAmount.Never -> LogMode.Never
        | LogAmount.OnlyWhenDebugEnabled -> LogMode.OnlyWhenDebugEnabled

[<RequireQualifiedAccess>]
type [<Newtonsoft.Json.JsonConverter(typeof<Converters.UnionConverter>, "ruleName", "Unknown")>] ActionParameter =
    | Include of policyName: string
    | Uri of ``base``: string option * path: string option
    | Sla of slaMs: int * timeoutMs: int
    | Log of req: LogAmount * res: LogAmount
    | Break of windowS: int * minRequests: int * failPct: float * breakS: float * dryRun: Nullable<bool>
    | Limit of maxParallel: int * maxQueue: int * dryRun: Nullable<bool>
    | Cutoff of timeoutMs: int * slaMs: int option * dryRun: Nullable<bool>
    | Isolate
    /// Catch-all case when a ruleName is unknown (allows us to add new policies but have existing instances safely ignore it)
    | Unknown

type PolicyParseException(json, inner) =
    inherit exn(sprintf "Failed to parse Policy: '%s'" json, inner)

type PolicyRecusiveIncludeException(policyName,alreadyIncluded, policies) =
    inherit exn(sprintf "Include Rule '%s' refers recursively to '%s' (policies: %s)" policyName alreadyIncluded policies)

type ActionMapParseException(msg, inner : exn) =
    inherit exn(msg, inner)
    static member FromJson (json, inner) = ActionMapParseException(sprintf "Failed to parse ActionMap: '%s'" json, inner)

type ActionMapParseMissingPolicyException(action, policy, policies) =
    inherit ActionMapParseException(sprintf "Rule for Action '%s' targets undefined Policy '%s' (policies: %s)" action policy policies, null)

type PolicyMissingIncludedPolicyException(stack, policyName, policies) =
    inherit ActionMapParseException(sprintf "Include Rule for Policy '%s' refers to undefined Policy '%s' (policies: %s)" stack policyName policies, null)

let parse policiesJson mapJson =
    let defs: Dictionary<string,ActionParameter[]> = try Newtonsoft.Deserialize policiesJson with e -> PolicyParseException(policiesJson, e) |> raise
    let dumpDefs () = sprintf "%A" [ for x in defs -> x.Key, List.ofArray x.Value ]
    let map: Dictionary<string,string> = try Newtonsoft.Deserialize mapJson with e -> ActionMapParseException.FromJson(mapJson, e) |> raise
    let (|TimeSpanMs|) ms = TimeSpan.FromMilliseconds(float ms)
    let rec mapPolicy stack polDefs =
        let rec unroll stack (def : ActionParameter) : ActionRule seq = seq {
            match def with
            | ActionParameter.Isolate -> yield ActionRule.Isolate
            | ActionParameter.Uri(``base``=b; path=p) ->
                match Option.toObj b with null -> () | b -> yield ActionRule.BaseUri(Uri b)
                match Option.toObj p with null -> () | p -> yield ActionRule.RelUri(Uri(p, UriKind.Relative))
            | ActionParameter.Sla(slaMs=TimeSpanMs sla; timeoutMs=TimeSpanMs timeout) -> yield ActionRule.Sla(sla,timeout)
            | ActionParameter.Log(req, res) -> yield ActionRule.Log(req.AsLogRule,res.AsLogRule)
            | ActionParameter.Break(windowS, min, failPct, breakS, dryRun) ->
                yield ActionRule.Break {
                    window = TimeSpan.FromSeconds (float windowS)
                    minThroughput = min
                    errorRateThreshold = failPct/100.
                    retryAfter = TimeSpan.FromSeconds breakS
                    dryRun = if dryRun.HasValue then dryRun.Value else false }
            | ActionParameter.Limit(maxParallel=dop; maxQueue=queue; dryRun=dryRun) ->
                yield ActionRule.Limit {
                    dop = dop
                    queue = queue
                    dryRun = if dryRun.HasValue then dryRun.Value else false }
            | ActionParameter.Cutoff(timeoutMs=TimeSpanMs timeout; slaMs=slaMs; dryRun=dryRun) ->
                yield ActionRule.Cutoff { timeout = timeout; sla = slaMs |> Option.map (|TimeSpanMs|); dryRun = if dryRun.HasValue then dryRun.Value else false }
            | ActionParameter.Unknown ->
                // TODO capture name of unknown rule, log once (NB recomputed every 10s so can't log every time)
                () // Ignore ruleNames we don't yet support (allows us to define rules only newer instances understand transparently)
            | ActionParameter.Include includedPolicyName ->
                match stack |> List.tryFindIndex (fun x -> x=includedPolicyName) with
                | None -> ()
                | Some x when x = List.length stack - 1 -> ()
                | _ -> raise <| PolicyRecusiveIncludeException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs ())
                match defs.TryGetValue includedPolicyName with
                | false, _ -> raise <| PolicyMissingIncludedPolicyException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs ())
                | true, defs ->
                    let stack' = includedPolicyName :: stack
                    yield! defs |> Seq.collect (unroll stack') }
        polDefs |> Seq.collect (unroll stack)

    seq { for KeyValue (actionName,policyName) in map ->
            match defs.TryGetValue policyName with
            | false, _ -> raise <| ActionMapParseMissingPolicyException(actionName, policyName, dumpDefs ())
            | true, rules ->
                actionName, policyName, mapPolicy [policyName; actionName] rules |> List.ofSeq }

let parseUpstreamPolicyWithoutDefault log pols map : UpstreamPolicyWithoutDefault =
    let rulesMap = parse pols map
    UpstreamPolicyWithoutDefault.Parse(log, rulesMap)

let parseUpstreamPolicy log pols map : UpstreamPolicy =
    let rulesMap = parse pols map
    UpstreamPolicy.Parse(log, rulesMap, defaultPolicyLabel)

let updateFrom pols map (x : UpstreamPolicy) : (string * ChangeLevel) seq =
    let rulesMap = parse pols map
    x.UpdateFrom rulesMap