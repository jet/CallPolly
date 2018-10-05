module CallPolly.Parser

open CallPolly.Rules
open Newtonsoft.Json
open System
open System.Collections.Generic

/// Wrappers for Newtonsoft.Json
type Newtonsoft() =
    static let settings =
        let tmp = Settings.CreateDefault()
        tmp.Converters.Add(Converters.OptionConverter())
        tmp

    /// <summary>Deserializes value of given type from json string.</summary>
    /// <param name="json">Json string to deserialize.</param>
    static member Deserialize<'T> (json : string) : 'T =
        JsonConvert.DeserializeObject<'T>(json, settings)

[<RequireQualifiedAccess>]
[<Newtonsoft.Json.JsonConverter(typeof<Converters.TypeSafeEnumConverter>)>]
type LogAmount =
    | Always
    | Never
    | OnlyWhenDebugEnabled

[<NoComparison>]
[<RequireQualifiedAccess>]
type [<Newtonsoft.Json.JsonConverter(typeof<Converters.UnionConverter>, "rule", "Unknown")>] RuleDefinition =
    | Include of policy: string
    | Uri of ``base``: string option * path: string option
    | Sla of slaMs: int * timeoutMs: int
    | Log of req: LogAmount * res: LogAmount
    | Break of windowS: int * minRequests: int * failPct: float * breakS: float * dryRun: bool option
    | Limit of maxParallel: int * maxQueue: int * dryRun: bool option
    | Cutoff of timeoutMs: int * slaMs: int option * dryRun: bool option
    | Isolate
    /// Catch-all case when a ruleName is unknown (allows us to add new policies but have existing instances safely ignore it)
    | Unknown

/// Defines a set of base call policies for a system, together with a set of service-specific rules

type [<NoComparison; NoEquality>]
    [<JsonObject(ItemRequired = Required.Always)>]
    CallPolicyDefinition =
    {
        /// Defines Call->Policy->Rule mappings per Service of an application
        services: IDictionary<string,CallPolicyServiceDefinition> }


and [<NoComparison; NoEquality>]
    [<JsonObject(ItemRequired = Required.Always)>]
    CallPolicyServiceDefinition =
    {
        /// Map of Call names to Policies to use (may refer to items in `globalPolicies`)
        calls: IDictionary<string,string>

        /// Default Policy to use if lookup fails
        /// - can be from `globalPolicies`)
        /// - can be 'null' to indicate code should throw
        [<JsonProperty(Required=Required.AllowNull)>] // exception: `null` is allowed here
        defaultPolicy: string

        // Map of Policy names to Rule lists specific to this Service
        policies: IDictionary<string,RuleDefinition[]> }

        // TOCONSIDER it is proposed that any fallback mechanism would be modelled like this
        // /// Reference to name of another ServicePolicy whose policies are to be included in this `policies` map
        // [<JsonProperty(Required=Required.DisallowNull)>] // exception: `null` is allowed here
        // includePoliciesFrom: string

[<RequireQualifiedAccess>]
type LogMode =
    | Always
    | Never
    | OnlyWhenDebugEnabled

[<NoComparison>]
[<RequireQualifiedAccess>]
type ActionRule =
    | BaseUri of Uri: Uri
    | RelUri of Uri: Uri
    | Sla of sla: TimeSpan * timeout: TimeSpan
    | Log of req: LogMode * res: LogMode
    | Break of BreakerConfig
    | Limit of BulkheadConfig
    | Cutoff of CutoffConfig
    | Isolate

let inferPolicy : ActionRule seq -> PolicyConfig =
    let folder s = function
        | ActionRule.Isolate -> { s with isolate = true }
        | ActionRule.Break breakerConfig -> { s with breaker = Some breakerConfig }
        | ActionRule.Limit bulkheadConfig -> { s with limit = Some bulkheadConfig }
        | ActionRule.Cutoff cutoffConfig -> { s with cutoff = Some cutoffConfig }
        // Covered by inferConfig
        | ActionRule.BaseUri _ | ActionRule.RelUri _ | ActionRule.Sla _ | ActionRule.Log _ -> s
    Seq.fold folder { isolate = false; cutoff = None; limit = None; breaker = None }

[<NoComparison>]
type CallConfiguration =
    {   timeout: TimeSpan option; sla: TimeSpan option
        ``base``: Uri option; rel: Uri option
        reqLog: LogMode; resLog: LogMode }

let inferConfig xs: CallConfiguration * Uri option =
    let folder s = function
        | ActionRule.BaseUri uri -> { s with ``base`` = Some uri }
        | ActionRule.RelUri uri -> { s with rel = Some uri }
        | ActionRule.Sla (sla=sla; timeout=t) -> { s with sla = Some sla; timeout = Some t }
        | ActionRule.Log (req=reqLevel; res=resLevel) -> { s with reqLog = reqLevel; resLog = resLevel }
        // Covered by inferPolicy
        | ActionRule.Isolate | ActionRule.Cutoff _ | ActionRule.Limit _ | ActionRule.Break _ -> s
    let def =
        {   reqLog = LogMode.Never; resLog = LogMode.Never
            timeout = None; sla = None
            ``base`` = None; rel = None }
    let config = Seq.fold folder def xs
    let effectiveAddress =
        match config.``base``, config.rel with
        | None, u | u, None -> u
        | Some b, Some r -> Uri(b,r) |> Some
    config, effectiveAddress

type PolicyParseException(json, inner) =
    inherit exn(sprintf "Failed to parse CallPolicy: '%s'" json, inner)

[<AbstractClass>]
type CallPolicyReferenceException(msg, ?inner : exn) =
    inherit exn(msg, defaultArg inner null)

type NotFoundCallPolicyReferenceException(service, call, policy, policies) =
    inherit CallPolicyReferenceException(sprintf "Service '%s' Call '%s' targets undefined Policy '%s' (policies: %s)" service call policy policies)

type RecursiveIncludeCallPolicyReferenceException(stack, policy, policies) =
    inherit CallPolicyReferenceException(sprintf "Include Rule at '%s' refers recursively to '%s' (policies: %s)" stack policy policies)

type IncludeNotFoundCallPolicyReferenceException(stack, policy, policies) =
    inherit CallPolicyReferenceException(sprintf "Include Rule at '%s' refers to undefined Policy '%s' (policies: %s)" stack policy policies)

let asLogRule = function
    | LogAmount.Always -> LogMode.Always
    | LogAmount.Never -> LogMode.Never
    | LogAmount.OnlyWhenDebugEnabled -> LogMode.OnlyWhenDebugEnabled

module Constants =
    let [<Literal>] defaultCallName = "(default)"

let parseInternal defsJson =
    let defs: CallPolicyDefinition = try Newtonsoft.Deserialize defsJson with e -> PolicyParseException(defsJson, e) |> raise
    let (|TimeSpanMs|) ms = TimeSpan.FromMilliseconds(float ms)
    let dumpDefs (serviceDef: CallPolicyServiceDefinition) = sprintf "%A" [ for x in serviceDef.policies -> x.Key, List.ofArray x.Value ]
    let rec mapService stack (serviceDef : CallPolicyServiceDefinition) polRules : ActionRule seq =
        let rec unroll stack (def : RuleDefinition) : ActionRule seq = seq {
            match def with
            | RuleDefinition.Isolate -> yield ActionRule.Isolate
            | RuleDefinition.Uri(``base``=b; path=p) ->
                match Option.toObj b with null -> () | b -> yield ActionRule.BaseUri(Uri b)
                match Option.toObj p with null -> () | p -> yield ActionRule.RelUri(Uri(p, UriKind.Relative))
            | RuleDefinition.Sla(slaMs=TimeSpanMs sla; timeoutMs=TimeSpanMs timeout) -> yield ActionRule.Sla(sla,timeout)
            | RuleDefinition.Log(req, res) -> yield ActionRule.Log(asLogRule req,asLogRule res)
            | RuleDefinition.Break(windowS, min, failPct, breakS, dryRun) ->
                yield ActionRule.Break {
                    window = TimeSpan.FromSeconds (float windowS)
                    minThroughput = min
                    errorRateThreshold = failPct/100.
                    retryAfter = TimeSpan.FromSeconds breakS
                    dryRun = dryRun |> Option.defaultValue false }
            | RuleDefinition.Limit(maxParallel=dop; maxQueue=queue; dryRun=dryRun) ->
                yield ActionRule.Limit {
                    dop = dop
                    queue = queue
                    dryRun = dryRun |> Option.defaultValue false }
            | RuleDefinition.Cutoff(timeoutMs=TimeSpanMs timeout; slaMs=slaMs; dryRun=dryRun) ->
                yield ActionRule.Cutoff { timeout = timeout; sla = slaMs |> Option.map (|TimeSpanMs|); dryRun = dryRun |> Option.defaultValue false }
            | RuleDefinition.Unknown ->
                // TODO capture name of unknown rule, log once (NB recomputed every 10s so can't log every time)
                () // Ignore ruleNames we don't yet support (allows us to define rules only newer instances understand transparently)
            | RuleDefinition.Include includedPolicyName ->
                match stack |> List.tryFindIndex (fun x -> x=includedPolicyName) with
                | None -> ()
                | Some x when x >= List.length stack - 1 -> () // can refer to serviceName or callName as long as it's not also a policyName
                | _ -> raise <| RecursiveIncludeCallPolicyReferenceException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs serviceDef)

                match serviceDef.policies.TryGetValue includedPolicyName with
                | false, _ -> raise <| IncludeNotFoundCallPolicyReferenceException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs serviceDef)
                | true, defs ->
                    let stack' = includedPolicyName :: stack
                    yield! defs |> Seq.collect (unroll stack') }
        polRules |> Seq.collect (unroll stack)

    seq { for KeyValue (serviceName,serviceDef) in defs.services ->
            let mapCall callName policyName =
                match serviceDef.policies.TryGetValue policyName with
                | false, _ -> raise <| NotFoundCallPolicyReferenceException(serviceName, callName, policyName, dumpDefs serviceDef)
                | true, rules ->
                    let mapped = mapService [policyName; callName; serviceName] serviceDef rules |> List.ofSeq
                    callName,
                    {   policyName = policyName
                        policyConfig = inferPolicy mapped
                        callConfig = inferConfig mapped
                        raw = mapped }
            let defaultCallName, callsMap =
                let explicitCalls = [ for KeyValue (callName,policyName) in serviceDef.calls -> mapCall callName policyName]
                match Option.ofObj serviceDef.defaultPolicy with
                | Some defPolName when not (serviceDef.policies.ContainsKey defPolName) ->
                    invalidOp (sprintf "Could not find a default policy entitled '%s' (policies: %s)" defPolName (dumpDefs serviceDef))
                | Some _ when serviceDef.calls.ContainsKey Constants.defaultCallName ->
                    invalidOp (sprintf "Service '%s' may not define a Call with the reserved name '%s' when a 'defaultPolicy' is declared" serviceName Constants.defaultCallName)
                | Some defPolName ->
                    Some Constants.defaultCallName, mapCall Constants.defaultCallName defPolName :: explicitCalls
                | _ ->
                    None, explicitCalls
            serviceName, { serviceName = serviceName; defaultPolicyName = defaultCallName; callsMap = callsMap } }

let parse log defs : Policy<_,_> =
    let rulesMap = parseInternal defs
    Policy.Create(log, rulesMap)

let updateFrom defs (x : Policy<_,_>) : (string * (string * ChangeLevel) list) list =
    let rulesMap = parseInternal defs
    x.UpdateFrom rulesMap