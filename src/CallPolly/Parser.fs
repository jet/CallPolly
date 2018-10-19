module CallPolly.Parser

open CallPolly.Rules
open Newtonsoft.Json
open Newtonsoft.Json.Converters.FSharp
open System
open System.Collections.Generic

// shims for F# < 4, can be removed if we stop supporting that
module private Option =
    let ofObj = function null -> None | x -> Some x

/// Wrappers for Newtonsoft.Json - OptionConverter is required
type Newtonsoft() =
    static let defaultSettings = Settings.CreateCorrect(OptionConverter())
    static member indentSettings = Settings.CreateCorrect(OptionConverter(),indent=true)

    /// <summary>Deserializes value of given type from json string.</summary>
    /// <param name="json">Json string to deserialize.</param>
    static member Deserialize<'T> (json : string) : 'T =
        JsonConvert.DeserializeObject<'T>(json, defaultSettings)

    /// <summary>Serializes value of given type from json string.</summary>
    /// <param name="json">Json string to deserialize.</param>
    static member Serialize<'T>(x:'T, ?settings) : string =
        JsonConvert.SerializeObject(x, defaultArg settings defaultSettings)

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
        policies: IDictionary<string,Input[]> }

        // TOCONSIDER it is proposed that any fallback mechanism would be modelled like this
        // /// Reference to name of another ServicePolicy whose policies are to be included in this `policies` map
        // [<JsonProperty(Required=Required.DisallowNull)>] // exception: `null` is allowed here
        // includePoliciesFrom: string
and [<NoComparison>]
    [<RequireQualifiedAccess>]
    [<Newtonsoft.Json.JsonConverter(typeof<UnionConverter>, "rule", "Unknown")>] Input =
    | Include of IncludeInput
    /// Catch-all case when a ruleName is unknown (allows us to add new policies but have existing instances safely ignore it)
    | Unknown of Newtonsoft.Json.Linq.JObject

    (* Config.Policy.Input.Value *)

    | Isolate
    | Break of Config.Policy.Input.BreakInput
    | Limit of Config.Policy.Input.LimitInput
    | LimitBy of Config.Policy.Input.LimitByInput
    | Cutoff of Config.Policy.Input.CutoffInput

    (* Config.Http.Input.Value *)

    | Uri of Config.Http.Input.UriInput
    | Sla of Config.Http.Input.SlaInput
    | Log of Config.Http.Input.LogInput
and IncludeInput = { [<JsonRequired>]policy: string }

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

module Constants =
    let [<Literal>] defaultCallName = "(default)"

[<NoComparison>]
[<RequireQualifiedAccess>]
type ParsedRule =
    | Policy of Config.Policy.Input.Value
    | Http of Config.Http.Input.Value
    | Unknown of Newtonsoft.Json.Linq.JObject
[<NoComparison>]
type ParsedCall = { callName: string; policyName: string; rules: ParsedRule list }
[<NoComparison>]
type ParsedService = { serviceName: string; defaultCallName: string option; calls: ParsedCall[] }

let parseInternal defsJson : ParsedService [] =
    let defs: CallPolicyDefinition = try Newtonsoft.Deserialize defsJson with e -> PolicyParseException(defsJson, e) |> raise
    let dumpDefs (serviceDef: CallPolicyServiceDefinition) = sprintf "%A" [ for x in serviceDef.policies -> x.Key, List.ofArray x.Value ]
    let rec mapService stack (serviceDef : CallPolicyServiceDefinition) polRules : ParsedRule list =
        let rec unroll stack (def : Input) : ParsedRule seq = seq {
            match def with
            | Input.Uri x -> yield ParsedRule.Http (Config.Http.Input.Value.Uri x)
            | Input.Sla x -> yield ParsedRule.Http (Config.Http.Input.Value.Sla x)
            | Input.Log x -> yield ParsedRule.Http (Config.Http.Input.Value.Log x)

            | Input.Isolate -> yield ParsedRule.Policy Config.Policy.Input.Value.Isolate
            | Input.Break x -> yield ParsedRule.Policy (Config.Policy.Input.Value.Break x)
            | Input.Limit x -> yield ParsedRule.Policy (Config.Policy.Input.Value.Limit x)
            | Input.LimitBy x -> yield ParsedRule.Policy (Config.Policy.Input.Value.LimitBy x)
            | Input.Cutoff x -> yield ParsedRule.Policy (Config.Policy.Input.Value.Cutoff x)

            | Input.Unknown x -> yield ParsedRule.Unknown x
            | Input.Include { policy=includedPolicyName } ->
                match stack |> List.tryFindIndex (fun x -> x=includedPolicyName) with
                | None -> ()
                | Some x when x >= List.length stack - 1 -> () // can refer to serviceName or callName as long as it's not also a policyName
                | _ -> raise <| RecursiveIncludeCallPolicyReferenceException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs serviceDef)

                match serviceDef.policies.TryGetValue includedPolicyName with
                | false, _ -> raise <| IncludeNotFoundCallPolicyReferenceException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs serviceDef)
                | true, defs ->
                    let stack' = includedPolicyName :: stack
                    for d in defs do
                        yield! unroll stack' d }
        [ for r in polRules do
            yield! unroll stack r ]

    [|  for KeyValue (serviceName,serviceDef) in defs.services ->
            let mapCall callName policyName =
                match serviceDef.policies.TryGetValue policyName with
                | false, _ -> raise <| NotFoundCallPolicyReferenceException(serviceName, callName, policyName, dumpDefs serviceDef)
                | true, rules ->
                    let mapped = mapService [policyName; callName; serviceName] serviceDef rules
                    { callName = callName; policyName = policyName; rules = mapped }
            let defaultCallName, callsMap =
                let explicitCalls = seq { for KeyValue (callName,policyName) in serviceDef.calls -> mapCall callName policyName }
                match Option.ofObj serviceDef.defaultPolicy with
                | Some defPolName when not (serviceDef.policies.ContainsKey defPolName) ->
                    invalidOp (sprintf "Could not find a default policy entitled '%s' (policies: %s)" defPolName (dumpDefs serviceDef))
                | Some _ when serviceDef.calls.ContainsKey Constants.defaultCallName ->
                    invalidOp (sprintf "Service '%s' may not define a Call with the reserved name '%s' when a 'defaultPolicy' is declared" serviceName Constants.defaultCallName)
                | Some defPolName ->
                    Some Constants.defaultCallName, [| yield mapCall Constants.defaultCallName defPolName; yield! explicitCalls |]
                | _ ->
                    None, Array.ofSeq explicitCalls
            { serviceName = serviceName; defaultCallName = defaultCallName; calls = callsMap } |]

[<NoComparison>]
type Warning = { serviceName: string; callName: string; unknownRule: Newtonsoft.Json.Linq.JObject }

type ParseResult(services: ParsedService[]) =
    let mapService (x : ParsedService) : (*serviceName:*) string * ServiceConfig<_> =
        x.serviceName,
        {   serviceName = x.serviceName
            defaultCallName = x.defaultCallName
            callsMap =
                [ for call in x.calls ->
                    let callConfig =
                        {   policyName = call.policyName
                            policy = call.rules |> Seq.choose (function ParsedRule.Policy x -> Some x | _ -> None) |> Config.Policy.ofInputs
                            config = call.rules |> Seq.choose (function ParsedRule.Http x -> Some x | _ -> None) |> Config.Http.ofInputs }
                    call.callName, callConfig ] }
    let mapped = services |> Seq.map mapService

    member __.Warnings =
        [| for service in services do
            for call in service.calls do
                for rule in call.rules do
                    match rule with
                    | ParsedRule.Unknown jo ->
                        yield { serviceName = service.serviceName; callName = call.callName; unknownRule = jo }
                    | _ -> () |]

    member __.Raw : Map<string,Map<string,ParsedRule list>> = Map.ofSeq <| seq {
        for service in services ->
            service.serviceName,
            Map.ofSeq <| seq {
                for call in service.calls ->
                    call.callName, call.rules } }

    member __.CreatePolicy(log, ?createFailurePolicyBuilder : CallConfig<_> -> Polly.PolicyBuilder) : Policy<_> =
        let createFailurePolicyBuilder =
            match createFailurePolicyBuilder with
            | None -> fun _callConfig -> Polly.Policy.Handle<TimeoutException>()
            | Some custom -> custom
        Policy(log, createFailurePolicyBuilder, mapped)

    member __.UpdatePolicy(x : Policy<_>) : (string * (string * ChangeLevel) list) list =
        x.UpdateFrom mapped

let parse defs : ParseResult =
    ParseResult(parseInternal defs)